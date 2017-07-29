/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.cassandra.cql3.statements;

import java.util.*;

import io.reactivex.Completable;
import io.reactivex.Maybe;
import org.apache.cassandra.auth.permission.CorePermission;
import org.apache.cassandra.cql3.*;
import org.apache.cassandra.db.marshal.*;
import org.apache.cassandra.exceptions.*;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.MigrationManager;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.transport.Event;

public abstract class AlterTypeStatement extends SchemaAlteringStatement
{
    protected final UTName name;

    protected AlterTypeStatement(UTName name)
    {
        this.name = name;
    }

    @Override
    public void prepareKeyspace(ClientState state) throws InvalidRequestException
    {
        if (!name.hasKeyspace())
            name.setKeyspace(state.getKeyspace());

        if (name.getKeyspace() == null)
            throw new InvalidRequestException("You need to be logged in a keyspace or use a fully qualified user type name");
    }

    protected abstract UserType makeUpdatedType(UserType toUpdate, KeyspaceMetadata ksm) throws InvalidRequestException;

    public static AlterTypeStatement addition(UTName name, FieldIdentifier fieldName, CQL3Type.Raw type)
    {
        return new Add(name, fieldName, type);
    }

    public static AlterTypeStatement alter(UTName name, FieldIdentifier fieldName, CQL3Type.Raw type)
    {
        throw new InvalidRequestException("Altering of types is not allowed");
    }

    public static AlterTypeStatement renames(UTName name, Map<FieldIdentifier, FieldIdentifier> renames)
    {
        return new Renames(name, renames);
    }

    @Override
    public void checkAccess(QueryState state)
    {
        state.checkKeyspacePermission(keyspace(), CorePermission.ALTER);
    }

    public void validate(QueryState state) throws RequestValidationException
    {
        // We definitively don't want to let user modify types in system keyspaces but we can't fully rely on
        // authorization for that because ALTER TYPE only require ALTER permission on the keyspace, and we do allow
        // that on some system (distributed) keyspace to allow changing the replication factor. So rejecting it now.
        if (!SchemaConstants.isUserKeyspace(keyspace()))
            throw new InvalidRequestException("Cannot ALTER types in system keyspace " + keyspace());

        // The rest of validation is mostly left to announceMigration as it's easier to do it while constructing the updated type.
        // It doesn't really change anything anyway.
    }

    @Override
    public String keyspace()
    {
        return name.getKeyspace();
    }

    public Maybe<Event.SchemaChange> announceMigration(QueryState queryState, boolean isLocalOnly) throws InvalidRequestException, ConfigurationException
    {
        KeyspaceMetadata ksm = Schema.instance.getKeyspaceMetadata(name.getKeyspace());
        if (ksm == null)
            return error(String.format("Cannot alter type in unknown keyspace %s", name.getKeyspace()));

        UserType toUpdate = ksm.types.getNullable(name.getUserTypeName());
        if (toUpdate == null)
            return error(String.format("No user type named %s exists.", name));

        UserType updated;
        try
        {
            updated = makeUpdatedType(toUpdate, ksm);
        }
        catch (InvalidRequestException exc)
        {
            return Maybe.error(exc);
        }

        // Now, we need to announce the type update to basically change it for new tables using this type,
        // but we also need to find all existing user types and CF using it and change them.
        List<Completable> migrations = new ArrayList<>();
        migrations.add(MigrationManager.announceTypeUpdate(updated, isLocalOnly));


        return Completable.concat(migrations)
                .andThen(Maybe.just(new Event.SchemaChange(Event.SchemaChange.Change.UPDATED, Event.SchemaChange.Target.TYPE, keyspace(), name.getStringTypeName())));
    }

    protected void checkTypeNotUsedByAggregate(KeyspaceMetadata ksm)
    {
        ksm.functions.udas().filter(aggregate -> aggregate.initialCondition() != null && aggregate.stateType().referencesUserType(name.getStringTypeName()))
                     .findAny()
                     .ifPresent((aggregate) -> {
                         throw new InvalidRequestException(String.format("Cannot alter user type %s as it is still used as an INITCOND by aggregate %s", name, aggregate));
                     });
    }

    private static class Add extends AlterTypeStatement
    {
        private final FieldIdentifier fieldName;
        private final CQL3Type.Raw type;

        public Add(UTName name, FieldIdentifier fieldName, CQL3Type.Raw type)
        {
            super(name);
            this.fieldName = fieldName;
            this.type = type;
        }

        protected UserType makeUpdatedType(UserType toUpdate, KeyspaceMetadata ksm) throws InvalidRequestException
        {
            if (toUpdate.fieldPosition(fieldName) >= 0)
                throw new InvalidRequestException(String.format("Cannot add new field %s to type %s: a field of the same name already exists", fieldName, name));

            List<FieldIdentifier> newNames = new ArrayList<>(toUpdate.size() + 1);
            newNames.addAll(toUpdate.fieldNames());
            newNames.add(fieldName);

            AbstractType<?> addType = type.prepare(keyspace()).getType();
            if (addType.referencesUserType(toUpdate.getNameAsString()))
                throw new InvalidRequestException(String.format("Cannot add new field %s of type %s to type %s as this would create a circular reference", fieldName, type, name));

            List<AbstractType<?>> newTypes = new ArrayList<>(toUpdate.size() + 1);
            newTypes.addAll(toUpdate.fieldTypes());
            newTypes.add(addType);

            return new UserType(toUpdate.keyspace, toUpdate.name, newNames, newTypes, toUpdate.isMultiCell());
        }
    }

    private static class Renames extends AlterTypeStatement
    {
        private final Map<FieldIdentifier, FieldIdentifier> renames;

        public Renames(UTName name, Map<FieldIdentifier, FieldIdentifier> renames)
        {
            super(name);
            this.renames = renames;
        }

        protected UserType makeUpdatedType(UserType toUpdate, KeyspaceMetadata ksm) throws InvalidRequestException
        {
            checkTypeNotUsedByAggregate(ksm);

            List<FieldIdentifier> newNames = new ArrayList<>(toUpdate.fieldNames());
            List<AbstractType<?>> newTypes = new ArrayList<>(toUpdate.fieldTypes());

            for (Map.Entry<FieldIdentifier, FieldIdentifier> entry : renames.entrySet())
            {
                FieldIdentifier from = entry.getKey();
                FieldIdentifier to = entry.getValue();
                int idx = toUpdate.fieldPosition(from);
                if (idx < 0)
                    throw new InvalidRequestException(String.format("Unknown field %s in type %s", from, name));
                newNames.set(idx, to);
            }

            UserType updated = new UserType(toUpdate.keyspace, toUpdate.name, newNames, newTypes, toUpdate.isMultiCell());
            String duplicate = CreateTypeStatement.haveDuplicateName(updated);
            if (duplicate != null)
                throw new InvalidRequestException(String.format("Duplicate field name %s in type %s", duplicate, updated.name));

            return updated;
        }

    }
}
