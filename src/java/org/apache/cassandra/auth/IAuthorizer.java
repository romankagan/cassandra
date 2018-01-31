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
package org.apache.cassandra.auth;

import java.util.*;

import org.apache.cassandra.auth.permission.CorePermission;
import org.apache.cassandra.exceptions.*;

/**
 * Primary Cassandra authorization interface.
 */
public interface IAuthorizer
{
    public enum TransitionalMode
    {
        /**
         * Transitional mode is disabled. Default.
         */
        DISABLED
        {
            @Override
            public boolean enforcePermissionsOnAuthenticatedUser()
            {
                return true;
            }

            @Override
            public boolean enforcePermissionsAgainstAnonymous()
            {
                return true;
            }

            @Override
            public boolean supportPermission(Permission perm)
            {
                return true;
            }
        },
        /**
         * Permissions can be passed to resources, but are not enforced.
         */
        NORMAL,
        /**
         * Permissions can be passed to resources, and are enforced on authenticated users.
         * Permissions are not enforced against anonymous users.
         */
        STRICT
        {
            @Override
            public boolean enforcePermissionsOnAuthenticatedUser()
            {
                return true;
            }
        };

        /**
         * Whether to enforce permissions against authenticated users.
         */
        public boolean enforcePermissionsOnAuthenticatedUser()
        {
            return false;
        }

        /**
         * Whether to enforce permissions against anonymous (if authentication is required).
         */
        public boolean enforcePermissionsAgainstAnonymous()
        {
            return false;
        }

        /**
         * Checks if the permission is supported by this transitional mode (e.g. AUTHORIZE).
         * @param perm the permission to check
         * @return {@code true} if the permission is supported, {@code false} otherwise.
         */
        public boolean supportPermission(Permission perm)
        {
            // For all transitional modes, the effective permissions do not include these permissions.
            // Since these permissions are nonetheless persisted, we have to filter those here.
            // NOTE: CorePermission.DESCRIBE was orignally always denied. However, DESCRIBE was not
            // used in the apollo+bdp code base, but is now used for system-keyspace-filtering, which
            // requires the DESCRIBE permission to work. But the scenario of transitional authorization
            // in combination with system-keyspace-filtering is untested.
            return perm != CorePermission.AUTHORIZE // AUTHORIZE _must_ be denied
                    // NOTE: READ+WRITE are neither used in DSE-DB nor bdp code base. While transitioning
                    // to authentication/authorization, such permissions should not have been granted.
                    && perm != CorePermission.READ
                    && perm != CorePermission.WRITE;
        }

    }

    default TransitionalMode getTransitionalMode()
    {
        return TransitionalMode.DISABLED;
    }

    default <T extends IAuthorizer> T implementation()
    {
        return (T) this;
    }

    default <T extends IAuthorizer> boolean isImplementationOf(Class<T> implClass)
    {
        return implClass.isAssignableFrom(implementation().getClass());
    }

    /**
     * Whether or not the authorizer will attempt authorization.
     * If false the authorizer will not be called for authorization of resources.
     */
    default boolean requireAuthorization()
    {
        return true;
    }

    /**
     * Return all permissions on all resources for a role ; granted, restricted and grantables.
     *
     * Returns a set of permissions of a user on a resource.
     * Since Roles were introduced in version 2.2, Cassandra does not distinguish in any
     * meaningful way between users and roles. A role may or may not have login privileges
     * and roles may be granted to other roles. In fact, Cassandra does not really have the
     * concept of a user, except to link a client session to role. AuthenticatedUser can be
     * thought of as a manifestation of a role, linked to a specific client connection.
     *
     * <em>Granted permissions:</em>
     * The set of <em>effective</em> permissions on a particular resource is the sum of all
     * granted permissions on the resource-chain inquired ({@link PermissionSets#granted PermissionSets.granted})
     * <em>minus</em> the sum of all restricted permissions on the resource-chain
     * ({@link PermissionSets#restricted PermissionSets.restricted}). This means, that negative (restricted)
     * permissions take precedence even if the negative permissions are placed on resource parents.
     *
     * <em>Restricted permissions:</em>
     * Retrurns a set of negative permissions that a user is denied on a resource.
     *
     * <em>Grantable permissions:</em>
     * Returns a set of permissions that a user can grant to other users on a resource.
     *
     * These permissions have no effect on the <em>effective</em> permissions of the user
     * on a resource.
     *
     * @return map of resource to permissions. {@code null} is not a valid return value.
     */
    Map<IResource, PermissionSets> allPermissionSets(RoleResource role);

    /**
     * Grants a set of permissions on a resource to a role.
     * The opposite of revoke().
     * This method is optional and may be called internally, so implementations which do
     * not support it should be sure to throw UnsupportedOperationException.
     *
     * @param performer User who grants the permissions.
     * @param permissions Set of permissions to grant.
     * @param resource Resource on which to grant the permissions.
     * @param grantee Role to which the permissions are to be granted.
     * @param grantModes whether to grant permissions on the resource, the resource with grant option or
     *                    only the permission to grant
     * @return the permissions that have been sucessfully granted.
     * @throws RequestValidationException
     * @throws RequestExecutionException
     * @throws java.lang.UnsupportedOperationException
     */
    Set<Permission> grant(AuthenticatedUser performer, Set<Permission> permissions, IResource resource, RoleResource grantee, GrantMode... grantModes)
    throws RequestValidationException, RequestExecutionException;

    /**
     * Revokes a set of permissions on a resource from a user.
     * The opposite of grant().
     * This method is optional and may be called internally, so implementations which do
     * not support it should be sure to throw UnsupportedOperationException.
     *
     * @param performer User who revokes the permissions.
     * @param permissions Set of permissions to revoke.
     * @param resource Resource on which to revoke the permissions.
     * @param revokee Role from which to the permissions are to be revoked.
     * @param grantModes what to revoke, the permission on the resource, the permission to grant or both
     * @return the permissions that have been sucessfully revoked.
     *
     * @throws RequestValidationException
     * @throws RequestExecutionException
     * @throws java.lang.UnsupportedOperationException
     */
    Set<Permission> revoke(AuthenticatedUser performer, Set<Permission> permissions, IResource resource, RoleResource revokee, GrantMode... grantModes)
    throws RequestValidationException, RequestExecutionException;

    /**
     * Returns a list of permissions on a resource granted to a role.
     * This method is optional and may be called internally, so implementations which do
     * not support it should be sure to throw UnsupportedOperationException.
     *
     * @param permissions Set of Permission values the user is interested in. The result should only include the
     *                    matching ones.
     * @param resource The resource on which permissions are requested. Can be null, in which case permissions on all
     *                 resources should be returned.
     * @param grantee The role whose permissions are requested. Can be null, in which case permissions of every
     *           role should be returned.
     *
     * @return All of the matching permission that the requesting user is authorized to know about.
     *
     * @throws RequestValidationException
     * @throws RequestExecutionException
     * @throws java.lang.UnsupportedOperationException
     */
    Set<PermissionDetails> list(Set<Permission> permissions, IResource resource, RoleResource grantee)
    throws RequestValidationException, RequestExecutionException;

    /**
     * Called before deleting a role with DROP ROLE statement (or the alias provided for compatibility,
     * DROP USER) so that a new role with the same name wouldn't inherit permissions of the deleted one in the future.
     * This removes all permissions granted to the Role in question.
     * This method is optional and may be called internally, so implementations which do
     * not support it should be sure to throw UnsupportedOperationException.
     *
     * @param revokee The role to revoke all permissions from.
     * @throws java.lang.UnsupportedOperationException
     */
    void revokeAllFrom(RoleResource revokee);

    /**
     * This method is called after a resource is removed (i.e. keyspace, table or role is dropped) and revokes all
     * permissions granted on the IResource in question.
     * This method is optional and may be called internally, so implementations which do
     * not support it should be sure to throw UnsupportedOperationException.
     *
     * @param droppedResource The resource to revoke all permissions on.
     * @throws java.lang.UnsupportedOperationException
     */
    void revokeAllOn(IResource droppedResource);

    /**
     * Set of resources that should be made inaccessible to users and only accessible internally.
     *
     * @return Keyspaces, column families that will be unmodifiable by users; other resources.
     */
    Set<? extends IResource> protectedResources();

    /**
     * Validates configuration of IAuthorizer implementation (if configurable).
     *
     * @throws ConfigurationException when there is a configuration error.
     */
    void validateConfiguration() throws ConfigurationException;

    /**
     * Setup is called once upon system startup to initialize the IAuthorizer.
     *
     * For example, use this method to create any required keyspaces/column families.
     */
    void setup();

    /**
     * Return the universe of valid permissions for a named IResource. By default, this
     * just delegates to the resource implementation itself, but this may be overridden
     * to allow custom permissions on certain types of resource.
     * @param resource the IResource to get all allowed permissions for
     * @return the set of all allowed permissions for the supplied resource
     */
    default Set<Permission> applicablePermissions(IResource resource)
    {
        return resource.applicablePermissions();
    }
}
