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

package org.apache.cassandra.service;

import java.util.Map;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.RowUpdateBuilder;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.schema.KeyspaceParams;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class StorageServiceTest extends CQLTester
{
    private static final String KS1 = "KS1";
    private static final String KS2 = "KS2";
    private static final String KS3 = "KS3";
    private static final String UNREPAIRED = "UNREPAIRED";
    private static final String REPAIRED = "REPAIRED";
    private static final String TABLE3 = "TABLE3";
    private static final String TABLE4 = "TABLE4";
    private static final String VIEW1 = "VIEW1";
    private static final String VIEW2 = "VIEW2";
    private static final String CDC1 = "CDC1";
    private static final String CDC2 = "CDC2";

    @BeforeClass
    public static void setUp() throws ConfigurationException
    {
        SchemaLoader.prepareServer();
        CFMetaData cdc1Meta = CFMetaData.compile(String.format("CREATE TABLE \"%s\" (key text, val int, primary key(key)) WITH cdc = true",
                                                               CDC1), KS2);
        CFMetaData cdc2Meta = CFMetaData.compile(String.format("CREATE TABLE \"%s\" (key text, val int, primary key(key)) WITH cdc = true",
                                                               CDC2), KS3);
        SchemaLoader.createKeyspace(KS1,
                                    KeyspaceParams.simple(1),
                                    SchemaLoader.standardCFMD(KS1, UNREPAIRED),
                                    SchemaLoader.standardCFMD(KS1, REPAIRED));
        SchemaLoader.createKeyspace(KS2,
                                    KeyspaceParams.simple(1),
                                    SchemaLoader.standardCFMD(KS2, TABLE3),
                                    SchemaLoader.standardCFMD(KS2, TABLE4),
                                    cdc1Meta);
        SchemaLoader.createKeyspace(KS3,
                                    KeyspaceParams.simple(1),
                                    cdc2Meta);
        SchemaLoader.createView(KS2, TABLE4, VIEW1);
        SchemaLoader.createView(KS3, CDC2, VIEW2);

        Keyspace.setInitialized();

        //Need to apply metadata to make sure cdc parameter is loaded
        Keyspace.open(KS2).getColumnFamilyStore(CDC1).metadata.apply(cdc1Meta);
        Keyspace.open(KS3).getColumnFamilyStore(CDC2).metadata.apply(cdc2Meta);
    }

    @Test
    public void testShouldFallBackToFullRepair() throws Exception
    {
        //Incremental repair on tables or keyspaces without materialized views should never fall back to full repairs
        assertFalse(StorageService.instance.shouldFallBackToFullRepair(KS1, new String[]{ UNREPAIRED }));
        assertFalse(StorageService.instance.shouldFallBackToFullRepair(KS1, new String[]{ REPAIRED }));
        assertFalse(StorageService.instance.shouldFallBackToFullRepair(KS1, new String[]{ UNREPAIRED, REPAIRED }));
        assertFalse(StorageService.instance.shouldFallBackToFullRepair(KS1, new String[]{}));
        assertFalse(StorageService.instance.shouldFallBackToFullRepair(KS2, new String[]{ TABLE3 }));

        //Incremental repairs only on tables/keyspaces with MVs/CDCs can safely fallback to full repairs
        assertTrue(StorageService.instance.shouldFallBackToFullRepair(KS2, new String[]{ TABLE4 }));
        assertTrue(StorageService.instance.shouldFallBackToFullRepair(KS2, new String[]{ VIEW1 }));
        assertTrue(StorageService.instance.shouldFallBackToFullRepair(KS2, new String[]{ VIEW1, TABLE4 }));
        assertTrue(StorageService.instance.shouldFallBackToFullRepair(KS2, new String[]{ VIEW1, CDC1 }));
        assertTrue(StorageService.instance.shouldFallBackToFullRepair(KS2, new String[]{ CDC1 }));
        assertTrue(StorageService.instance.shouldFallBackToFullRepair(KS3, new String[]{ }));
        assertTrue(StorageService.instance.shouldFallBackToFullRepair(KS3, new String[]{ CDC2, VIEW2 }));

        //Incremental repair on mixed MVs and non-MVs tables should not be allowed and throw exception
        try
        {
            StorageService.instance.shouldFallBackToFullRepair(KS2, new String[]{});
            fail("Should have thrown IllegalArgumentException");
        } catch (IllegalArgumentException e)
        {
            //pass
        }

        try
        {
            StorageService.instance.shouldFallBackToFullRepair(KS2, new String[]{TABLE3, TABLE4});
            fail("Should have thrown IllegalArgumentException");
        } catch (IllegalArgumentException e)
        {
            //pass
        }
    }

    @Test
    public void testGetTableInfos() throws Exception
    {
        Keyspace keyspace = Keyspace.open(KS1);
        ColumnFamilyStore unrepaired = keyspace.getColumnFamilyStore(UNREPAIRED);
        ColumnFamilyStore repaired = keyspace.getColumnFamilyStore(REPAIRED);

        //add sstable to unrepaired keyspace
        new RowUpdateBuilder(unrepaired.metadata, 0, "key1")
        .clustering("Column1")
        .add("val", "asdf")
        .build()
        .applyUnsafe();
        unrepaired.forceBlockingFlush();

        assertEquals(1, unrepaired.getLiveSSTables().size());

        //add sstable to repaired keyspace
        new RowUpdateBuilder(repaired.metadata, 1, "key1")
        .clustering("Column1")
        .add("val", "asdf")
        .build()
        .applyUnsafe();
        repaired.forceBlockingFlush();
        assertEquals(1, repaired.getLiveSSTables().size());

        SSTableReader repairedSSTable = repaired.getLiveSSTables().iterator().next();
        repairedSSTable.descriptor.getMetadataSerializer().mutateRepairedAt(repairedSSTable.descriptor, System.currentTimeMillis());
        repairedSSTable.reloadSSTableMetadata();

         //also add unrepaired sstable to repaired table
        new RowUpdateBuilder(repaired.metadata, 1, "key2")
        .clustering("Column2")
        .add("val", "asdf")
        .build()
        .applyUnsafe();
        repaired.forceBlockingFlush();
        assertEquals(2, repaired.getLiveSSTables().size());

        //Get everything from KS1
        Map<String, Map<String, String>> tableInfos = StorageService.instance.getTableInfos(KS1);
        assertEquals(2, tableInfos.size());
        assertTrue(tableInfos.containsKey(UNREPAIRED));
        assertTrue(tableInfos.containsKey(REPAIRED));
        assertTableInfo(tableInfos.get(UNREPAIRED), false, false, false, false);
        assertTableInfo(tableInfos.get(REPAIRED), false, false, true, false);

        //Get everything from KS2
        tableInfos = StorageService.instance.getTableInfos(KS2);
        assertEquals(4, tableInfos.size());
        assertTrue(tableInfos.containsKey(TABLE3));
        assertTrue(tableInfos.containsKey(TABLE4));
        assertTrue(tableInfos.containsKey(VIEW1));
        assertTrue(tableInfos.containsKey(CDC1));
        assertTableInfo(tableInfos.get(TABLE3), false, false, false, false);
        assertTableInfo(tableInfos.get(TABLE4), true, false, false, false);
        assertTableInfo(tableInfos.get(VIEW1), false, true, false, false);
        assertTableInfo(tableInfos.get(CDC1), false, false, false, true);

        //Get everything from KS3
        tableInfos = StorageService.instance.getTableInfos(KS3);
        assertEquals(2, tableInfos.size());
        assertTrue(tableInfos.containsKey(CDC2));
        assertTrue(tableInfos.containsKey(VIEW2));
        assertTableInfo(tableInfos.get(CDC2), true, false, false, true);
        assertTableInfo(tableInfos.get(VIEW2), false, true, false, false);

        //Get table pair
        tableInfos = StorageService.instance.getTableInfos(KS2, TABLE3, TABLE4);
        assertEquals(2, tableInfos.size());
        assertTrue(tableInfos.containsKey(TABLE3));
        assertTrue(tableInfos.containsKey(TABLE4));
        assertTableInfo(tableInfos.get(TABLE3), false, false, false, false);
        assertTableInfo(tableInfos.get(TABLE4), true, false, false, false);

        //Get individual tables
        tableInfos = StorageService.instance.getTableInfos(KS1, REPAIRED);
        assertEquals(1, tableInfos.size());
        assertTrue(tableInfos.containsKey(REPAIRED));
        assertTableInfo(tableInfos.get(REPAIRED), false, false, true, false);

        tableInfos = StorageService.instance.getTableInfos(KS2, VIEW1);
        assertEquals(1, tableInfos.size());
        assertTrue(tableInfos.containsKey(VIEW1));
        assertTableInfo(tableInfos.get(VIEW1), false, true, false, false);

        tableInfos = StorageService.instance.getTableInfos(KS2, CDC1);
        assertEquals(1, tableInfos.size());
        assertTrue(tableInfos.containsKey(CDC1));
        assertTableInfo(tableInfos.get(CDC1), false, false, false, true);
    }

    private static void assertTableInfo(Map<String, String> infoAsMap, boolean hasViews, boolean isView,
                                        boolean hasIncrementallyRepaired, boolean isCdcEnabled)
    {
        TableInfo tableInfo = TableInfo.fromMap(infoAsMap);
        assertEquals(hasViews, tableInfo.hasViews);
        assertEquals(isView, tableInfo.isView);
        assertEquals(hasIncrementallyRepaired, tableInfo.wasIncrementallyRepaired);
        assertEquals(isCdcEnabled, tableInfo.isCdcEnabled);
    }
}