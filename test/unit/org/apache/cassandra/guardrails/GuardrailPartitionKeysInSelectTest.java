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

package org.apache.cassandra.guardrails;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.assertj.core.api.Assertions;

public class GuardrailPartitionKeysInSelectTest extends GuardrailTester
{
    private static int defaultPartitionKeysInSelectQuery;

    @BeforeClass
    public static void setup()
    {
        defaultPartitionKeysInSelectQuery = DatabaseDescriptor.getGuardrailsConfig().partition_keys_in_select_failure_threshold;
    }

    @AfterClass
    public static void tearDown()
    {
        DatabaseDescriptor.getGuardrailsConfig().partition_keys_in_select_failure_threshold = defaultPartitionKeysInSelectQuery;
    }

    @Before
    public void setUp() throws Throwable
    {
        createTable("CREATE TABLE IF NOT EXISTS %s (k INT, c INT, v TEXT, PRIMARY KEY(k, c))");
        DatabaseDescriptor.getGuardrailsConfig().partition_keys_in_select_failure_threshold = 3;
    }

    @Test
    public void testConfigValidation()
    {
        testValidationOfStrictlyPositiveProperty((c, v) -> c.partition_keys_in_select_failure_threshold = v.intValue(),
                                                 "partition_keys_in_select_failure_threshold");
    }

    @Test
    public void testFilterOnFewPartitions() throws Throwable
    {
        // test that it does not throw
        execute("SELECT * FROM %s WHERE k IN (1,2)");
    }

    @Test
    public void testFilterOnManyPartitions() throws Throwable
    {
        Assertions.assertThatThrownBy(() -> execute("SELECT * FROM %s WHERE k IN (1,2,3,4,5)"))
                  .isInstanceOf(InvalidRequestException.class)
                  .hasMessage("Select query cannot be completed because it selects 5 partitions keys - more than the maximum allowed 3");
    }

    @Test
    public void testFilterOnClusteringColumns() throws Throwable
    {
        // test that it does not throw
        execute("SELECT * FROM %s WHERE c IN (1,2,3,4,5) ALLOW FILTERING");
        execute("SELECT * FROM %s WHERE k = 3 AND c IN (1,2,3,4,5)");
    }

    @Test
    public void testFilterOnOneRepeatedPartitions() throws Throwable
    {
        // test that it does not throw
        execute("SELECT * FROM %s WHERE k IN (1,1,1,1,1)");
    }
}