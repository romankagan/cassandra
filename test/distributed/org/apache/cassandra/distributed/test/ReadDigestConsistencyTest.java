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

package org.apache.cassandra.distributed.test;

import java.util.Arrays;
import java.util.UUID;

import org.junit.Assert;
import org.junit.Test;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.shared.DistributedTestBase;

public class ReadDigestConsistencyTest extends TestBaseImpl
{
    public static final String TABLE_NAME = "tbl";
    public static final String CREATE_TABLE = String.format("CREATE TABLE %s.%s (key int, s1 text static, c1 text, c2 text, c3 text, PRIMARY KEY (key, c1))", DistributedTestBase.KEYSPACE, TABLE_NAME);
    public static final String INSERT = String.format("INSERT INTO %s.%s (key, s1, c1, c2, c3) VALUES (?, ?, ?, ?, ?)", DistributedTestBase.KEYSPACE, TABLE_NAME);

    public static final String SELECT_C1 = String.format("SELECT key, c1 FROM %s.%s WHERE key = ?", DistributedTestBase.KEYSPACE, TABLE_NAME);
    public static final String SELECT_C1_S1_ROW = String.format("SELECT key, c1, s1 FROM %s.%s WHERE key = ? and c1 = ? ", DistributedTestBase.KEYSPACE, TABLE_NAME);
    public static final String SELECT_TRACE = "SELECT activity FROM system_traces.events where session_id = ? and source = ? ALLOW FILTERING;";

    @Test
    public void testDigestConsistency() throws Exception
    {
        try (Cluster cluster = init(builder().withNodes(2).start()))
        {
            cluster.schemaChange(CREATE_TABLE);
            cluster.coordinator(1).execute(INSERT, ConsistencyLevel.ALL, 1, "static", "foo", "bar", "baz");
            cluster.coordinator(1).execute(INSERT, ConsistencyLevel.ALL, 1, "static", "fi", "biz", "baz");
            cluster.coordinator(1).execute(INSERT, ConsistencyLevel.ALL, 1, "static", "fo", "boz", "baz");

            checkTraceForDigestMismatch(cluster, 1, SELECT_C1, 1);
            checkTraceForDigestMismatch(cluster, 2, SELECT_C1, 1);
            checkTraceForDigestMismatch(cluster, 1, SELECT_C1_S1_ROW, 1, "foo");
            checkTraceForDigestMismatch(cluster, 2, SELECT_C1_S1_ROW, 1, "fi");
        }
    }

    public static void checkTraceForDigestMismatch(Cluster cluster, int coordinatorNode, String query, Object... boundValues)
    {
        UUID sessionId = UUID.randomUUID();
        cluster.coordinator(coordinatorNode).executeWithTracing(sessionId, query, ConsistencyLevel.ALL, boundValues);
        Object[][] results = cluster.coordinator(coordinatorNode)
                                    .execute(SELECT_TRACE, ConsistencyLevel.ALL,
                                             sessionId, cluster.get(coordinatorNode).broadcastAddress().getAddress());
        for (Object[] result : results)
        {
            String activity = (String) result[0];
            Assert.assertFalse(String.format("Found Digest Mismatch while executing query: %s with bound values %s on cluster %s using coordinator node %s", query, Arrays.toString(boundValues), cluster, coordinatorNode), activity.toLowerCase().contains("mismatch for key"));
        }
    }

}
