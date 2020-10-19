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

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Objects;

import org.junit.Test;

import com.datastax.driver.core.Session;
import org.apache.cassandra.db.marshal.CompositeType;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.api.NodeToolResult;
import org.apache.cassandra.distributed.api.QueryResults;
import org.apache.cassandra.distributed.api.SimpleQueryResult;
import org.apache.cassandra.distributed.shared.AssertUtils;
import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.ColumnOrSuperColumn;
import org.apache.cassandra.thrift.Mutation;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.thrift.TException;
import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;

public class ClientNetworkStopStartTest extends TestBaseImpl
{
    /**
     * @see <a href="https://issues.apache.org/jira/browse/CASSANDRA-16127">CASSANDRA-16127</a>
     */
    @Test
    public void stopStartThrift() throws IOException, TException
    {
        try (Cluster cluster = init(Cluster.build(1).withConfig(c -> c.with(Feature.NATIVE_PROTOCOL)).start()))
        {
            IInvokableInstance node = cluster.get(1);
            assertTransportStatus(node, "binary", true);
            assertTransportStatus(node, "thrift", true);
            node.nodetoolResult("disablethrift").asserts().success();
            assertTransportStatus(node, "binary", true);
            assertTransportStatus(node, "thrift", false);
            node.nodetoolResult("enablethrift").asserts().success();
            assertTransportStatus(node, "binary", true);
            assertTransportStatus(node, "thrift", true);

            // now use it to make sure it still works!
            cluster.schemaChange("CREATE TABLE " + KEYSPACE + ".tbl (pk int, value int, PRIMARY KEY (pk))");

            ThriftClientUtils.thriftClient(node, thrift -> {
                thrift.set_keyspace(KEYSPACE);
                Mutation mutation = new Mutation();
                ColumnOrSuperColumn csoc = new ColumnOrSuperColumn();
                Column column = new Column();
                column.setName(CompositeType.build(ByteBufferUtil.bytes("value")));
                column.setValue(ByteBufferUtil.bytes(0));
                column.setTimestamp(System.currentTimeMillis());
                csoc.setColumn(column);
                mutation.setColumn_or_supercolumn(csoc);

                thrift.batch_mutate(Collections.singletonMap(ByteBufferUtil.bytes(0),
                                                             Collections.singletonMap("tbl", Arrays.asList(mutation))),
                                    org.apache.cassandra.thrift.ConsistencyLevel.ALL);
            });

            SimpleQueryResult qr = cluster.coordinator(1).executeWithResult("SELECT * FROM " + KEYSPACE + ".tbl", ConsistencyLevel.ALL);
            AssertUtils.assertRows(qr, QueryResults.builder().row(0, 0).build());
        }
    }

    /**
     * @see <a href="https://issues.apache.org/jira/browse/CASSANDRA-16127">CASSANDRA-16127</a>
     */
    @Test
    public void stopStartNative() throws IOException
    {
        try (Cluster cluster = init(Cluster.build(1).withConfig(c -> c.with(Feature.NATIVE_PROTOCOL)).start()))
        {
            IInvokableInstance node = cluster.get(1);
            assertTransportStatus(node, "binary", true);
            assertTransportStatus(node, "thrift", true);
            node.nodetoolResult("disablebinary").asserts().success();
            assertTransportStatus(node, "binary", false);
            assertTransportStatus(node, "thrift", true);
            node.nodetoolResult("enablebinary").asserts().success();
            assertTransportStatus(node, "binary", true);
            assertTransportStatus(node, "thrift", true);

            // now use it to make sure it still works!
            cluster.schemaChange("CREATE TABLE " + KEYSPACE + ".tbl (pk int, value int, PRIMARY KEY (pk))");

            try (com.datastax.driver.core.Cluster client = com.datastax.driver.core.Cluster.builder().addContactPoints(node.broadcastAddress().getAddress()).build();
                 Session session = client.connect())
            {
                session.execute("INSERT INTO " + KEYSPACE + ".tbl (pk, value) VALUES (?, ?)", 0, 0);
            }

            SimpleQueryResult qr = cluster.coordinator(1).executeWithResult("SELECT * FROM " + KEYSPACE + ".tbl", ConsistencyLevel.ALL);
            AssertUtils.assertRows(qr, QueryResults.builder().row(0, 0).build());
        }
    }

    private static void assertTransportStatus(IInvokableInstance node, String transport, boolean running)
    {
        assertNodetoolStdout(node, running ? "running" : "not running", running ? "not running" : null, "status" + transport);
    }

    private static void assertNodetoolStdout(IInvokableInstance node, String expectedStatus, String notExpected, String... nodetool)
    {
        NodeToolResult.Asserts asserts = node.nodetoolResult(nodetool).asserts();
        asserts.stdoutContains(expectedStatus);
        if (notExpected != null)
            asserts.stdoutNotContains(notExpected);
    }

    private static final class StringContains extends BaseMatcher<String>
    {
        private final String expected;

        private StringContains(String expected)
        {
            this.expected = Objects.requireNonNull(expected);
        }

        public boolean matches(Object o)
        {
            return o.toString().contains(expected);
        }

        public void describeTo(Description description)
        {
            description.appendText("Expected to find '" + expected + "', but did not");
        }
    }

    private static final class StringNotContains extends BaseMatcher<String>
    {
        private final String notExpected;

        private StringNotContains(String expected)
        {
            this.notExpected = Objects.requireNonNull(expected);
        }

        public boolean matches(Object o)
        {
            return !o.toString().contains(notExpected);
        }

        public void describeTo(Description description)
        {
            description.appendText("Expected not to find '" + notExpected + "', but did");
        }
    }
}
