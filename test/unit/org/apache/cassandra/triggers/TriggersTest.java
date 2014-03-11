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
package org.apache.cassandra.triggers;

import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Collections;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.ArrayBackedSortedColumns;
import org.apache.cassandra.db.Cell;
import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.composites.CellNames;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.ColumnOrSuperColumn;
import org.apache.cassandra.thrift.ColumnParent;
import org.apache.cassandra.thrift.TFramedTransportFactory;
import org.apache.cassandra.thrift.ThriftServer;
import org.apache.thrift.protocol.TBinaryProtocol;

import static org.apache.cassandra.utils.ByteBufferUtil.bytes;
import static org.junit.Assert.assertEquals;

public class TriggersTest extends SchemaLoader
{
    private static boolean triggerCreated = false;
    private static ThriftServer thriftServer;

    private static String ksName = "triggers_test_ks";
    private static String cfName = "test_table";

    @Before
    public void setup() throws Exception
    {
        StorageService.instance.initServer(0);
        if (thriftServer == null || ! thriftServer.isRunning())
        {
            thriftServer = new ThriftServer(InetAddress.getLocalHost(), 9170, 50);
            thriftServer.start();
        }

        String cql = String.format("CREATE KEYSPACE IF NOT EXISTS %s " +
                                   "WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1}",
                                   ksName);
        QueryProcessor.process(cql, ConsistencyLevel.ONE);

        cql = String.format("CREATE TABLE IF NOT EXISTS %s.%s (k int, v1 int, v2 int, PRIMARY KEY (k))", ksName, cfName);
        QueryProcessor.process(cql, ConsistencyLevel.ONE);

        // no conditional execution of create trigger stmt yet
        if (! triggerCreated)
        {
            cql = String.format("CREATE TRIGGER trigger_1 ON %s.%s USING '%s'",
                                ksName, cfName, TestTrigger.class.getName());
            QueryProcessor.process(cql, ConsistencyLevel.ONE);
            triggerCreated = true;
        }
    }

    @AfterClass
    public static void teardown()
    {
        if (thriftServer != null && thriftServer.isRunning())
        {
            thriftServer.stop();
        }
    }

    @Test
    public void executeTriggerOnCqlInsert() throws Exception
    {
        String cql = String.format("INSERT INTO %s.%s (k, v1) VALUES (0, 0)", ksName, cfName);
        QueryProcessor.process(cql, ConsistencyLevel.ONE);
        assertUpdateIsAugmented(0);
    }

    @Test
    public void executeTriggerOnCqlBatchInsert() throws Exception
    {
        String cql = String.format("BEGIN BATCH " +
                                   "    INSERT INTO %s.%s (k, v1) VALUES (1, 1); " +
                                   "APPLY BATCH",
                                   ksName, cfName);
        QueryProcessor.process(cql, ConsistencyLevel.ONE);
        assertUpdateIsAugmented(1);
    }

    @Test
    public void executeTriggerOnThriftInsert() throws Exception
    {
        Cassandra.Client client = new Cassandra.Client(
                                        new TBinaryProtocol(
                                            new TFramedTransportFactory().openTransport(
                                                InetAddress.getLocalHost().getHostName(), 9170)));
        client.set_keyspace(ksName);
        client.insert(bytes(2),
                      new ColumnParent(cfName),
                      getColumnForInsert("v1", 2),
                      org.apache.cassandra.thrift.ConsistencyLevel.ONE);

        assertUpdateIsAugmented(2);
    }

    @Test
    public void executeTriggerOnThriftBatchUpdate() throws Exception
    {
        Cassandra.Client client = new Cassandra.Client(
                                    new TBinaryProtocol(
                                        new TFramedTransportFactory().openTransport(
                                            InetAddress.getLocalHost().getHostName(), 9170)));
        client.set_keyspace(ksName);
        org.apache.cassandra.thrift.Mutation mutation = new org.apache.cassandra.thrift.Mutation();
        ColumnOrSuperColumn cosc = new ColumnOrSuperColumn();
        cosc.setColumn(getColumnForInsert("v1", 3));
        mutation.setColumn_or_supercolumn(cosc);
        client.batch_mutate(
            Collections.singletonMap(bytes(3),
                                     Collections.singletonMap(cfName,
                                                              Collections.singletonList(mutation))),
            org.apache.cassandra.thrift.ConsistencyLevel.ONE);

        assertUpdateIsAugmented(3);
    }

    private void assertUpdateIsAugmented(int key)
    {
        UntypedResultSet rs = QueryProcessor.processInternal(
                                String.format("SELECT * FROM %s.%s WHERE k=%s", ksName, cfName, key));
        assertEquals(999, rs.one().getInt("v2"));
    }

    private org.apache.cassandra.thrift.Column getColumnForInsert(String columnName, int value)
    {
        org.apache.cassandra.thrift.Column column = new org.apache.cassandra.thrift.Column();
        column.setName(Schema.instance.getCFMetaData(ksName, cfName).comparator.asAbstractType().fromString(columnName));
        column.setValue(bytes(value));
        column.setTimestamp(System.currentTimeMillis());
        return column;
    }

    public static class TestTrigger implements ITrigger
    {
        public Collection<Mutation> augment(ByteBuffer key, ColumnFamily update)
        {
            ColumnFamily extraUpdate = update.cloneMeShallow(ArrayBackedSortedColumns.factory, false);
            extraUpdate.addColumn(new Cell(update.metadata().comparator.makeCellName(bytes("v2")),
                                           bytes(999)));
            Mutation mutation = new Mutation(ksName, key);
            mutation.add(extraUpdate);
            return Collections.singletonList(mutation);
        }
    }
}
