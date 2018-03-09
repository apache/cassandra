/*
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing,
* software distributed under the License is distributed on an
* "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
* KIND, either express or implied.  See the License for the
* specific language governing permissions and limitations
* under the License.
*/
package org.apache.cassandra.db;

import static org.junit.Assert.*;

import java.io.*;

import com.google.common.base.Predicate;

import org.junit.BeforeClass;
import org.junit.Test;
import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.Util;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.commitlog.CommitLogTestReplayer;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.marshal.AsciiType;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.db.partitions.FilteredPartition;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.utils.ByteBufferUtil;

public class ReadMessageTest
{
    private static final String KEYSPACE1 = "ReadMessageTest1";
    private static final String KEYSPACENOCOMMIT = "ReadMessageTest_NoCommit";
    private static final String CF = "Standard1";
    private static final String CF_FOR_READ_TEST = "Standard2";
    private static final String CF_FOR_COMMIT_TEST = "Standard3";

    @BeforeClass
    public static void defineSchema() throws ConfigurationException
    {
        DatabaseDescriptor.daemonInitialization();

        TableMetadata.Builder cfForReadMetadata =
            TableMetadata.builder(KEYSPACE1, CF_FOR_READ_TEST)
                         .addPartitionKeyColumn("key", BytesType.instance)
                         .addClusteringColumn("col1", AsciiType.instance)
                         .addClusteringColumn("col2", AsciiType.instance)
                         .addRegularColumn("a", AsciiType.instance)
                         .addRegularColumn("b", AsciiType.instance);

        TableMetadata.Builder cfForCommitMetadata1 =
            TableMetadata.builder(KEYSPACE1, CF_FOR_COMMIT_TEST)
                         .addPartitionKeyColumn("key", BytesType.instance)
                         .addClusteringColumn("name", AsciiType.instance)
                         .addRegularColumn("commit1", AsciiType.instance);

        TableMetadata.Builder cfForCommitMetadata2 =
            TableMetadata.builder(KEYSPACENOCOMMIT, CF_FOR_COMMIT_TEST)
                         .addPartitionKeyColumn("key", BytesType.instance)
                         .addClusteringColumn("name", AsciiType.instance)
                         .addRegularColumn("commit2", AsciiType.instance);

        SchemaLoader.prepareServer();

        SchemaLoader.createKeyspace(KEYSPACE1,
                                    KeyspaceParams.simple(1),
                                    SchemaLoader.standardCFMD(KEYSPACE1, CF),
                                    cfForReadMetadata,
                                    cfForCommitMetadata1);

        SchemaLoader.createKeyspace(KEYSPACENOCOMMIT,
                                    KeyspaceParams.simpleTransient(1),
                                    SchemaLoader.standardCFMD(KEYSPACENOCOMMIT, CF),
                                    cfForCommitMetadata2);
    }

    @Test
    public void testMakeReadMessage() throws IOException
    {
        ColumnFamilyStore cfs = Keyspace.open(KEYSPACE1).getColumnFamilyStore(CF_FOR_READ_TEST);
        ReadCommand rm, rm2;

        rm = Util.cmd(cfs, Util.dk("key1"))
                 .includeRow("col1", "col2")
                 .build();
        rm2 = serializeAndDeserializeReadMessage(rm);
        assertEquals(rm.toString(), rm2.toString());

        rm = Util.cmd(cfs, Util.dk("key1"))
                 .includeRow("col1", "col2")
                 .reverse()
                 .build();
        rm2 = serializeAndDeserializeReadMessage(rm);
        assertEquals(rm.toString(), rm2.toString());

        rm = Util.cmd(cfs)
                 .build();
        rm2 = serializeAndDeserializeReadMessage(rm);
        assertEquals(rm.toString(), rm2.toString());

        rm = Util.cmd(cfs)
                 .fromKeyIncl(ByteBufferUtil.bytes("key1"))
                 .toKeyIncl(ByteBufferUtil.bytes("key2"))
                 .build();
        rm2 = serializeAndDeserializeReadMessage(rm);
        assertEquals(rm.toString(), rm2.toString());

        rm = Util.cmd(cfs)
                 .columns("a")
                 .build();
        rm2 = serializeAndDeserializeReadMessage(rm);
        assertEquals(rm.toString(), rm2.toString());

        rm = Util.cmd(cfs)
                 .includeRow("col1", "col2")
                 .columns("a")
                 .build();
        rm2 = serializeAndDeserializeReadMessage(rm);
        assertEquals(rm.toString(), rm2.toString());

        rm = Util.cmd(cfs)
                 .fromKeyIncl(ByteBufferUtil.bytes("key1"))
                 .includeRow("col1", "col2")
                 .columns("a")
                 .build();
        rm2 = serializeAndDeserializeReadMessage(rm);
        assertEquals(rm.toString(), rm2.toString());
    }

    private ReadCommand serializeAndDeserializeReadMessage(ReadCommand rm) throws IOException
    {
        IVersionedSerializer<ReadCommand> rms = ReadCommand.serializer;
        DataOutputBuffer out = new DataOutputBuffer();

        rms.serialize(rm, out, MessagingService.current_version);

        DataInputPlus dis = new DataInputBuffer(out.getData());
        return rms.deserialize(dis, MessagingService.current_version);
    }


    @Test
    public void testGetColumn()
    {
        ColumnFamilyStore cfs = Keyspace.open(KEYSPACE1).getColumnFamilyStore(CF);

        new RowUpdateBuilder(cfs.metadata(), 0, ByteBufferUtil.bytes("key1"))
                .clustering("Column1")
                .add("val", ByteBufferUtil.bytes("abcd"))
                .build()
                .apply();

        ColumnMetadata col = cfs.metadata().getColumn(ByteBufferUtil.bytes("val"));
        int found = 0;
        for (FilteredPartition partition : Util.getAll(Util.cmd(cfs).build()))
        {
            for (Row r : partition)
            {
                if (r.getCell(col).value().equals(ByteBufferUtil.bytes("abcd")))
                    ++found;
            }
        }
        assertEquals(1, found);
    }

    @Test
    public void testNoCommitLog() throws Exception
    {
        ColumnFamilyStore cfs = Keyspace.open(KEYSPACE1).getColumnFamilyStore(CF_FOR_COMMIT_TEST);

        ColumnFamilyStore cfsnocommit = Keyspace.open(KEYSPACENOCOMMIT).getColumnFamilyStore(CF_FOR_COMMIT_TEST);

        new RowUpdateBuilder(cfs.metadata(), 0, ByteBufferUtil.bytes("row"))
                .clustering("c")
                .add("commit1", ByteBufferUtil.bytes("abcd"))
                .build()
                .apply();

        new RowUpdateBuilder(cfsnocommit.metadata(), 0, ByteBufferUtil.bytes("row"))
                .clustering("c")
                .add("commit2", ByteBufferUtil.bytes("abcd"))
                .build()
                .apply();

        Checker checker = new Checker(cfs.metadata().getColumn(ByteBufferUtil.bytes("commit1")),
                                      cfsnocommit.metadata().getColumn(ByteBufferUtil.bytes("commit2")));

        CommitLogTestReplayer replayer = new CommitLogTestReplayer(checker);
        replayer.examineCommitLog();

        assertTrue(checker.commitLogMessageFound);
        assertFalse(checker.noCommitLogMessageFound);
    }

    static class Checker implements Predicate<Mutation>
    {
        private final ColumnMetadata withCommit;
        private final ColumnMetadata withoutCommit;

        boolean commitLogMessageFound = false;
        boolean noCommitLogMessageFound = false;

        public Checker(ColumnMetadata withCommit, ColumnMetadata withoutCommit)
        {
            this.withCommit = withCommit;
            this.withoutCommit = withoutCommit;
        }

        public boolean apply(Mutation mutation)
        {
            for (PartitionUpdate upd : mutation.getPartitionUpdates())
            {
                Row r = upd.getRow(Clustering.make(ByteBufferUtil.bytes("c")));
                if (r != null)
                {
                    if (r.getCell(withCommit) != null)
                        commitLogMessageFound = true;
                    if (r.getCell(withoutCommit) != null)
                        noCommitLogMessageFound = true;
                }
            }
            return true;
        }
    }
}
