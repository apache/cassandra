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
import java.nio.ByteBuffer;
import java.util.SortedSet;
import java.util.TreeSet;

import com.google.common.base.Predicate;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.Util;
import org.apache.cassandra.config.KSMetaData;
import org.apache.cassandra.db.commitlog.CommitLogTestReplayer;
import org.apache.cassandra.db.composites.*;
import org.apache.cassandra.db.filter.NamesQueryFilter;
import org.apache.cassandra.db.filter.SliceQueryFilter;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.locator.SimpleStrategy;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;


public class ReadMessageTest
{
    private static final String KEYSPACE1 = "ReadMessageTest1";
    private static final String KEYSPACENOCOMMIT = "ReadMessageTest_NoCommit";
    private static final String CF = "Standard1";

    @BeforeClass
    public static void defineSchema() throws ConfigurationException
    {
        SchemaLoader.prepareServer();
        SchemaLoader.createKeyspace(KEYSPACE1,
                                    SimpleStrategy.class,
                                    KSMetaData.optsWithRF(1),
                                    SchemaLoader.standardCFMD(KEYSPACE1, CF));
        SchemaLoader.createKeyspace(KEYSPACENOCOMMIT,
                                    false,
                                    true,
                                    SimpleStrategy.class,
                                    KSMetaData.optsWithRF(1),
                                    SchemaLoader.standardCFMD(KEYSPACENOCOMMIT, CF));
    }

    @Test
    public void testMakeReadMessage() throws IOException
    {
        CellNameType type = Keyspace.open(KEYSPACE1).getColumnFamilyStore("Standard1").getComparator();

        SortedSet<CellName> colList = new TreeSet<CellName>(type);
        colList.add(Util.cellname("col1"));
        colList.add(Util.cellname("col2"));

        ReadCommand rm, rm2;
        DecoratedKey dk = Util.dk("row1");
        long ts = System.currentTimeMillis();

        rm = new SliceByNamesReadCommand(KEYSPACE1, dk.getKey(), "Standard1", ts, new NamesQueryFilter(colList));
        rm2 = serializeAndDeserializeReadMessage(rm);
        assert rm2.toString().equals(rm.toString());

        rm = new SliceFromReadCommand(KEYSPACE1, dk.getKey(), "Standard1", ts, new SliceQueryFilter(Composites.EMPTY, Composites.EMPTY, true, 2));
        rm2 = serializeAndDeserializeReadMessage(rm);
        assert rm2.toString().equals(rm.toString());

        rm = new SliceFromReadCommand(KEYSPACE1, dk.getKey(), "Standard1", ts, new SliceQueryFilter(Util.cellname("a"), Util.cellname("z"), true, 5));
        rm2 = serializeAndDeserializeReadMessage(rm);
        assert rm2.toString().equals(rm.toString());
    }

    private ReadCommand serializeAndDeserializeReadMessage(ReadCommand rm) throws IOException
    {
        ReadCommandSerializer rms = ReadCommand.serializer;
        DataOutputBuffer out = new DataOutputBuffer();
        ByteArrayInputStream bis;

        rms.serialize(rm, out, MessagingService.current_version);
        bis = new ByteArrayInputStream(out.getData(), 0, out.getLength());
        return rms.deserialize(new DataInputStream(bis), MessagingService.current_version);
    }

    @Test
    public void testGetColumn()
    {
        Keyspace keyspace = Keyspace.open(KEYSPACE1);
        CellNameType type = keyspace.getColumnFamilyStore("Standard1").getComparator();
        Mutation rm;
        DecoratedKey dk = Util.dk("key1");

        // add data
        rm = new Mutation(KEYSPACE1, dk.getKey());
        rm.add("Standard1", Util.cellname("Column1"), ByteBufferUtil.bytes("abcd"), 0);
        rm.apply();

        ReadCommand command = new SliceByNamesReadCommand(KEYSPACE1, dk.getKey(), "Standard1", System.currentTimeMillis(), new NamesQueryFilter(FBUtilities.singleton(Util.cellname("Column1"), type)));
        Row row = command.getRow(keyspace);
        Cell col = row.cf.getColumn(Util.cellname("Column1"));
        assertEquals(col.value(), ByteBuffer.wrap("abcd".getBytes()));
    }

    @Test
    public void testNoCommitLog() throws Exception
    {
        Mutation rm = new Mutation(KEYSPACE1, ByteBufferUtil.bytes("row"));
        rm.add("Standard1", Util.cellname("commit1"), ByteBufferUtil.bytes("abcd"), 0);
        rm.apply();

        rm = new Mutation(KEYSPACENOCOMMIT, ByteBufferUtil.bytes("row"));
        rm.add("Standard1", Util.cellname("commit2"), ByteBufferUtil.bytes("abcd"), 0);
        rm.apply();

        Checker checker = new Checker();
        CommitLogTestReplayer.examineCommitLog(checker);

        assertTrue(checker.commitLogMessageFound);
        assertFalse(checker.noCommitLogMessageFound);
    }

    static class Checker implements Predicate<Mutation>
    {
        boolean commitLogMessageFound = false;
        boolean noCommitLogMessageFound = false;

        public boolean apply(Mutation mutation)
        {
            for (ColumnFamily cf : mutation.getColumnFamilies())
            {
                if (cf.getColumn(Util.cellname("commit1")) != null)
                    commitLogMessageFound = true;
                if (cf.getColumn(Util.cellname("commit2")) != null)
                    noCommitLogMessageFound = true;
            }
            return true;
        }
    }
}
