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

import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.Util;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.filter.NamesQueryFilter;
import org.apache.cassandra.db.filter.SliceQueryFilter;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.utils.ByteBufferUtil;


public class ReadMessageTest extends SchemaLoader
{
    @Test
    public void testMakeReadMessage() throws IOException
    {
        SortedSet<ByteBuffer> colList = new TreeSet<ByteBuffer>();
        colList.add(ByteBufferUtil.bytes("col1"));
        colList.add(ByteBufferUtil.bytes("col2"));

        ReadCommand rm, rm2;
        DecoratedKey dk = Util.dk("row1");
        long ts = System.currentTimeMillis();

        rm = new SliceByNamesReadCommand("Keyspace1", dk.key, "Standard1", ts, new NamesQueryFilter(colList));
        rm2 = serializeAndDeserializeReadMessage(rm);
        assert rm2.toString().equals(rm.toString());

        rm = new SliceFromReadCommand("Keyspace1", dk.key, "Standard1", ts, new SliceQueryFilter(ByteBufferUtil.EMPTY_BYTE_BUFFER, ByteBufferUtil.EMPTY_BYTE_BUFFER, true, 2));
        rm2 = serializeAndDeserializeReadMessage(rm);
        assert rm2.toString().equals(rm.toString());

        rm = new SliceFromReadCommand("Keyspace1", dk.key, "Standard1", ts, new SliceQueryFilter(ByteBufferUtil.bytes("a"), ByteBufferUtil.bytes("z"), true, 5));
        rm2 = serializeAndDeserializeReadMessage(rm);
        assertEquals(rm2.toString(), rm.toString());

        rm = new SliceFromReadCommand("Keyspace1", dk.key, "Standard1", ts, new SliceQueryFilter(ByteBufferUtil.EMPTY_BYTE_BUFFER, ByteBufferUtil.EMPTY_BYTE_BUFFER, true, 2));
        rm2 = serializeAndDeserializeReadMessage(rm);
        assert rm2.toString().equals(rm.toString());

        rm = new SliceFromReadCommand("Keyspace1", dk.key, "Standard1", ts, new SliceQueryFilter(ByteBufferUtil.bytes("a"), ByteBufferUtil.bytes("z"), true, 5));
        rm2 = serializeAndDeserializeReadMessage(rm);
        assertEquals(rm2.toString(), rm.toString());
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
    public void testGetColumn() throws IOException, ColumnFamilyNotDefinedException
    {
        Keyspace keyspace = Keyspace.open("Keyspace1");
        RowMutation rm;
        DecoratedKey dk = Util.dk("key1");

        // add data
        rm = new RowMutation("Keyspace1", dk.key);
        rm.add("Standard1", ByteBufferUtil.bytes("Column1"), ByteBufferUtil.bytes("abcd"), 0);
        rm.apply();

        ReadCommand command = new SliceByNamesReadCommand("Keyspace1", dk.key, "Standard1", System.currentTimeMillis(), new NamesQueryFilter(ByteBufferUtil.bytes("Column1")));
        Row row = command.getRow(keyspace);
        Column col = row.cf.getColumn(ByteBufferUtil.bytes("Column1"));
        assertEquals(col.value(), ByteBuffer.wrap("abcd".getBytes()));
    }

    @Test
    public void testNoCommitLog() throws Exception
    {

        RowMutation rm = new RowMutation("Keyspace1", ByteBufferUtil.bytes("row"));
        rm.add("Standard1", ByteBufferUtil.bytes("commit1"), ByteBufferUtil.bytes("abcd"), 0);
        rm.apply();

        rm = new RowMutation("NoCommitlogSpace", ByteBufferUtil.bytes("row"));
        rm.add("Standard1", ByteBufferUtil.bytes("commit2"), ByteBufferUtil.bytes("abcd"), 0);
        rm.apply();

        boolean commitLogMessageFound = false;
        boolean noCommitLogMessageFound = false;

        File commitLogDir = new File(DatabaseDescriptor.getCommitLogLocation());

        byte[] commitBytes = "commit".getBytes("UTF-8");

        for(String filename : commitLogDir.list())
        {
            BufferedInputStream is = null;
            try
            {
                is = new BufferedInputStream(new FileInputStream(commitLogDir.getAbsolutePath()+File.separator+filename));

                if (!isEmptyCommitLog(is))
                {
                    while (findPatternInStream(commitBytes, is))
                    {
                        char c = (char)is.read();

                        if (c == '1')
                            commitLogMessageFound = true;
                        else if (c == '2')
                            noCommitLogMessageFound = true;
                    }
                }
            }
            finally
            {
                if (is != null)
                    is.close();
            }
        }

        assertTrue(commitLogMessageFound);
        assertFalse(noCommitLogMessageFound);
    }

    private boolean isEmptyCommitLog(BufferedInputStream is) throws IOException
    {
        DataInputStream dis = new DataInputStream(is);
        byte[] lookahead = new byte[100];

        dis.mark(100);
        dis.readFully(lookahead);
        dis.reset();

        for (int i = 0; i < 100; i++)
        {
            if (lookahead[i] != 0)
                return false;
        }

        return true;
    }

    private boolean findPatternInStream(byte[] pattern, InputStream is) throws IOException
    {
        int patternOffset = 0;

        int b = is.read();
        while (b != -1)
        {
            if (pattern[patternOffset] == ((byte) b))
            {
                patternOffset++;
                if (patternOffset == pattern.length)
                    return true;
            }
            else
                patternOffset = 0;

            b = is.read();
        }

        return false;
    }
}
