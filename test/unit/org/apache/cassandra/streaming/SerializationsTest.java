package org.apache.cassandra.streaming;
/*
 * 
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 * 
 */


import org.apache.cassandra.AbstractSerializationsTester;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.RowMutation;
import org.apache.cassandra.db.Table;
import org.apache.cassandra.db.filter.QueryPath;
import org.apache.cassandra.dht.BytesToken;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.SSTable;
import org.apache.cassandra.io.sstable.SSTableReader;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessageSerializer;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Pair;
import org.junit.Ignore;
import org.junit.Test;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;

public class SerializationsTest extends AbstractSerializationsTester
{
    private static MessageSerializer messageSerializer = new MessageSerializer();

    private void testPendingFileWrite() throws IOException
    {
        // make sure to test serializing null and a pf with no sstable.
        PendingFile normal = makePendingFile(true, 100, OperationType.BOOTSTRAP);
        PendingFile noSections = makePendingFile(true, 0, OperationType.AES);
        PendingFile noSST = makePendingFile(false, 100, OperationType.RESTORE_REPLICA_COUNT);

        DataOutputStream out = getOutput("streaming.PendingFile.bin");
        PendingFile.serializer().serialize(normal, out, getVersion());
        PendingFile.serializer().serialize(noSections, out, getVersion());
        PendingFile.serializer().serialize(noSST, out, getVersion());
        PendingFile.serializer().serialize(null, out, getVersion());
        out.close();
    }
    
    @Test
    public void testPendingFileRead() throws IOException
    {
        if (EXECUTE_WRITES)
            testPendingFileWrite();
        
        DataInputStream in = getInput("streaming.PendingFile.bin");
        assert PendingFile.serializer().deserialize(in, getVersion()) != null;
        assert PendingFile.serializer().deserialize(in, getVersion()) != null;
        assert PendingFile.serializer().deserialize(in, getVersion()) != null;
        assert PendingFile.serializer().deserialize(in, getVersion()) == null;
        in.close();
    }
    
    private void testStreamHeaderWrite() throws IOException
    {
        StreamHeader sh0 = new StreamHeader("Keyspace1", 123L, makePendingFile(true, 100, OperationType.BOOTSTRAP));
        StreamHeader sh1 = new StreamHeader("Keyspace1", 124L, makePendingFile(false, 100, OperationType.BOOTSTRAP));
        Collection<PendingFile> files = new ArrayList<PendingFile>();
        for (int i = 0; i < 50; i++)
            files.add(makePendingFile(i % 2 == 0, 100, OperationType.BOOTSTRAP));
        StreamHeader sh2 = new StreamHeader("Keyspace1", 125L, makePendingFile(true, 100, OperationType.BOOTSTRAP), files);
        StreamHeader sh3 = new StreamHeader("Keyspace1", 125L, null, files);
        StreamHeader sh4 = new StreamHeader("Keyspace1", 125L, makePendingFile(true, 100, OperationType.BOOTSTRAP), new ArrayList<PendingFile>());

        DataOutputStream out = getOutput("streaming.StreamHeader.bin");
        StreamHeader.serializer().serialize(sh0, out, getVersion());
        StreamHeader.serializer().serialize(sh1, out, getVersion());
        StreamHeader.serializer().serialize(sh2, out, getVersion());
        StreamHeader.serializer().serialize(sh3, out, getVersion());
        StreamHeader.serializer().serialize(sh4, out, getVersion());
        out.close();
    }
    
    @Test
    public void testStreamHeaderRead() throws IOException
    {
        if (EXECUTE_WRITES)
            testStreamHeaderWrite();
        
        DataInputStream in = getInput("streaming.StreamHeader.bin");
        assert StreamHeader.serializer().deserialize(in, getVersion()) != null;
        assert StreamHeader.serializer().deserialize(in, getVersion()) != null;
        assert StreamHeader.serializer().deserialize(in, getVersion()) != null;
        assert StreamHeader.serializer().deserialize(in, getVersion()) != null;
        assert StreamHeader.serializer().deserialize(in, getVersion()) != null;
        in.close();
    }
    
    private void testStreamReplyWrite() throws IOException
    {
        StreamReply rep = new StreamReply("this is a file", 123L, StreamReply.Status.FILE_FINISHED);
        DataOutputStream out = getOutput("streaming.StreamReply.bin");
        StreamReply.serializer.serialize(rep, out, getVersion());
        messageSerializer.serialize(rep.getMessage(getVersion()), out, getVersion());
        out.close();
    }
    
    @Test
    public void testStreamReplyRead() throws IOException
    {
        if (EXECUTE_WRITES)
            testStreamReplyWrite();
        
        DataInputStream in = getInput("streaming.StreamReply.bin");
        assert StreamReply.serializer.deserialize(in, getVersion()) != null;
        assert messageSerializer.deserialize(in, getVersion()) != null;
        in.close();
    }
    
    private static PendingFile makePendingFile(boolean sst, int numSecs, OperationType op)
    {
        Descriptor desc = new Descriptor("z", new File("path/doesn't/matter"), "Keyspace1", "Standard1", 23, false);
        List<Pair<Long, Long>> sections = new ArrayList<Pair<Long, Long>>();
        for (int i = 0; i < numSecs; i++)
            sections.add(new Pair<Long, Long>(new Long(i), new Long(i * i)));
        return new PendingFile(sst ? makeSSTable() : null, desc, SSTable.COMPONENT_DATA, sections, op);
    }
    
    private void testStreamRequestMessageWrite() throws IOException
    {
        Collection<Range> ranges = new ArrayList<Range>();
        for (int i = 0; i < 5; i++)
            ranges.add(new Range(new BytesToken(ByteBufferUtil.bytes(Integer.toString(10*i))), new BytesToken(ByteBufferUtil.bytes(Integer.toString(10*i+5)))));
        List<ColumnFamilyStore> stores = Collections.singletonList(Table.open("Keyspace1").getColumnFamilyStore("Standard1"));
        StreamRequestMessage msg0 = new StreamRequestMessage(FBUtilities.getBroadcastAddress(), ranges, "Keyspace1", stores, 123L, OperationType.RESTORE_REPLICA_COUNT);
        StreamRequestMessage msg1 = new StreamRequestMessage(FBUtilities.getBroadcastAddress(), makePendingFile(true, 100, OperationType.BOOTSTRAP), 124L);
        StreamRequestMessage msg2 = new StreamRequestMessage(FBUtilities.getBroadcastAddress(), makePendingFile(false, 100, OperationType.BOOTSTRAP), 124L);

        DataOutputStream out = getOutput("streaming.StreamRequestMessage.bin");
        StreamRequestMessage.serializer().serialize(msg0, out, getVersion());
        StreamRequestMessage.serializer().serialize(msg1, out, getVersion());
        StreamRequestMessage.serializer().serialize(msg2, out, getVersion());
        messageSerializer.serialize(msg0.getMessage(getVersion()), out, getVersion());
        messageSerializer.serialize(msg1.getMessage(getVersion()), out, getVersion());
        messageSerializer.serialize(msg2.getMessage(getVersion()), out, getVersion());
        out.close();
    }
    
    @Test
    public void testStreamRequestMessageRead() throws IOException
    {
        if (EXECUTE_WRITES)
            testStreamRequestMessageWrite();
        
        DataInputStream in = getInput("streaming.StreamRequestMessage.bin");
        assert StreamRequestMessage.serializer().deserialize(in, getVersion()) != null;
        assert StreamRequestMessage.serializer().deserialize(in, getVersion()) != null;
        assert StreamRequestMessage.serializer().deserialize(in, getVersion()) != null;
        assert messageSerializer.deserialize(in, getVersion()) != null;
        assert messageSerializer.deserialize(in, getVersion()) != null;
        assert messageSerializer.deserialize(in, getVersion()) != null;
        in.close();
    }
    
    private static SSTableReader makeSSTable()
    {
        Table t = Table.open("Keyspace1");
        for (int i = 0; i < 100; i++)
        {
            RowMutation rm = new RowMutation(t.name, ByteBufferUtil.bytes(Long.toString(System.nanoTime())));
            rm.add(new QueryPath("Standard1", null, ByteBufferUtil.bytes("cola")), ByteBufferUtil.bytes("value"), 0);
            try
            {
                rm.apply();
            }
            catch (IOException ex) 
            {
                throw new RuntimeException(ex);
            }
        }
        try
        {
            t.getColumnFamilyStore("Standard1").forceBlockingFlush();
            return t.getColumnFamilyStore("Standard1").getSSTables().iterator().next();
        }
        catch (Exception any)
        {
            throw new RuntimeException(any);
        }
    }
}
