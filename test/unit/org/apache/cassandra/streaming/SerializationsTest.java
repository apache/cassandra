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
import org.apache.cassandra.db.RowMutation;
import org.apache.cassandra.db.Table;
import org.apache.cassandra.db.filter.QueryPath;
import org.apache.cassandra.dht.BigIntegerToken;
import org.apache.cassandra.dht.BytesToken;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.SSTable;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Pair;
import org.junit.Test;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class SerializationsTest extends AbstractSerializationsTester
{
    private void testPendingFileWrite() throws IOException
    {
        // make sure to test serializing null and a pf with no sstable.
        PendingFile normal = makePendingFile(true, "fake_component", 100);
        PendingFile noSections = makePendingFile(true, "not_real", 0);
        PendingFile noSST = makePendingFile(false, "also_fake", 100);
        
        DataOutputStream out = getOutput("streaming.PendingFile.bin");
        PendingFile.serializer().serialize(normal, out);
        PendingFile.serializer().serialize(noSections, out);
        PendingFile.serializer().serialize(noSST, out);
        PendingFile.serializer().serialize(null, out);
        out.close();
    }
    
    @Test
    public void testPendingFileRead() throws IOException
    {
        if (EXECUTE_WRITES)
            testPendingFileWrite();
        
        DataInputStream in = getInput("streaming.PendingFile.bin");
        assert PendingFile.serializer().deserialize(in) != null;
        assert PendingFile.serializer().deserialize(in) != null;
        assert PendingFile.serializer().deserialize(in) != null;
        assert PendingFile.serializer().deserialize(in) == null;
        in.close();
    }
    
    private void testStreamHeaderWrite() throws IOException
    {
        StreamHeader sh0 = new StreamHeader("Keyspace1", 123L, makePendingFile(true, "zz", 100));
        StreamHeader sh1 = new StreamHeader("Keyspace1", 124L, makePendingFile(false, "zz", 100));
        Collection<PendingFile> files = new ArrayList<PendingFile>();
        for (int i = 0; i < 50; i++)
            files.add(makePendingFile(i % 2 == 0, "aa", 100));
        StreamHeader sh2 = new StreamHeader("Keyspace1", 125L, makePendingFile(true, "bb", 100), files);
        StreamHeader sh3 = new StreamHeader("Keyspace1", 125L, null, files);
        StreamHeader sh4 = new StreamHeader("Keyspace1", 125L, makePendingFile(true, "bb", 100), new ArrayList<PendingFile>());
        
        DataOutputStream out = getOutput("streaming.StreamHeader.bin");
        StreamHeader.serializer().serialize(sh0, out);
        StreamHeader.serializer().serialize(sh1, out);
        StreamHeader.serializer().serialize(sh2, out);
        StreamHeader.serializer().serialize(sh3, out);
        StreamHeader.serializer().serialize(sh4, out);
        out.close();
    }
    
    @Test
    public void testStreamHeaderRead() throws IOException
    {
        if (EXECUTE_WRITES)
            testStreamHeaderWrite();
        
        DataInputStream in = getInput("streaming.StreamHeader.bin");
        assert StreamHeader.serializer().deserialize(in) != null;
        assert StreamHeader.serializer().deserialize(in) != null;
        assert StreamHeader.serializer().deserialize(in) != null;
        assert StreamHeader.serializer().deserialize(in) != null;
        assert StreamHeader.serializer().deserialize(in) != null;
        in.close();
    }
    
    private void testStreamReplyWrite() throws IOException
    {
        StreamReply rep = new StreamReply("this is a file", 123L, StreamReply.Status.FILE_FINISHED);
        DataOutputStream out = getOutput("streaming.StreamReply.bin");
        StreamReply.serializer.serialize(rep, out);
        Message.serializer().serialize(rep.createMessage(), out);
        out.close();
    }
    
    @Test
    public void testStreamReplyRead() throws IOException
    {
        if (EXECUTE_WRITES)
            testStreamReplyWrite();
        
        DataInputStream in = getInput("streaming.StreamReply.bin");
        assert StreamReply.serializer.deserialize(in) != null;
        assert Message.serializer().deserialize(in) != null;
        in.close();
    }
    
    private static PendingFile makePendingFile(boolean sst, String comp, int numSecs)
    {
        Descriptor desc = new Descriptor("z", new File("path/doesn't/matter"), "Keyspace1", "Standard1", 23, false);
        List<Pair<Long, Long>> sections = new ArrayList<Pair<Long, Long>>();
        for (int i = 0; i < numSecs; i++)
            sections.add(new Pair<Long, Long>(new Long(i), new Long(i * i)));
        return new PendingFile(sst ? makeSSTable() : null, desc, comp, sections);
    }
    
    private void testStreamRequestMessageWrite() throws IOException
    {
        Collection<Range> ranges = new ArrayList<Range>();
        for (int i = 0; i < 5; i++)
            ranges.add(new Range(new BytesToken(ByteBuffer.wrap(Integer.toString(10*i).getBytes())), new BytesToken(ByteBuffer.wrap(Integer.toString(10*i+5).getBytes()))));
        StreamRequestMessage msg0 = new StreamRequestMessage(FBUtilities.getLocalAddress(), ranges, "Keyspace1", 123L);
        StreamRequestMessage msg1 = new StreamRequestMessage(FBUtilities.getLocalAddress(), makePendingFile(true, "aa", 100), 124L);
        StreamRequestMessage msg2 = new StreamRequestMessage(FBUtilities.getLocalAddress(), makePendingFile(false, "aa", 100), 124L);
        
        DataOutputStream out = getOutput("streaming.StreamRequestMessage.bin");
        StreamRequestMessage.serializer().serialize(msg0, out);
        StreamRequestMessage.serializer().serialize(msg1, out);
        StreamRequestMessage.serializer().serialize(msg2, out);
        Message.serializer().serialize(msg0.makeMessage(), out);
        Message.serializer().serialize(msg1.makeMessage(), out);
        Message.serializer().serialize(msg2.makeMessage(), out);
        out.close();
    }
    
    @Test
    public void testStreamRequestMessageRead() throws IOException
    {
        if (EXECUTE_WRITES)
            testStreamRequestMessageWrite();
        
        DataInputStream in = getInput("streaming.StreamRequestMessage.bin");
        assert StreamRequestMessage.serializer().deserialize(in) != null;
        assert StreamRequestMessage.serializer().deserialize(in) != null;
        assert StreamRequestMessage.serializer().deserialize(in) != null;
        assert Message.serializer().deserialize(in) != null;
        assert Message.serializer().deserialize(in) != null;
        assert Message.serializer().deserialize(in) != null;
        in.close();
    }
    
    private static SSTable makeSSTable()
    {
        Table t = Table.open("Keyspace1");
        for (int i = 0; i < 100; i++)
        {
            RowMutation rm = new RowMutation(t.name, ByteBuffer.wrap(Long.toString(System.nanoTime()).getBytes()));
            rm.add(new QueryPath("Standard1", null, ByteBuffer.wrap("cola".getBytes())), ByteBuffer.wrap("value".getBytes()), 0);
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
