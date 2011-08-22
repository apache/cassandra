package org.apache.cassandra.db;
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
import org.apache.cassandra.Util;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.db.filter.QueryPath;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessageSerializer;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.thrift.SlicePredicate;
import org.apache.cassandra.thrift.SliceRange;
import org.apache.cassandra.utils.ByteBufferUtil;

import org.junit.Test;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.HashMap;

public class SerializationsTest extends AbstractSerializationsTester
{
    private static MessageSerializer messageSerializer = new MessageSerializer();

    private void testRangeSliceCommandWrite() throws IOException
    {
        ByteBuffer startCol = ByteBufferUtil.bytes("Start");
        ByteBuffer stopCol = ByteBufferUtil.bytes("Stop");
        ByteBuffer emptyCol = ByteBufferUtil.bytes("");
        SlicePredicate namesPred = new SlicePredicate();
        namesPred.column_names = Statics.NamedCols;
        SliceRange emptySliceRange = new SliceRange(emptyCol, emptyCol, false, 100); 
        SliceRange nonEmptySliceRange = new SliceRange(startCol, stopCol, true, 100);
        SlicePredicate emptyRangePred = new SlicePredicate();
        emptyRangePred.slice_range = emptySliceRange;
        SlicePredicate nonEmptyRangePred = new SlicePredicate();
        nonEmptyRangePred.slice_range = nonEmptySliceRange;
        IPartitioner part = StorageService.getPartitioner();
        AbstractBounds bounds = new Range(part.getRandomToken(), part.getRandomToken());
        
        Message namesCmd = new RangeSliceCommand(Statics.KS, "Standard1", null, namesPred, bounds, 100).getMessage(MessagingService.version_);
        Message emptyRangeCmd = new RangeSliceCommand(Statics.KS, "Standard1", null, emptyRangePred, bounds, 100).getMessage(MessagingService.version_);
        Message regRangeCmd = new RangeSliceCommand(Statics.KS, "Standard1", null,  nonEmptyRangePred, bounds, 100).getMessage(MessagingService.version_);
        Message namesCmdSup = new RangeSliceCommand(Statics.KS, "Super1", Statics.SC, namesPred, bounds, 100).getMessage(MessagingService.version_);
        Message emptyRangeCmdSup = new RangeSliceCommand(Statics.KS, "Super1", Statics.SC, emptyRangePred, bounds, 100).getMessage(MessagingService.version_);
        Message regRangeCmdSup = new RangeSliceCommand(Statics.KS, "Super1", Statics.SC,  nonEmptyRangePred, bounds, 100).getMessage(MessagingService.version_);
        
        DataOutputStream dout = getOutput("db.RangeSliceCommand.bin");
        
        messageSerializer.serialize(namesCmd, dout, getVersion());
        messageSerializer.serialize(emptyRangeCmd, dout, getVersion());
        messageSerializer.serialize(regRangeCmd, dout, getVersion());
        messageSerializer.serialize(namesCmdSup, dout, getVersion());
        messageSerializer.serialize(emptyRangeCmdSup, dout, getVersion());
        messageSerializer.serialize(regRangeCmdSup, dout, getVersion());
        dout.close();
    }
    
    @Test
    public void testRangeSliceCommandRead() throws IOException
    {
        if (EXECUTE_WRITES)
            testRangeSliceCommandWrite();
        
        DataInputStream in = getInput("db.RangeSliceCommand.bin");
        for (int i = 0; i < 6; i++)
        {
            Message msg = messageSerializer.deserialize(in, getVersion());
            RangeSliceCommand cmd = RangeSliceCommand.read(msg);
        }
        in.close();
    }
    
    private void testSliceByNamesReadCommandWrite() throws IOException
    {
        SliceByNamesReadCommand standardCmd = new SliceByNamesReadCommand(Statics.KS, Statics.Key, Statics.StandardPath, Statics.NamedCols);
        SliceByNamesReadCommand superCmd = new SliceByNamesReadCommand(Statics.KS, Statics.Key, Statics.SuperPath, Statics.NamedCols);
        
        DataOutputStream out = getOutput("db.SliceByNamesReadCommand.bin");
        SliceByNamesReadCommand.serializer().serialize(standardCmd, out, getVersion());
        SliceByNamesReadCommand.serializer().serialize(superCmd, out, getVersion());
        ReadCommand.serializer().serialize(standardCmd, out, getVersion());
        ReadCommand.serializer().serialize(superCmd, out, getVersion());
        messageSerializer.serialize(standardCmd.getMessage(getVersion()), out, getVersion());
        messageSerializer.serialize(superCmd.getMessage(getVersion()), out, getVersion());
        out.close();
    }
    
    @Test 
    public void testSliceByNamesReadCommandRead() throws IOException
    {
        if (EXECUTE_WRITES)
            testSliceByNamesReadCommandWrite();
        
        DataInputStream in = getInput("db.SliceByNamesReadCommand.bin");
        assert SliceByNamesReadCommand.serializer().deserialize(in, getVersion()) != null;
        assert SliceByNamesReadCommand.serializer().deserialize(in, getVersion()) != null;
        assert ReadCommand.serializer().deserialize(in, getVersion()) != null;
        assert ReadCommand.serializer().deserialize(in, getVersion()) != null;
        assert messageSerializer.deserialize(in, getVersion()) != null;
        assert messageSerializer.deserialize(in, getVersion()) != null;
        in.close();
    }
    
    private void testSliceFromReadCommandWrite() throws IOException
    {
        SliceFromReadCommand standardCmd = new SliceFromReadCommand(Statics.KS, Statics.Key, Statics.StandardPath, Statics.Start, Statics.Stop, true, 100);
        SliceFromReadCommand superCmd = new SliceFromReadCommand(Statics.KS, Statics.Key, Statics.SuperPath, Statics.Start, Statics.Stop, true, 100);
        DataOutputStream out = getOutput("db.SliceFromReadCommand.bin");
        SliceFromReadCommand.serializer().serialize(standardCmd, out, getVersion());
        SliceFromReadCommand.serializer().serialize(superCmd, out, getVersion());
        ReadCommand.serializer().serialize(standardCmd, out, getVersion());
        ReadCommand.serializer().serialize(superCmd, out, getVersion());
        messageSerializer.serialize(standardCmd.getMessage(getVersion()), out, getVersion());
        messageSerializer.serialize(superCmd.getMessage(getVersion()), out, getVersion());
        out.close();
    }
    
    @Test
    public void testSliceFromReadCommandRead() throws IOException
    {
        if (EXECUTE_WRITES)
            testSliceFromReadCommandWrite();
        
        DataInputStream in = getInput("db.SliceFromReadCommand.bin");
        assert SliceFromReadCommand.serializer().deserialize(in, getVersion()) != null;
        assert SliceFromReadCommand.serializer().deserialize(in, getVersion()) != null;
        assert ReadCommand.serializer().deserialize(in, getVersion()) != null;
        assert ReadCommand.serializer().deserialize(in, getVersion()) != null;
        assert messageSerializer.deserialize(in, getVersion()) != null;
        assert messageSerializer.deserialize(in, getVersion()) != null;
        in.close();
    }
    
    private void testRowWrite() throws IOException
    {
        DataOutputStream out = getOutput("db.Row.bin");
        Row.serializer().serialize(Statics.StandardRow, out, getVersion());
        Row.serializer().serialize(Statics.SuperRow, out, getVersion());
        Row.serializer().serialize(Statics.NullRow, out, getVersion());
        out.close();
    }
    
    @Test
    public void testRowRead() throws IOException
    {
        if (EXECUTE_WRITES)
            testRowWrite();
        
        DataInputStream in = getInput("db.Row.bin");
        assert Row.serializer().deserialize(in, getVersion()) != null;
        assert Row.serializer().deserialize(in, getVersion()) != null;
        assert Row.serializer().deserialize(in, getVersion()) != null;
        in.close();
    }
    
    private void restRowMutationWrite() throws IOException
    {
        RowMutation emptyRm = new RowMutation(Statics.KS, Statics.Key);
        RowMutation standardRowRm = new RowMutation(Statics.KS, Statics.StandardRow);
        RowMutation superRowRm = new RowMutation(Statics.KS, Statics.SuperRow);
        RowMutation standardRm = new RowMutation(Statics.KS, Statics.Key);
        standardRm.add(Statics.StandardCf);
        RowMutation superRm = new RowMutation(Statics.KS, Statics.Key);
        superRm.add(Statics.SuperCf);
        Map<Integer, ColumnFamily> mods = new HashMap<Integer, ColumnFamily>();
        mods.put(Statics.StandardCf.metadata().cfId, Statics.StandardCf);
        mods.put(Statics.SuperCf.metadata().cfId, Statics.SuperCf);
        RowMutation mixedRm = new RowMutation(Statics.KS, Statics.Key, mods);
        
        DataOutputStream out = getOutput("db.RowMutation.bin");
        RowMutation.serializer().serialize(emptyRm, out, getVersion());
        RowMutation.serializer().serialize(standardRowRm, out, getVersion());
        RowMutation.serializer().serialize(superRowRm, out, getVersion());
        RowMutation.serializer().serialize(standardRm, out, getVersion());
        RowMutation.serializer().serialize(superRm, out, getVersion());
        RowMutation.serializer().serialize(mixedRm, out, getVersion());
        messageSerializer.serialize(emptyRm.getMessage(getVersion()), out, getVersion());
        messageSerializer.serialize(standardRowRm.getMessage(getVersion()), out, getVersion());
        messageSerializer.serialize(superRowRm.getMessage(getVersion()), out, getVersion());
        messageSerializer.serialize(standardRm.getMessage(getVersion()), out, getVersion());
        messageSerializer.serialize(superRm.getMessage(getVersion()), out, getVersion());
        messageSerializer.serialize(mixedRm.getMessage(getVersion()), out, getVersion());
        out.close(); 
    }
    
    @Test
    public void testRowMutationRead() throws IOException
    {
        if (EXECUTE_WRITES)
            restRowMutationWrite();
        
        DataInputStream in = getInput("db.RowMutation.bin");
        assert RowMutation.serializer().deserialize(in, getVersion()) != null;
        assert RowMutation.serializer().deserialize(in, getVersion()) != null;
        assert RowMutation.serializer().deserialize(in, getVersion()) != null;
        assert RowMutation.serializer().deserialize(in, getVersion()) != null;
        assert RowMutation.serializer().deserialize(in, getVersion()) != null;
        assert RowMutation.serializer().deserialize(in, getVersion()) != null;
        assert messageSerializer.deserialize(in, getVersion()) != null;
        assert messageSerializer.deserialize(in, getVersion()) != null;
        assert messageSerializer.deserialize(in, getVersion()) != null;
        assert messageSerializer.deserialize(in, getVersion()) != null;
        assert messageSerializer.deserialize(in, getVersion()) != null;
        assert messageSerializer.deserialize(in, getVersion()) != null;
        in.close();
    }
    
    public void testTruncateWrite() throws IOException
    {
        Truncation tr = new Truncation(Statics.KS, "Doesn't Really Matter");
        TruncateResponse aff = new TruncateResponse(Statics.KS, "Doesn't Matter Either", true);
        TruncateResponse neg = new TruncateResponse(Statics.KS, "Still Doesn't Matter", false);
        DataOutputStream out = getOutput("db.Truncation.bin");
        Truncation.serializer().serialize(tr, out, getVersion());
        TruncateResponse.serializer().serialize(aff, out, getVersion());
        TruncateResponse.serializer().serialize(neg, out, getVersion());
        messageSerializer.serialize(tr.getMessage(getVersion()), out, getVersion());
        messageSerializer.serialize(TruncateResponse.makeTruncateResponseMessage(tr.getMessage(getVersion()), aff), out, getVersion());
        messageSerializer.serialize(TruncateResponse.makeTruncateResponseMessage(tr.getMessage(getVersion()), neg), out, getVersion());
        // todo: notice how CF names weren't validated.
        out.close();
    }
    
    @Test
    public void testTruncateRead() throws IOException
    {
        if (EXECUTE_WRITES)
            testTruncateWrite();
        
        DataInputStream in = getInput("db.Truncation.bin");
        assert Truncation.serializer().deserialize(in, getVersion()) != null;
        assert TruncateResponse.serializer().deserialize(in, getVersion()) != null;
        assert TruncateResponse.serializer().deserialize(in, getVersion()) != null;
        assert messageSerializer.deserialize(in, getVersion()) != null;
        assert messageSerializer.deserialize(in, getVersion()) != null;
        assert messageSerializer.deserialize(in, getVersion()) != null;
        in.close();
    }
    
    private void testWriteResponseWrite() throws IOException
    {
        WriteResponse aff = new WriteResponse(Statics.KS, Statics.Key, true);
        WriteResponse neg = new WriteResponse(Statics.KS, Statics.Key, false);
        DataOutputStream out = getOutput("db.WriteResponse.bin");
        WriteResponse.serializer().serialize(aff, out, getVersion());
        WriteResponse.serializer().serialize(neg, out, getVersion());
        out.close();
    }
    
    @Test
    public void testWriteResponseRead() throws IOException
    {
        if (EXECUTE_WRITES)
            testWriteResponseWrite();
        
        DataInputStream in = getInput("db.WriteResponse.bin");
        assert WriteResponse.serializer().deserialize(in, getVersion()) != null;
        assert WriteResponse.serializer().deserialize(in, getVersion()) != null;
        in.close();
    }
    
    private static ByteBuffer bb(String s) {
        return ByteBufferUtil.bytes(s);
    }
    
    private static class Statics 
    {
        private static final String KS = "Keyspace1";
        private static final ByteBuffer Key = ByteBufferUtil.bytes("Key01");
        private static final List<ByteBuffer> NamedCols = new ArrayList<ByteBuffer>() 
        {{
            add(ByteBufferUtil.bytes("AAA"));
            add(ByteBufferUtil.bytes("BBB"));
            add(ByteBufferUtil.bytes("CCC"));
        }};
        private static final ByteBuffer SC = ByteBufferUtil.bytes("SCName");
        private static final QueryPath StandardPath = new QueryPath("Standard1");
        private static final QueryPath SuperPath = new QueryPath("Super1", SC);
        private static final ByteBuffer Start = ByteBufferUtil.bytes("Start");
        private static final ByteBuffer Stop = ByteBufferUtil.bytes("Stop");
        
        private static final ColumnFamily StandardCf = ColumnFamily.create(Statics.KS, "Standard1");
        private static final ColumnFamily SuperCf = ColumnFamily.create(Statics.KS, "Super1");
        
        private static final SuperColumn SuperCol = new SuperColumn(Statics.SC, Schema.instance.getComparator(Statics.KS, "Super1"))
        {{
            addColumn(new Column(bb("aaaa")));
            addColumn(new Column(bb("bbbb"), bb("bbbbb-value")));
            addColumn(new Column(bb("cccc"), bb("ccccc-value"), 1000L));
            addColumn(new DeletedColumn(bb("dddd"), 500, 1000));
            addColumn(new DeletedColumn(bb("eeee"), bb("eeee-value"), 1001));
            addColumn(new ExpiringColumn(bb("ffff"), bb("ffff-value"), 2000, 1000));
            addColumn(new ExpiringColumn(bb("gggg"), bb("gggg-value"), 2001, 1000, 2002));
        }};

        private static final Row StandardRow = new Row(Util.dk("key0"), Statics.StandardCf);
        private static final Row SuperRow = new Row(Util.dk("key1"), Statics.SuperCf);
        private static final Row NullRow = new Row(Util.dk("key2"), null);
        
        static {
            StandardCf.addColumn(new Column(bb("aaaa")));
            StandardCf.addColumn(new Column(bb("bbbb"), bb("bbbbb-value")));
            StandardCf.addColumn(new Column(bb("cccc"), bb("ccccc-value"), 1000L));
            StandardCf.addColumn(new DeletedColumn(bb("dddd"), 500, 1000));
            StandardCf.addColumn(new DeletedColumn(bb("eeee"), bb("eeee-value"), 1001));
            StandardCf.addColumn(new ExpiringColumn(bb("ffff"), bb("ffff-value"), 2000, 1000));
            StandardCf.addColumn(new ExpiringColumn(bb("gggg"), bb("gggg-value"), 2001, 1000, 2002));
            
            SuperCf.addColumn(Statics.SuperCol);
        }
    }
}
