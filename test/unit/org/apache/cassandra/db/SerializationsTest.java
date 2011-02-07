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
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.filter.QueryPath;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.thrift.SlicePredicate;
import org.apache.cassandra.thrift.SliceRange;
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
    private void testRangeSliceCommandWrite() throws IOException
    {
        ByteBuffer startCol = ByteBuffer.wrap("Start".getBytes());
        ByteBuffer stopCol = ByteBuffer.wrap("Stop".getBytes());
        ByteBuffer emptyCol = ByteBuffer.wrap("".getBytes());
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
        
        Message.serializer().serialize(namesCmd, dout);
        Message.serializer().serialize(emptyRangeCmd, dout);
        Message.serializer().serialize(regRangeCmd, dout);
        Message.serializer().serialize(namesCmdSup, dout);
        Message.serializer().serialize(emptyRangeCmdSup, dout);
        Message.serializer().serialize(regRangeCmdSup, dout);
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
            Message msg = Message.serializer().deserialize(in);
            RangeSliceCommand cmd = RangeSliceCommand.read(msg);
        }
        in.close();
    }
    
    private void testSliceByNamesReadCommandWrite() throws IOException
    {
        SliceByNamesReadCommand standardCmd = new SliceByNamesReadCommand(Statics.KS, Statics.Key, Statics.StandardPath, Statics.NamedCols);
        SliceByNamesReadCommand superCmd = new SliceByNamesReadCommand(Statics.KS, Statics.Key, Statics.SuperPath, Statics.NamedCols);
        
        DataOutputStream out = getOutput("db.SliceByNamesReadCommand.bin");
        SliceByNamesReadCommand.serializer().serialize(standardCmd, out);
        SliceByNamesReadCommand.serializer().serialize(superCmd, out);
        ReadCommand.serializer().serialize(standardCmd, out);
        ReadCommand.serializer().serialize(superCmd, out);
        Message.serializer().serialize(standardCmd.getMessage(MessagingService.version_), out);
        Message.serializer().serialize(superCmd.getMessage(MessagingService.version_), out);
        out.close();
    }
    
    @Test 
    public void testSliceByNamesReadCommandRead() throws IOException
    {
        if (EXECUTE_WRITES)
            testSliceByNamesReadCommandWrite();
        
        DataInputStream in = getInput("db.SliceByNamesReadCommand.bin");
        assert SliceByNamesReadCommand.serializer().deserialize(in) != null;
        assert SliceByNamesReadCommand.serializer().deserialize(in) != null;
        assert ReadCommand.serializer().deserialize(in) != null;
        assert ReadCommand.serializer().deserialize(in) != null;
        assert Message.serializer().deserialize(in) != null;
        assert Message.serializer().deserialize(in) != null;
        in.close();
    }
    
    private void testSliceFromReadCommandWrite() throws IOException
    {
        SliceFromReadCommand standardCmd = new SliceFromReadCommand(Statics.KS, Statics.Key, Statics.StandardPath, Statics.Start, Statics.Stop, true, 100);
        SliceFromReadCommand superCmd = new SliceFromReadCommand(Statics.KS, Statics.Key, Statics.SuperPath, Statics.Start, Statics.Stop, true, 100);
        DataOutputStream out = getOutput("db.SliceFromReadCommand.bin");
        SliceFromReadCommand.serializer().serialize(standardCmd, out);
        SliceFromReadCommand.serializer().serialize(superCmd, out);
        ReadCommand.serializer().serialize(standardCmd, out);
        ReadCommand.serializer().serialize(superCmd, out);
        Message.serializer().serialize(standardCmd.getMessage(MessagingService.version_), out);
        Message.serializer().serialize(superCmd.getMessage(MessagingService.version_), out);
        out.close();
    }
    
    @Test
    public void testSliceFromReadCommandRead() throws IOException
    {
        if (EXECUTE_WRITES)
            testSliceFromReadCommandWrite();
        
        DataInputStream in = getInput("db.SliceFromReadCommand.bin");
        assert SliceFromReadCommand.serializer().deserialize(in) != null;
        assert SliceFromReadCommand.serializer().deserialize(in) != null;
        assert ReadCommand.serializer().deserialize(in) != null;
        assert ReadCommand.serializer().deserialize(in) != null;
        assert Message.serializer().deserialize(in) != null;
        assert Message.serializer().deserialize(in) != null;
        in.close();
    }
    
    private void testRowWrite() throws IOException
    {
        DataOutputStream out = getOutput("db.Row.bin");
        Row.serializer().serialize(Statics.StandardRow, out);
        Row.serializer().serialize(Statics.SuperRow, out);
        Row.serializer().serialize(Statics.NullRow, out);
        out.close();
    }
    
    @Test
    public void testRowRead() throws IOException
    {
        if (EXECUTE_WRITES)
            testRowWrite();
        
        DataInputStream in = getInput("db.Row.bin");
        assert Row.serializer().deserialize(in) != null;
        assert Row.serializer().deserialize(in) != null;
        assert Row.serializer().deserialize(in) != null;
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
        RowMutation.serializer().serialize(emptyRm, out);
        RowMutation.serializer().serialize(standardRowRm, out);
        RowMutation.serializer().serialize(superRowRm, out);
        RowMutation.serializer().serialize(standardRm, out);
        RowMutation.serializer().serialize(superRm, out);
        RowMutation.serializer().serialize(mixedRm, out);
        Message.serializer().serialize(emptyRm.getMessage(MessagingService.version_), out);
        Message.serializer().serialize(standardRowRm.getMessage(MessagingService.version_), out);
        Message.serializer().serialize(superRowRm.getMessage(MessagingService.version_), out);
        Message.serializer().serialize(standardRm.getMessage(MessagingService.version_), out);
        Message.serializer().serialize(superRm.getMessage(MessagingService.version_), out);
        Message.serializer().serialize(mixedRm.getMessage(MessagingService.version_), out);
        out.close(); 
    }
    
    @Test
    public void testRowMutationRead() throws IOException
    {
        if (EXECUTE_WRITES)
            restRowMutationWrite();
        
        DataInputStream in = getInput("db.RowMutation.bin");
        assert RowMutation.serializer().deserialize(in) != null;
        assert RowMutation.serializer().deserialize(in) != null;
        assert RowMutation.serializer().deserialize(in) != null;
        assert RowMutation.serializer().deserialize(in) != null;
        assert RowMutation.serializer().deserialize(in) != null;
        assert RowMutation.serializer().deserialize(in) != null;
        assert Message.serializer().deserialize(in) != null;
        assert Message.serializer().deserialize(in) != null;
        assert Message.serializer().deserialize(in) != null;
        assert Message.serializer().deserialize(in) != null;
        assert Message.serializer().deserialize(in) != null;
        assert Message.serializer().deserialize(in) != null;
        in.close();
    }
    
    public void testTruncateWrite() throws IOException
    {
        Truncation tr = new Truncation(Statics.KS, "Doesn't Really Matter");
        TruncateResponse aff = new TruncateResponse(Statics.KS, "Doesn't Matter Either", true);
        TruncateResponse neg = new TruncateResponse(Statics.KS, "Still Doesn't Matter", false);
        DataOutputStream out = getOutput("db.Truncation.bin");
        Truncation.serializer().serialize(tr, out);
        TruncateResponse.serializer().serialize(aff, out);
        TruncateResponse.serializer().serialize(neg, out);
        Message.serializer().serialize(tr.getMessage(MessagingService.version_), out);
        Message.serializer().serialize(TruncateResponse.makeTruncateResponseMessage(tr.getMessage(MessagingService.version_), aff), out);
        Message.serializer().serialize(TruncateResponse.makeTruncateResponseMessage(tr.getMessage(MessagingService.version_), neg), out);
        // todo: notice how CF names weren't validated.
        out.close();
    }
    
    @Test
    public void testTruncateRead() throws IOException
    {
        if (EXECUTE_WRITES)
            testTruncateWrite();
        
        DataInputStream in = getInput("db.Truncation.bin");
        assert Truncation.serializer().deserialize(in) != null;
        assert TruncateResponse.serializer().deserialize(in) != null;
        assert TruncateResponse.serializer().deserialize(in) != null;
        assert Message.serializer().deserialize(in) != null;
        assert Message.serializer().deserialize(in) != null;
        assert Message.serializer().deserialize(in) != null;
        in.close();
    }
    
    private void testWriteResponseWrite() throws IOException
    {
        WriteResponse aff = new WriteResponse(Statics.KS, Statics.Key, true);
        WriteResponse neg = new WriteResponse(Statics.KS, Statics.Key, false);
        DataOutputStream out = getOutput("db.WriteResponse.bin");
        WriteResponse.serializer().serialize(aff, out);
        WriteResponse.serializer().serialize(neg, out);
        out.close();
    }
    
    @Test
    public void testWriteResponseRead() throws IOException
    {
        if (EXECUTE_WRITES)
            testWriteResponseWrite();
        
        DataInputStream in = getInput("db.WriteResponse.bin");
        assert WriteResponse.serializer().deserialize(in) != null;
        assert WriteResponse.serializer().deserialize(in) != null;
        in.close();
    }
    
    private static ByteBuffer bb(String s) {
        return ByteBuffer.wrap(s.getBytes());
    }
    
    private static class Statics 
    {
        private static final String KS = "Keyspace1";
        private static final ByteBuffer Key = ByteBuffer.wrap("Key01".getBytes());
        private static final List<ByteBuffer> NamedCols = new ArrayList<ByteBuffer>() 
        {{
            add(ByteBuffer.wrap("AAA".getBytes()));     
            add(ByteBuffer.wrap("BBB".getBytes()));     
            add(ByteBuffer.wrap("CCC".getBytes()));     
        }};
        private static final ByteBuffer SC = ByteBuffer.wrap("SCName".getBytes());
        private static final QueryPath StandardPath = new QueryPath("Standard1");
        private static final QueryPath SuperPath = new QueryPath("Super1", SC);
        private static final ByteBuffer Start = ByteBuffer.wrap("Start".getBytes());
        private static final ByteBuffer Stop = ByteBuffer.wrap("Stop".getBytes());
        
        private static final ColumnFamily StandardCf = ColumnFamily.create(Statics.KS, "Standard1");
        private static final ColumnFamily SuperCf = ColumnFamily.create(Statics.KS, "Super1");
        
        private static final SuperColumn SuperCol = new SuperColumn(Statics.SC, DatabaseDescriptor.getComparator(Statics.KS, "Super1"))
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
