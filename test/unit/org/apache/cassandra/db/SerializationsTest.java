/*
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
 */
package org.apache.cassandra.db;

import org.apache.cassandra.AbstractSerializationsTester;
import org.apache.cassandra.Util;
import org.apache.cassandra.db.filter.*;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.db.marshal.CompositeType;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.net.CallbackInfo;
import org.apache.cassandra.net.MessageIn;
import org.apache.cassandra.net.MessageOut;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.ByteBufferUtil;

import org.junit.BeforeClass;
import org.junit.Test;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;

public class SerializationsTest extends AbstractSerializationsTester
{
    Statics statics = new Statics();

    @BeforeClass
    public static void loadSchema() throws IOException, ConfigurationException
    {
        loadSchema(true);
    }

    private ByteBuffer startCol = ByteBufferUtil.bytes("Start");
    private ByteBuffer stopCol = ByteBufferUtil.bytes("Stop");
    private ByteBuffer emptyCol = ByteBufferUtil.bytes("");
    public NamesQueryFilter namesPred = new NamesQueryFilter(statics.NamedCols);
    public NamesQueryFilter namesSCPred = new NamesQueryFilter(statics.NamedSCCols);
    public SliceQueryFilter emptyRangePred = new SliceQueryFilter(emptyCol, emptyCol, false, 100);
    public SliceQueryFilter nonEmptyRangePred = new SliceQueryFilter(startCol, stopCol, true, 100);
    public SliceQueryFilter nonEmptyRangeSCPred = new SliceQueryFilter(CompositeType.build(statics.SC, startCol), CompositeType.build(statics.SC, stopCol), true, 100);

    private void testRangeSliceCommandWrite() throws IOException
    {
        IPartitioner part = StorageService.getPartitioner();
        AbstractBounds<RowPosition> bounds = new Range<Token>(part.getRandomToken(), part.getRandomToken()).toRowBounds();

        RangeSliceCommand namesCmd = new RangeSliceCommand(statics.KS, "Standard1", statics.readTs, namesPred, bounds, 100);
        MessageOut<RangeSliceCommand> namesCmdMsg = namesCmd.createMessage();
        RangeSliceCommand emptyRangeCmd = new RangeSliceCommand(statics.KS, "Standard1", statics.readTs, emptyRangePred, bounds, 100);
        MessageOut<RangeSliceCommand> emptyRangeCmdMsg = emptyRangeCmd.createMessage();
        RangeSliceCommand regRangeCmd = new RangeSliceCommand(statics.KS, "Standard1", statics.readTs, nonEmptyRangePred, bounds, 100);
        MessageOut<RangeSliceCommand> regRangeCmdMsg = regRangeCmd.createMessage();
        RangeSliceCommand namesCmdSup = new RangeSliceCommand(statics.KS, "Super1", statics.readTs, namesSCPred, bounds, 100);
        MessageOut<RangeSliceCommand> namesCmdSupMsg = namesCmdSup.createMessage();
        RangeSliceCommand emptyRangeCmdSup = new RangeSliceCommand(statics.KS, "Super1", statics.readTs, emptyRangePred, bounds, 100);
        MessageOut<RangeSliceCommand> emptyRangeCmdSupMsg = emptyRangeCmdSup.createMessage();
        RangeSliceCommand regRangeCmdSup = new RangeSliceCommand(statics.KS, "Super1", statics.readTs, nonEmptyRangeSCPred, bounds, 100);
        MessageOut<RangeSliceCommand> regRangeCmdSupMsg = regRangeCmdSup.createMessage();

        DataOutputStream out = getOutput("db.RangeSliceCommand.bin");
        namesCmdMsg.serialize(out, getVersion());
        emptyRangeCmdMsg.serialize(out, getVersion());
        regRangeCmdMsg.serialize(out, getVersion());
        namesCmdSupMsg.serialize(out, getVersion());
        emptyRangeCmdSupMsg.serialize(out, getVersion());
        regRangeCmdSupMsg.serialize(out, getVersion());
        out.close();

        // test serializedSize
        testSerializedSize(namesCmd, RangeSliceCommand.serializer);
        testSerializedSize(emptyRangeCmd, RangeSliceCommand.serializer);
        testSerializedSize(regRangeCmd, RangeSliceCommand.serializer);
        testSerializedSize(namesCmdSup, RangeSliceCommand.serializer);
        testSerializedSize(emptyRangeCmdSup, RangeSliceCommand.serializer);
        testSerializedSize(regRangeCmdSup, RangeSliceCommand.serializer);
    }

    @Test
    public void testRangeSliceCommandRead() throws IOException
    {
        if (EXECUTE_WRITES)
            testRangeSliceCommandWrite();

        DataInputStream in = getInput("db.RangeSliceCommand.bin");
        for (int i = 0; i < 6; i++)
            MessageIn.read(in, getVersion(), -1);
        in.close();
    }

    private void testSliceByNamesReadCommandWrite() throws IOException
    {
        SliceByNamesReadCommand standardCmd = new SliceByNamesReadCommand(statics.KS, statics.Key, statics.StandardCF, statics.readTs, namesPred);
        SliceByNamesReadCommand superCmd = new SliceByNamesReadCommand(statics.KS, statics.Key, statics.SuperCF, statics.readTs, namesSCPred);

        DataOutputStream out = getOutput("db.SliceByNamesReadCommand.bin");
        SliceByNamesReadCommand.serializer.serialize(standardCmd, out, getVersion());
        SliceByNamesReadCommand.serializer.serialize(superCmd, out, getVersion());
        ReadCommand.serializer.serialize(standardCmd, out, getVersion());
        ReadCommand.serializer.serialize(superCmd, out, getVersion());
        standardCmd.createMessage().serialize(out, getVersion());
        superCmd.createMessage().serialize(out, getVersion());
        out.close();

        // test serializedSize
        testSerializedSize(standardCmd, SliceByNamesReadCommand.serializer);
        testSerializedSize(superCmd, SliceByNamesReadCommand.serializer);
    }

    @Test
    public void testSliceByNamesReadCommandRead() throws IOException
    {
        if (EXECUTE_WRITES)
            testSliceByNamesReadCommandWrite();

        DataInputStream in = getInput("db.SliceByNamesReadCommand.bin");
        assert SliceByNamesReadCommand.serializer.deserialize(in, getVersion()) != null;
        assert SliceByNamesReadCommand.serializer.deserialize(in, getVersion()) != null;
        assert ReadCommand.serializer.deserialize(in, getVersion()) != null;
        assert ReadCommand.serializer.deserialize(in, getVersion()) != null;
        assert MessageIn.read(in, getVersion(), -1) != null;
        assert MessageIn.read(in, getVersion(), -1) != null;
        in.close();
    }

    private void testSliceFromReadCommandWrite() throws IOException
    {
        SliceFromReadCommand standardCmd = new SliceFromReadCommand(statics.KS, statics.Key, statics.StandardCF, statics.readTs, nonEmptyRangePred);
        SliceFromReadCommand superCmd = new SliceFromReadCommand(statics.KS, statics.Key, statics.SuperCF, statics.readTs, nonEmptyRangeSCPred);
        
        DataOutputStream out = getOutput("db.SliceFromReadCommand.bin");
        SliceFromReadCommand.serializer.serialize(standardCmd, out, getVersion());
        SliceFromReadCommand.serializer.serialize(superCmd, out, getVersion());
        ReadCommand.serializer.serialize(standardCmd, out, getVersion());
        ReadCommand.serializer.serialize(superCmd, out, getVersion());
        standardCmd.createMessage().serialize(out, getVersion());
        superCmd.createMessage().serialize(out, getVersion());

        out.close();

        // test serializedSize
        testSerializedSize(standardCmd, SliceFromReadCommand.serializer);
        testSerializedSize(superCmd, SliceFromReadCommand.serializer);
    }

    @Test
    public void testSliceFromReadCommandRead() throws IOException
    {
        if (EXECUTE_WRITES)
            testSliceFromReadCommandWrite();

        DataInputStream in = getInput("db.SliceFromReadCommand.bin");
        assert SliceFromReadCommand.serializer.deserialize(in, getVersion()) != null;
        assert SliceFromReadCommand.serializer.deserialize(in, getVersion()) != null;
        assert ReadCommand.serializer.deserialize(in, getVersion()) != null;
        assert ReadCommand.serializer.deserialize(in, getVersion()) != null;
        assert MessageIn.read(in, getVersion(), -1) != null;
        assert MessageIn.read(in, getVersion(), -1) != null;
        in.close();
    }

    private void testRowWrite() throws IOException
    {
        DataOutputStream out = getOutput("db.Row.bin");
        Row.serializer.serialize(statics.StandardRow, out, getVersion());
        Row.serializer.serialize(statics.SuperRow, out, getVersion());
        Row.serializer.serialize(statics.NullRow, out, getVersion());
        out.close();

        // test serializedSize
        testSerializedSize(statics.StandardRow, Row.serializer);
        testSerializedSize(statics.SuperRow, Row.serializer);
        testSerializedSize(statics.NullRow, Row.serializer);
    }

    @Test
    public void testRowRead() throws IOException
    {
        if (EXECUTE_WRITES)
            testRowWrite();

        DataInputStream in = getInput("db.Row.bin");
        assert Row.serializer.deserialize(in, getVersion()) != null;
        assert Row.serializer.deserialize(in, getVersion()) != null;
        assert Row.serializer.deserialize(in, getVersion()) != null;
        in.close();
    }

    private void testRowMutationWrite() throws IOException
    {
        RowMutation standardRowRm = new RowMutation(statics.KS, statics.StandardRow);
        RowMutation superRowRm = new RowMutation(statics.KS, statics.SuperRow);
        RowMutation standardRm = new RowMutation(statics.KS, statics.Key, statics.StandardCf);
        RowMutation superRm = new RowMutation(statics.KS, statics.Key, statics.SuperCf);
        Map<UUID, ColumnFamily> mods = new HashMap<UUID, ColumnFamily>();
        mods.put(statics.StandardCf.metadata().cfId, statics.StandardCf);
        mods.put(statics.SuperCf.metadata().cfId, statics.SuperCf);
        RowMutation mixedRm = new RowMutation(statics.KS, statics.Key, mods);

        DataOutputStream out = getOutput("db.RowMutation.bin");
        RowMutation.serializer.serialize(standardRowRm, out, getVersion());
        RowMutation.serializer.serialize(superRowRm, out, getVersion());
        RowMutation.serializer.serialize(standardRm, out, getVersion());
        RowMutation.serializer.serialize(superRm, out, getVersion());
        RowMutation.serializer.serialize(mixedRm, out, getVersion());

        standardRowRm.createMessage().serialize(out, getVersion());
        superRowRm.createMessage().serialize(out, getVersion());
        standardRm.createMessage().serialize(out, getVersion());
        superRm.createMessage().serialize(out, getVersion());
        mixedRm.createMessage().serialize(out, getVersion());

        out.close();

        // test serializedSize
        testSerializedSize(standardRowRm, RowMutation.serializer);
        testSerializedSize(superRowRm, RowMutation.serializer);
        testSerializedSize(standardRm, RowMutation.serializer);
        testSerializedSize(superRm, RowMutation.serializer);
        testSerializedSize(mixedRm, RowMutation.serializer);
    }

    @Test
    public void testRowMutationRead() throws IOException
    {
        // row mutation deserialization requires being able to look up the keyspace in the schema,
        // so we need to rewrite this each time.  We can go back to testing on-disk data
        // once we pull RM.keyspace field out.
        testRowMutationWrite();

        DataInputStream in = getInput("db.RowMutation.bin");
        assert RowMutation.serializer.deserialize(in, getVersion()) != null;
        assert RowMutation.serializer.deserialize(in, getVersion()) != null;
        assert RowMutation.serializer.deserialize(in, getVersion()) != null;
        assert RowMutation.serializer.deserialize(in, getVersion()) != null;
        assert RowMutation.serializer.deserialize(in, getVersion()) != null;
        assert MessageIn.read(in, getVersion(), -1) != null;
        assert MessageIn.read(in, getVersion(), -1) != null;
        assert MessageIn.read(in, getVersion(), -1) != null;
        assert MessageIn.read(in, getVersion(), -1) != null;
        assert MessageIn.read(in, getVersion(), -1) != null;
        in.close();
    }

    private void testTruncateWrite() throws IOException
    {
        Truncation tr = new Truncation(statics.KS, "Doesn't Really Matter");
        TruncateResponse aff = new TruncateResponse(statics.KS, "Doesn't Matter Either", true);
        TruncateResponse neg = new TruncateResponse(statics.KS, "Still Doesn't Matter", false);
        DataOutputStream out = getOutput("db.Truncation.bin");
        Truncation.serializer.serialize(tr, out, getVersion());
        TruncateResponse.serializer.serialize(aff, out, getVersion());
        TruncateResponse.serializer.serialize(neg, out, getVersion());

        tr.createMessage().serialize(out, getVersion());
        aff.createMessage().serialize(out, getVersion());
        neg.createMessage().serialize(out, getVersion());
        // todo: notice how CF names weren't validated.
        out.close();

        // test serializedSize
        testSerializedSize(tr, Truncation.serializer);
        testSerializedSize(aff, TruncateResponse.serializer);
        testSerializedSize(neg, TruncateResponse.serializer);
    }

    @Test
    public void testTruncateRead() throws IOException
    {
        if (EXECUTE_WRITES)
            testTruncateWrite();

        DataInputStream in = getInput("db.Truncation.bin");
        assert Truncation.serializer.deserialize(in, getVersion()) != null;
        assert TruncateResponse.serializer.deserialize(in, getVersion()) != null;
        assert TruncateResponse.serializer.deserialize(in, getVersion()) != null;
        assert MessageIn.read(in, getVersion(), -1) != null;

        // set up some fake callbacks so deserialization knows that what it's deserializing is a TruncateResponse
        MessagingService.instance().setCallbackForTests(1, new CallbackInfo(null, null, TruncateResponse.serializer));
        MessagingService.instance().setCallbackForTests(2, new CallbackInfo(null, null, TruncateResponse.serializer));

        assert MessageIn.read(in, getVersion(), 1) != null;
        assert MessageIn.read(in, getVersion(), 2) != null;
        in.close();
    }

    private void testWriteResponseWrite() throws IOException
    {
        WriteResponse aff = new WriteResponse();
        WriteResponse neg = new WriteResponse();
        DataOutputStream out = getOutput("db.WriteResponse.bin");
        WriteResponse.serializer.serialize(aff, out, getVersion());
        WriteResponse.serializer.serialize(neg, out, getVersion());
        out.close();

        // test serializedSize
        testSerializedSize(aff, WriteResponse.serializer);
        testSerializedSize(neg, WriteResponse.serializer);
    }

    @Test
    public void testWriteResponseRead() throws IOException
    {
        if (EXECUTE_WRITES)
            testWriteResponseWrite();

        DataInputStream in = getInput("db.WriteResponse.bin");
        assert WriteResponse.serializer.deserialize(in, getVersion()) != null;
        assert WriteResponse.serializer.deserialize(in, getVersion()) != null;
        in.close();
    }

    private static ByteBuffer bb(String s) {
        return ByteBufferUtil.bytes(s);
    }

    private static class Statics
    {
        private final String KS = "Keyspace1";
        private final ByteBuffer Key = ByteBufferUtil.bytes("Key01");
        private final SortedSet<ByteBuffer> NamedCols = new TreeSet<ByteBuffer>(BytesType.instance)
        {{
            add(ByteBufferUtil.bytes("AAA"));
            add(ByteBufferUtil.bytes("BBB"));
            add(ByteBufferUtil.bytes("CCC"));
        }};
        private final ByteBuffer SC = ByteBufferUtil.bytes("SCName");
        private final SortedSet<ByteBuffer> NamedSCCols = new TreeSet<ByteBuffer>(BytesType.instance)
        {{
            add(CompositeType.build(SC, ByteBufferUtil.bytes("AAA")));
            add(CompositeType.build(SC, ByteBufferUtil.bytes("BBB")));
            add(CompositeType.build(SC, ByteBufferUtil.bytes("CCC")));
        }};
        private final String StandardCF = "Standard1";
        private final String SuperCF = "Super1";

        private final long readTs = 1369935512292L;

        private final ColumnFamily StandardCf = TreeMapBackedSortedColumns.factory.create(KS, StandardCF);
        private final ColumnFamily SuperCf = TreeMapBackedSortedColumns.factory.create(KS, SuperCF);

        private final Row StandardRow = new Row(Util.dk("key0"), StandardCf);
        private final Row SuperRow = new Row(Util.dk("key1"), SuperCf);
        private final Row NullRow = new Row(Util.dk("key2"), null);

        private Statics()
        {
            StandardCf.addColumn(new Column(bb("aaaa")));
            StandardCf.addColumn(new Column(bb("bbbb"), bb("bbbbb-value")));
            StandardCf.addColumn(new Column(bb("cccc"), bb("ccccc-value"), 1000L));
            StandardCf.addColumn(new DeletedColumn(bb("dddd"), 500, 1000));
            StandardCf.addColumn(new DeletedColumn(bb("eeee"), bb("eeee-value"), 1001));
            StandardCf.addColumn(new ExpiringColumn(bb("ffff"), bb("ffff-value"), 2000, 1000));
            StandardCf.addColumn(new ExpiringColumn(bb("gggg"), bb("gggg-value"), 2001, 1000, 2002));

            SuperCf.addColumn(new Column(CompositeType.build(SC, bb("aaaa"))));
            SuperCf.addColumn(new Column(CompositeType.build(SC, bb("bbbb")), bb("bbbbb-value")));
            SuperCf.addColumn(new Column(CompositeType.build(SC, bb("cccc")), bb("ccccc-value"), 1000L));
            SuperCf.addColumn(new DeletedColumn(CompositeType.build(SC, bb("dddd")), 500, 1000));
            SuperCf.addColumn(new DeletedColumn(CompositeType.build(SC, bb("eeee")), bb("eeee-value"), 1001));
            SuperCf.addColumn(new ExpiringColumn(CompositeType.build(SC, bb("ffff")), bb("ffff-value"), 2000, 1000));
            SuperCf.addColumn(new ExpiringColumn(CompositeType.build(SC, bb("gggg")), bb("gggg-value"), 2001, 1000, 2002));
        }
    }
}
