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
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.db.filter.*;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
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
    @BeforeClass
    public static void loadSchema() throws IOException
    {
        loadSchema(true);
    }

    private void testRangeSliceCommandWrite() throws IOException
    {
        ByteBuffer startCol = ByteBufferUtil.bytes("Start");
        ByteBuffer stopCol = ByteBufferUtil.bytes("Stop");
        ByteBuffer emptyCol = ByteBufferUtil.bytes("");
        NamesQueryFilter namesPred = new NamesQueryFilter(Statics.NamedCols);
        SliceQueryFilter emptyRangePred = new SliceQueryFilter(emptyCol, emptyCol, false, 100);
        SliceQueryFilter nonEmptyRangePred = new SliceQueryFilter(startCol, stopCol, true, 100);
        IPartitioner part = StorageService.getPartitioner();
        AbstractBounds<RowPosition> bounds = new Range<Token>(part.getRandomToken(), part.getRandomToken()).toRowBounds();

        RangeSliceCommand namesCmd = new RangeSliceCommand(Statics.KS, "Standard1", null, namesPred, bounds, 100);
        MessageOut<RangeSliceCommand> namesCmdMsg = namesCmd.createMessage();
        RangeSliceCommand emptyRangeCmd = new RangeSliceCommand(Statics.KS, "Standard1", null, emptyRangePred, bounds, 100);
        MessageOut<RangeSliceCommand> emptyRangeCmdMsg = emptyRangeCmd.createMessage();
        RangeSliceCommand regRangeCmd = new RangeSliceCommand(Statics.KS, "Standard1", null,  nonEmptyRangePred, bounds, 100);
        MessageOut<RangeSliceCommand> regRangeCmdMsg = regRangeCmd.createMessage();
        RangeSliceCommand namesCmdSup = new RangeSliceCommand(Statics.KS, "Super1", Statics.SC, namesPred, bounds, 100);
        MessageOut<RangeSliceCommand> namesCmdSupMsg = namesCmdSup.createMessage();
        RangeSliceCommand emptyRangeCmdSup = new RangeSliceCommand(Statics.KS, "Super1", Statics.SC, emptyRangePred, bounds, 100);
        MessageOut<RangeSliceCommand> emptyRangeCmdSupMsg = emptyRangeCmdSup.createMessage();
        RangeSliceCommand regRangeCmdSup = new RangeSliceCommand(Statics.KS, "Super1", Statics.SC,  nonEmptyRangePred, bounds, 100);
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
            MessageIn.read(in, getVersion(), "id");
        in.close();
    }

    private void testSliceByNamesReadCommandWrite() throws IOException
    {
        SliceByNamesReadCommand standardCmd = new SliceByNamesReadCommand(Statics.KS, Statics.Key, Statics.StandardPath, Statics.NamedCols);
        SliceByNamesReadCommand superCmd = new SliceByNamesReadCommand(Statics.KS, Statics.Key, Statics.SuperPath, Statics.NamedCols);

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
        assert MessageIn.read(in, getVersion(), "id") != null;
        assert MessageIn.read(in, getVersion(), "id") != null;
        in.close();
    }

    private void testSliceFromReadCommandWrite() throws IOException
    {
        SliceFromReadCommand standardCmd = new SliceFromReadCommand(Statics.KS, Statics.Key, Statics.StandardPath, Statics.Start, Statics.Stop, true, 100);
        SliceFromReadCommand superCmd = new SliceFromReadCommand(Statics.KS, Statics.Key, Statics.SuperPath, Statics.Start, Statics.Stop, true, 100);
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
        assert MessageIn.read(in, getVersion(), "id") != null;
        assert MessageIn.read(in, getVersion(), "id") != null;
        in.close();
    }

    private void testRowWrite() throws IOException
    {
        DataOutputStream out = getOutput("db.Row.bin");
        Row.serializer.serialize(Statics.StandardRow, out, getVersion());
        Row.serializer.serialize(Statics.SuperRow, out, getVersion());
        Row.serializer.serialize(Statics.NullRow, out, getVersion());
        out.close();

        // test serializedSize
        testSerializedSize(Statics.StandardRow, Row.serializer);
        testSerializedSize(Statics.SuperRow, Row.serializer);
        testSerializedSize(Statics.NullRow, Row.serializer);
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
        RowMutation emptyRm = new RowMutation(Statics.KS, Statics.Key);
        RowMutation standardRowRm = new RowMutation(Statics.KS, Statics.StandardRow);
        RowMutation superRowRm = new RowMutation(Statics.KS, Statics.SuperRow);
        RowMutation standardRm = new RowMutation(Statics.KS, Statics.Key);
        standardRm.add(Statics.StandardCf);
        RowMutation superRm = new RowMutation(Statics.KS, Statics.Key);
        superRm.add(Statics.SuperCf);
        Map<UUID, ColumnFamily> mods = new HashMap<UUID, ColumnFamily>();
        mods.put(Statics.StandardCf.metadata().cfId, Statics.StandardCf);
        mods.put(Statics.SuperCf.metadata().cfId, Statics.SuperCf);
        RowMutation mixedRm = new RowMutation(Statics.KS, Statics.Key, mods);

        DataOutputStream out = getOutput("db.RowMutation.bin");
        RowMutation.serializer.serialize(emptyRm, out, getVersion());
        RowMutation.serializer.serialize(standardRowRm, out, getVersion());
        RowMutation.serializer.serialize(superRowRm, out, getVersion());
        RowMutation.serializer.serialize(standardRm, out, getVersion());
        RowMutation.serializer.serialize(superRm, out, getVersion());
        RowMutation.serializer.serialize(mixedRm, out, getVersion());

        emptyRm.createMessage().serialize(out, getVersion());
        standardRowRm.createMessage().serialize(out, getVersion());
        superRowRm.createMessage().serialize(out, getVersion());
        standardRm.createMessage().serialize(out, getVersion());
        superRm.createMessage().serialize(out, getVersion());
        mixedRm.createMessage().serialize(out, getVersion());

        out.close();

        // test serializedSize
        testSerializedSize(emptyRm, RowMutation.serializer);
        testSerializedSize(standardRowRm, RowMutation.serializer);
        testSerializedSize(superRowRm, RowMutation.serializer);
        testSerializedSize(standardRm, RowMutation.serializer);
        testSerializedSize(superRm, RowMutation.serializer);
        testSerializedSize(mixedRm, RowMutation.serializer);
    }

    @Test
    public void testRowMutationRead() throws IOException
    {
        if (EXECUTE_WRITES)
            testRowMutationWrite();

        DataInputStream in = getInput("db.RowMutation.bin");
        assert RowMutation.serializer.deserialize(in, getVersion()) != null;
        assert RowMutation.serializer.deserialize(in, getVersion()) != null;
        assert RowMutation.serializer.deserialize(in, getVersion()) != null;
        assert RowMutation.serializer.deserialize(in, getVersion()) != null;
        assert RowMutation.serializer.deserialize(in, getVersion()) != null;
        assert RowMutation.serializer.deserialize(in, getVersion()) != null;
        assert MessageIn.read(in, getVersion(), "id") != null;
        assert MessageIn.read(in, getVersion(), "id") != null;
        assert MessageIn.read(in, getVersion(), "id") != null;
        assert MessageIn.read(in, getVersion(), "id") != null;
        assert MessageIn.read(in, getVersion(), "id") != null;
        assert MessageIn.read(in, getVersion(), "id") != null;
        in.close();
    }

    private void testTruncateWrite() throws IOException
    {
        Truncation tr = new Truncation(Statics.KS, "Doesn't Really Matter");
        TruncateResponse aff = new TruncateResponse(Statics.KS, "Doesn't Matter Either", true);
        TruncateResponse neg = new TruncateResponse(Statics.KS, "Still Doesn't Matter", false);
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
        assert MessageIn.read(in, getVersion(), "id") != null;

        // set up some fake callbacks so deserialization knows that what it's deserializing is a TruncateResponse
        MessagingService.instance().setCallbackForTests("tr1", new CallbackInfo(null, null, TruncateResponse.serializer));
        MessagingService.instance().setCallbackForTests("tr2", new CallbackInfo(null, null, TruncateResponse.serializer));

        assert MessageIn.read(in, getVersion(), "tr1") != null;
        assert MessageIn.read(in, getVersion(), "tr2") != null;
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
        private static final String KS = "Keyspace1";
        private static final ByteBuffer Key = ByteBufferUtil.bytes("Key01");
        private static final SortedSet<ByteBuffer> NamedCols = new TreeSet<ByteBuffer>(BytesType.instance)
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
