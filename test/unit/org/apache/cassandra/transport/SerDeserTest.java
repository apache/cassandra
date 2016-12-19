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
package org.apache.cassandra.transport;

import java.nio.ByteBuffer;
import java.util.*;

import io.netty.buffer.Unpooled;
import io.netty.buffer.ByteBuf;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.Util;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.*;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.marshal.*;
import org.apache.cassandra.serializers.CollectionSerializer;
import org.apache.cassandra.service.pager.PagingState;
import org.apache.cassandra.transport.Event.TopologyChange;
import org.apache.cassandra.transport.Event.SchemaChange;
import org.apache.cassandra.transport.Event.StatusChange;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Pair;

import static org.junit.Assert.assertEquals;
import static org.apache.cassandra.utils.ByteBufferUtil.bytes;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;

/**
 * Serialization/deserialization tests for protocol objects and messages.
 */
public class SerDeserTest
{
    @BeforeClass
    public static void setupDD()
    {
        // required for making the paging state
        DatabaseDescriptor.daemonInitialization();
    }

    @Test
    public void collectionSerDeserTest() throws Exception
    {
        for (ProtocolVersion version : ProtocolVersion.SUPPORTED)
            collectionSerDeserTest(version);
    }

    public void collectionSerDeserTest(ProtocolVersion version) throws Exception
    {
        // Lists
        ListType<?> lt = ListType.getInstance(Int32Type.instance, true);
        List<Integer> l = Arrays.asList(2, 6, 1, 9);

        List<ByteBuffer> lb = new ArrayList<>(l.size());
        for (Integer i : l)
            lb.add(Int32Type.instance.decompose(i));

        assertEquals(l, lt.getSerializer().deserializeForNativeProtocol(CollectionSerializer.pack(lb, lb.size(), version), version));

        // Sets
        SetType<?> st = SetType.getInstance(UTF8Type.instance, true);
        Set<String> s = new LinkedHashSet<>();
        s.addAll(Arrays.asList("bar", "foo", "zee"));

        List<ByteBuffer> sb = new ArrayList<>(s.size());
        for (String t : s)
            sb.add(UTF8Type.instance.decompose(t));

        assertEquals(s, st.getSerializer().deserializeForNativeProtocol(CollectionSerializer.pack(sb, sb.size(), version), version));

        // Maps
        MapType<?, ?> mt = MapType.getInstance(UTF8Type.instance, LongType.instance, true);
        Map<String, Long> m = new LinkedHashMap<>();
        m.put("bar", 12L);
        m.put("foo", 42L);
        m.put("zee", 14L);

        List<ByteBuffer> mb = new ArrayList<>(m.size() * 2);
        for (Map.Entry<String, Long> entry : m.entrySet())
        {
            mb.add(UTF8Type.instance.decompose(entry.getKey()));
            mb.add(LongType.instance.decompose(entry.getValue()));
        }

        assertEquals(m, mt.getSerializer().deserializeForNativeProtocol(CollectionSerializer.pack(mb, m.size(), version), version));
    }

    @Test
    public void eventSerDeserTest() throws Exception
    {
        for (ProtocolVersion version : ProtocolVersion.SUPPORTED)
            eventSerDeserTest(version);
    }

    public void eventSerDeserTest(ProtocolVersion version) throws Exception
    {
        List<Event> events = new ArrayList<>();

        events.add(TopologyChange.newNode(FBUtilities.getBroadcastAddress(), 42));
        events.add(TopologyChange.removedNode(FBUtilities.getBroadcastAddress(), 42));
        events.add(TopologyChange.movedNode(FBUtilities.getBroadcastAddress(), 42));

        events.add(StatusChange.nodeUp(FBUtilities.getBroadcastAddress(), 42));
        events.add(StatusChange.nodeDown(FBUtilities.getBroadcastAddress(), 42));

        events.add(new SchemaChange(SchemaChange.Change.CREATED, "ks"));
        events.add(new SchemaChange(SchemaChange.Change.UPDATED, "ks"));
        events.add(new SchemaChange(SchemaChange.Change.DROPPED, "ks"));

        events.add(new SchemaChange(SchemaChange.Change.CREATED, SchemaChange.Target.TABLE, "ks", "table"));
        events.add(new SchemaChange(SchemaChange.Change.UPDATED, SchemaChange.Target.TABLE, "ks", "table"));
        events.add(new SchemaChange(SchemaChange.Change.DROPPED, SchemaChange.Target.TABLE, "ks", "table"));

        if (version.isGreaterOrEqualTo(ProtocolVersion.V3))
        {
            events.add(new SchemaChange(SchemaChange.Change.CREATED, SchemaChange.Target.TYPE, "ks", "type"));
            events.add(new SchemaChange(SchemaChange.Change.UPDATED, SchemaChange.Target.TYPE, "ks", "type"));
            events.add(new SchemaChange(SchemaChange.Change.DROPPED, SchemaChange.Target.TYPE, "ks", "type"));
        }

        if (version.isGreaterOrEqualTo(ProtocolVersion.V4))
        {
            List<String> moreTypes = Arrays.asList("text", "bigint");

            events.add(new SchemaChange(SchemaChange.Change.CREATED, SchemaChange.Target.FUNCTION, "ks", "func", Collections.<String>emptyList()));
            events.add(new SchemaChange(SchemaChange.Change.UPDATED, SchemaChange.Target.FUNCTION, "ks", "func", moreTypes));
            events.add(new SchemaChange(SchemaChange.Change.DROPPED, SchemaChange.Target.FUNCTION, "ks", "func", moreTypes));

            events.add(new SchemaChange(SchemaChange.Change.CREATED, SchemaChange.Target.AGGREGATE, "ks", "aggr", Collections.<String>emptyList()));
            events.add(new SchemaChange(SchemaChange.Change.UPDATED, SchemaChange.Target.AGGREGATE, "ks", "aggr", moreTypes));
            events.add(new SchemaChange(SchemaChange.Change.DROPPED, SchemaChange.Target.AGGREGATE, "ks", "aggr", moreTypes));
        }

        for (Event ev : events)
        {
            ByteBuf buf = Unpooled.buffer(ev.serializedSize(version));
            ev.serialize(buf, version);
            assertEquals(ev, Event.deserialize(buf, version));
        }
    }

    private static ByteBuffer bb(String str)
    {
        return UTF8Type.instance.decompose(str);
    }

    private static FieldIdentifier field(String field)
    {
        return FieldIdentifier.forQuoted(field);
    }

    private static ColumnIdentifier ci(String name)
    {
        return new ColumnIdentifier(name, false);
    }

    private static Constants.Literal lit(long v)
    {
        return Constants.Literal.integer(String.valueOf(v));
    }

    private static Constants.Literal lit(String v)
    {
        return Constants.Literal.string(v);
    }

    private static ColumnSpecification columnSpec(String name, AbstractType<?> type)
    {
        return new ColumnSpecification("ks", "cf", ci(name), type);
    }

    @Test
    public void udtSerDeserTest() throws Exception
    {
        for (ProtocolVersion version : ProtocolVersion.SUPPORTED)
            udtSerDeserTest(version);
    }


    public void udtSerDeserTest(ProtocolVersion version) throws Exception
    {
        ListType<?> lt = ListType.getInstance(Int32Type.instance, true);
        SetType<?> st = SetType.getInstance(UTF8Type.instance, true);
        MapType<?, ?> mt = MapType.getInstance(UTF8Type.instance, LongType.instance, true);

        UserType udt = new UserType("ks",
                                    bb("myType"),
                                    Arrays.asList(field("f1"), field("f2"), field("f3"), field("f4")),
                                    Arrays.asList(LongType.instance, lt, st, mt),
                                    true);

        Map<FieldIdentifier, Term.Raw> value = new HashMap<>();
        value.put(field("f1"), lit(42));
        value.put(field("f2"), new Lists.Literal(Arrays.<Term.Raw>asList(lit(3), lit(1))));
        value.put(field("f3"), new Sets.Literal(Arrays.<Term.Raw>asList(lit("foo"), lit("bar"))));
        value.put(field("f4"), new Maps.Literal(Arrays.<Pair<Term.Raw, Term.Raw>>asList(
                                   Pair.<Term.Raw, Term.Raw>create(lit("foo"), lit(24)),
                                   Pair.<Term.Raw, Term.Raw>create(lit("bar"), lit(12)))));

        UserTypes.Literal u = new UserTypes.Literal(value);
        Term t = u.prepare("ks", columnSpec("myValue", udt));

        QueryOptions options = QueryOptions.DEFAULT;

        ByteBuffer serialized = t.bindAndGet(options);

        ByteBuffer[] fields = udt.split(serialized);

        assertEquals(4, fields.length);

        assertEquals(bytes(42L), fields[0]);

        // Note that no matter what the protocol version has been used in bindAndGet above, the collections inside
        // a UDT should alway be serialized with version 3 of the protocol. Which is why we don't use 'version'
        // on purpose below.

        assertEquals(Arrays.asList(3, 1), lt.getSerializer().deserializeForNativeProtocol(fields[1], ProtocolVersion.V3));

        LinkedHashSet<String> s = new LinkedHashSet<>();
        s.addAll(Arrays.asList("bar", "foo"));
        assertEquals(s, st.getSerializer().deserializeForNativeProtocol(fields[2], ProtocolVersion.V3));

        LinkedHashMap<String, Long> m = new LinkedHashMap<>();
        m.put("bar", 12L);
        m.put("foo", 24L);
        assertEquals(m, mt.getSerializer().deserializeForNativeProtocol(fields[3], ProtocolVersion.V3));
    }

    @Test
    public void preparedMetadataSerializationTest()
    {
        for (ProtocolVersion version : ProtocolVersion.SUPPORTED)
            preparedMetadataSerializationTest(version);
    }

    private void preparedMetadataSerializationTest(ProtocolVersion version)
    {
        List<ColumnSpecification> columnNames = new ArrayList<>();
        for (int i = 0; i < 3; i++)
            columnNames.add(new ColumnSpecification("ks", "cf", new ColumnIdentifier("col" + i, false), Int32Type.instance));

        if (version == ProtocolVersion.V3)
        {
            // v3 encoding doesn't include partition key bind indexes
            ResultSet.PreparedMetadata meta = new ResultSet.PreparedMetadata(columnNames, new short[]{ 2, 1 });
            ByteBuf buf = Unpooled.buffer(ResultSet.PreparedMetadata.codec.encodedSize(meta, version));
            ResultSet.PreparedMetadata.codec.encode(meta, buf, version);
            ResultSet.PreparedMetadata decodedMeta = ResultSet.PreparedMetadata.codec.decode(buf, version);

            assertNotSame(meta, decodedMeta);

            // however, if there are no partition key indexes, they should be the same
            ResultSet.PreparedMetadata metaWithoutIndexes = new ResultSet.PreparedMetadata(columnNames, null);
            buf = Unpooled.buffer(metaWithoutIndexes.codec.encodedSize(metaWithoutIndexes, version));
            metaWithoutIndexes.codec.encode(metaWithoutIndexes, buf, version);
            ResultSet.PreparedMetadata decodedMetaWithoutIndexes = metaWithoutIndexes.codec.decode(buf, version);

            assertEquals(decodedMeta, decodedMetaWithoutIndexes);
        }
        else
        {
            ResultSet.PreparedMetadata meta = new ResultSet.PreparedMetadata(columnNames, new short[]{ 2, 1 });
            ByteBuf buf = Unpooled.buffer(ResultSet.PreparedMetadata.codec.encodedSize(meta, version));
            ResultSet.PreparedMetadata.codec.encode(meta, buf, version);
            ResultSet.PreparedMetadata decodedMeta = ResultSet.PreparedMetadata.codec.decode(buf, version);
            assertEquals(meta, decodedMeta);
        }
    }

    @Test
    public void metadataSerializationTest()
    {
        for (ProtocolVersion version : ProtocolVersion.SUPPORTED)
            metadataSerializationTest(version);
    }

    private void metadataSerializationTest(ProtocolVersion version)
    {
        List<ColumnSpecification> columnNames = new ArrayList<>();
        for (int i = 0; i < 3; i++)
            columnNames.add(new ColumnSpecification("ks", "cf", new ColumnIdentifier("col" + i, false), Int32Type.instance));

        ResultSet.ResultMetadata meta = new ResultSet.ResultMetadata(columnNames);
        ByteBuf buf = Unpooled.buffer(meta.codec.encodedSize(meta, version));
        meta.codec.encode(meta, buf, version);
        ResultSet.ResultMetadata decodedMeta = meta.codec.decode(buf, version);

        assertEquals(meta, decodedMeta);
    }

    @Test
    public void queryOptionsSerDeserTest() throws Exception
    {
        for (ProtocolVersion version : ProtocolVersion.SUPPORTED)
            queryOptionsSerDeserTest(version);
    }

    private void queryOptionsSerDeserTest(ProtocolVersion version) throws Exception
    {
        QueryOptions options = QueryOptions.create(ConsistencyLevel.ALL,
                                                   Collections.singletonList(ByteBuffer.wrap(new byte[] { 0x00, 0x01, 0x02 })),
                                                   false,
                                                   5000,
                                                   Util.makeSomePagingState(version),
                                                   ConsistencyLevel.SERIAL,
                                                   version
                                                   );

        ByteBuf buf = Unpooled.buffer(QueryOptions.codec.encodedSize(options, version));
        QueryOptions.codec.encode(options, buf, version);
        QueryOptions decodedOptions = QueryOptions.codec.decode(buf, version);

        assertNotNull(decodedOptions);
        assertEquals(options.getConsistency(), decodedOptions.getConsistency());
        assertEquals(options.getSerialConsistency(), decodedOptions.getSerialConsistency());
        assertEquals(options.getPageSize(), decodedOptions.getPageSize());
        assertEquals(options.getProtocolVersion(), decodedOptions.getProtocolVersion());
        assertEquals(options.getValues(), decodedOptions.getValues());
        assertEquals(options.getPagingState(), decodedOptions.getPagingState());
        assertEquals(options.skipMetadata(), decodedOptions.skipMetadata());
    }
}
