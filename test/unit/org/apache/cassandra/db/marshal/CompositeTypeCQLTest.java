/*
 * Copyright IBM Corp.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.db.marshal;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.FieldIdentifier;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.index.SecondaryIndexManager;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputBufferFixed;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.serializers.AbstractTypeSerializer;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.assertj.core.api.Assertions;
import org.awaitility.core.ThrowingRunnable;

public class CompositeTypeCQLTest extends CQLTester
{
    @Test
    public void testNoCorruptionDetectedOnSerializationRoundtripUDT() throws Throwable
    {
        UserType udt = new UserType("ks", ByteBufferUtil.bytes("udt"),
                                    List.of(FieldIdentifier.forQuoted("int")),
                                    List.of(Int32Type.instance),
                                    false);
        testNoCorruptionDetectedOnSerializationRoundtrip(udt);
    }

    @Test
    public void testNoCorruptionDetectedOnSerializationRoundtripTuple() throws Throwable
    {
        testNoCorruptionDetectedOnSerializationRoundtrip(new TupleType(Arrays.asList(Int32Type.instance, Int32Type.instance), false));
    }

    @Test
    public void testNoCorruptionDetectedOnSerializationRoundtripList() throws Throwable
    {
        testNoCorruptionDetectedOnSerializationRoundtrip(ListType.getInstance(Int32Type.instance, false));
    }

    @Test
    public void testNoCorruptionDetectedOnSerializationRoundtripSet() throws Throwable
    {
        testNoCorruptionDetectedOnSerializationRoundtrip(SetType.getInstance(Int32Type.instance, false));
    }

    @Test
    public void testNoCorruptionDetectedOnSerializationRoundtripMap() throws Throwable
    {
        testNoCorruptionDetectedOnSerializationRoundtrip(MapType.getInstance(Int32Type.instance, Int32Type.instance, false));
    }

    private void testNoCorruptionDetectedOnSerializationRoundtrip(AbstractType<?> subtype) throws Throwable
    {
        assert !subtype.isMultiCell();

        assertNoCorruptionDetected(() -> {

            CompositeType type = CompositeType.getInstance(subtype);

            // everything in the type is frozen, but frozen is not printed in string representations
            Assertions.assertThat(type).matches(t -> !t.isMultiCell());
            Assertions.assertThat(type.subTypes()).noneMatch(AbstractType::isMultiCell);
            Assertions.assertThat(type.toString(false)).doesNotContainIgnoringCase("frozen");
            Assertions.assertThat(type.toString(true)).doesNotContainIgnoringCase("frozen");

            // serialize the type, which is based on toString, frozen is not added
            AbstractTypeSerializer serializer = new AbstractTypeSerializer();
            long size = serializer.serializedSize(type);
            ByteBuffer buf = ByteBuffer.allocate((int) size);
            DataOutputPlus out = new DataOutputBufferFixed(buf);
            serializer.serialize(type, out);

            // deserialize the type, it shouldn't emit a log error about detected and fixed corrupted type (CNDB-18411)
            buf.flip();
            DataInputPlus in = new DataInputBuffer(buf, false);
            AbstractType<?> deserialized = serializer.deserialize(in);
            Assert.assertEquals(type, deserialized);
        });
    }

    @Test
    public void testNoCorruptionDetectedOnPartitionKeyUDT() throws Throwable
    {
        assertNoCorruptionDetected(() -> {
            String udt = createType("CREATE TYPE %s (n int)");
            createTable("CREATE TABLE %s(k1 int, k2 frozen<" + udt + ">, v int, PRIMARY KEY ((k1, k2)))");
            execute("INSERT INTO %s(k1, k2, v) VALUES (2, {n: 0}, 0)");
            flush();
            openFirstSSTable();
        });
    }

    @Test
    public void testNoCorruptionDetectedOnPartitionKeyTuple() throws Throwable
    {
        assertNoCorruptionDetected(() -> {
            createTable("CREATE TABLE %s(k1 int, k2 tuple<int, int>, v int, PRIMARY KEY ((k1, k2)))");
            execute("INSERT INTO %s(k1, k2, v) VALUES (1, (2, 3), 0)");
            flush();
            openFirstSSTable();
        });
    }

    @Test
    public void testNoCorruptionDetectedOnPartitionKeyList() throws Throwable
    {
        assertNoCorruptionDetected(() -> {
            createTable("CREATE TABLE %s(k1 int, k2 frozen<list<int>>, v int, PRIMARY KEY ((k1, k2)))");
            execute("INSERT INTO %s(k1, k2, v) VALUES (1, [2, 3], 0)");
            flush();
            openFirstSSTable();
        });
    }

    @Test
    public void testNoCorruptionDetectedOnPartitionKeySet() throws Throwable
    {
        assertNoCorruptionDetected(() -> {
            createTable("CREATE TABLE %s(k1 int, k2 frozen<set<int>>, v int, PRIMARY KEY ((k1, k2)))");
            execute("INSERT INTO %s(k1, k2, v) VALUES (1, {2, 3}, 0)");
            flush();
            openFirstSSTable();
        });
    }

    @Test
    public void testNoCorruptionDetectedOnPartitionKeyMap() throws Throwable
    {
        assertNoCorruptionDetected(() -> {
            createTable("CREATE TABLE %s(k1 int, k2 frozen<map<int, int>>, v int, PRIMARY KEY ((k1, k2)))");
            execute("INSERT INTO %s(k1, k2, v) VALUES (1, {2:20, 3:30}, 0)");
            flush();
            openFirstSSTable();
        });
    }

    @Test
    public void testNoCorruptionDetectedOnIndex() throws Throwable
    {
        assertNoCorruptionDetected(() -> {

            String udt = createType("CREATE TYPE %s (n int)");
            createTable("CREATE TABLE %s(k int PRIMARY KEY, m map<int, frozen<" + udt + ">>)");
            createIndex("CREATE INDEX ON %s(entries(m))");

            execute("INSERT INTO %s(k, m) VALUES (2, {0: {n: 0}} )");
            execute("INSERT INTO %s(k, m) VALUES (3, {0: {n: 0}, 1: {n: 1}} )");
            execute("INSERT INTO %s(k, m) VALUES (4, {1: {n: 1}, 2: {n: 2}} )");
            flush();

            // Open the index sstable so it calls SSTableReader.validate().
            SecondaryIndexManager sim = getCurrentColumnFamilyStore().indexManager;
            ColumnFamilyStore indexCfs = sim.getAllIndexColumnFamilyStores().iterator().next();
            openFirstSSTable(indexCfs);
        });
    }

    private void openFirstSSTable()
    {
        openFirstSSTable(getCurrentColumnFamilyStore());
    }

    private void openFirstSSTable(ColumnFamilyStore cfs)
    {
        SSTableReader sstable = cfs.getLiveSSTables().iterator().next();
        SSTableReader.openNoValidation(sstable.descriptor, cfs.metadata);
    }

    private void assertNoCorruptionDetected(ThrowingRunnable runnable) throws Throwable
    {
        withLogAppender(AbstractType.class, appender -> {
            runnable.run();
            Assertions.assertThat(appender.list)
                      .noneMatch(e -> e.getFormattedMessage().contains("Detected corrupted type"));
        });
    }
}
