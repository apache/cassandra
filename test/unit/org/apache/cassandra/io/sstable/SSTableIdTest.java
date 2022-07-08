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

package org.apache.cassandra.io.sstable;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Supplier;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.common.collect.Sets;
import com.google.common.primitives.UnsignedBytes;
import org.junit.Test;

import de.huxhorn.sulky.ulid.ULID;
import org.apache.cassandra.concurrent.DebuggableThreadPoolExecutor;
import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.utils.TimeUUID;
import org.awaitility.Awaitility;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.quicktheories.QuickTheory.qt;
import static org.quicktheories.generators.SourceDSL.longs;

public class SSTableIdTest
{
    @Test
    public void testSequenceBasedIdProperties()
    {
        testSSTableIdProperties(SequenceBasedSSTableId.Builder.instance);
    }

    @Test
    public void testUUIDBasedIdProperties()
    {
        testSSTableIdProperties(UUIDBasedSSTableId.Builder.instance);
    }

    @Test
    public void testULIDBasedIdProperties()
    {
        testSSTableIdProperties(ULIDBasedSSTableId.Builder.instance);
    }

    private void testSSTableIdProperties(SSTableId.Builder<?> builder)
    {
        List<SSTableId> ids = Stream.generate(builder.generator(Stream.empty()))
                                    .limit(100).collect(Collectors.toList());
        assertThat(ids).isSorted();
        assertThat(Sets.newHashSet(ids)).hasSameSizeAs(ids);

        List<ByteBuffer> serIds = ids.stream().map(SSTableId::asBytes).collect(Collectors.toList());
        assertThat(serIds).isSortedAccordingTo((o1, o2) -> UnsignedBytes.lexicographicalComparator().compare(o1.array(), o2.array()));

        List<SSTableId> deserIds = serIds.stream().map(builder::fromBytes).collect(Collectors.toList());
        assertThat(deserIds).containsExactlyElementsOf(ids);

        List<String> stringifiedIds = ids.stream().map(SSTableId::toString).collect(Collectors.toList());
        if (!(builder instanceof SequenceBasedSSTableId.Builder))
        {
            // the legacy string representation is not sortable
            assertThat(stringifiedIds).isSorted();
        }

        List<SSTableId> destringifiedIds = stringifiedIds.stream().map(builder::fromString).collect(Collectors.toList());
        assertThat(destringifiedIds).containsExactlyElementsOf(ids);

        generatorFuzzTest(builder);
    }

    @Test
    public void testUUIDBytesSerDe()
    {
        qt().forAll(longs().all(), longs().all()).checkAssert((msb, lsb) -> {
            msb = (msb & ~0xf000) | 0x1000; // v1
            TimeUUID uuid = TimeUUID.fromBytes(msb, lsb);
            UUIDBasedSSTableId id = new UUIDBasedSSTableId(uuid);

            testBytesSerialization(id);
            testStringSerialization(id);
        });
    }

    @Test
    public void testULIDBytesSerDe()
    {
        qt().forAll(longs().all(), longs().all()).checkAssert((msb, lsb) -> {
            ULID.Value ulid = new ULID.Value(msb, lsb);
            ULIDBasedSSTableId id = new ULIDBasedSSTableId(ulid);

            testBytesSerialization(id);
            testStringSerialization(id);
        });
    }

    private void testBytesSerialization(UUIDBasedSSTableId id)
    {
        ByteBuffer buf = id.asBytes();
        assertThat(buf.remaining()).isEqualTo(UUIDBasedSSTableId.BYTES_LEN);
        assertThat(UUIDBasedSSTableId.Builder.instance.isUniqueIdentifier(buf)).isTrue();
        assertThat(ULIDBasedSSTableId.Builder.instance.isUniqueIdentifier(buf)).isTrue();
        assertThat(SequenceBasedSSTableId.Builder.instance.isUniqueIdentifier(buf)).isFalse();
        SSTableId fromBytes = SSTableIdFactory.instance.fromBytes(buf);
        assertThat(fromBytes).isEqualTo(id);
    }

    private void testBytesSerialization(ULIDBasedSSTableId id)
    {
        ByteBuffer buf = id.asBytes();
        assertThat(buf.remaining()).isEqualTo(ULIDBasedSSTableId.BYTES_LEN);
        assertThat(UUIDBasedSSTableId.Builder.instance.isUniqueIdentifier(buf)).isTrue();
        assertThat(ULIDBasedSSTableId.Builder.instance.isUniqueIdentifier(buf)).isTrue();
        assertThat(SequenceBasedSSTableId.Builder.instance.isUniqueIdentifier(buf)).isFalse();
        SSTableId fromBytes = SSTableIdFactory.instance.fromBytes(buf);
        assertThat(fromBytes).isInstanceOf(UUIDBasedSSTableId.class); // UUID and ULID bytes representation are indistinguishable
    }

    private void testStringSerialization(UUIDBasedSSTableId id)
    {
        String s = id.toString();
        assertThat(s).hasSize(UUIDBasedSSTableId.STRING_LEN);
        assertThat(s).matches(Pattern.compile("[0-9a-z]{4}_[0-9a-z]{4}_[0-9a-z]{18}"));
        assertThat(UUIDBasedSSTableId.Builder.instance.isUniqueIdentifier(s)).isTrue();
        assertThat(ULIDBasedSSTableId.Builder.instance.isUniqueIdentifier(s)).isFalse();
        assertThat(SequenceBasedSSTableId.Builder.instance.isUniqueIdentifier(s)).isFalse();
        SSTableId fromString = SSTableIdFactory.instance.fromString(s);
        assertThat(fromString).isEqualTo(id);
    }

    private void testStringSerialization(ULIDBasedSSTableId id)
    {
        String s = id.toString();
        assertThat(s).hasSize(ULIDBasedSSTableId.STRING_LEN);
        assertThat(s).matches(Pattern.compile("[0-9a-zA-Z]{26}"));
        assertThat(UUIDBasedSSTableId.Builder.instance.isUniqueIdentifier(s)).isFalse();
        assertThat(ULIDBasedSSTableId.Builder.instance.isUniqueIdentifier(s)).isTrue();
        assertThat(SequenceBasedSSTableId.Builder.instance.isUniqueIdentifier(s)).isFalse();
        SSTableId fromString = SSTableIdFactory.instance.fromString(s);
        assertThat(fromString).isEqualTo(id);
    }

    @Test
    public void testComparator()
    {
        ULID ulid = new ULID();
        List<SSTableId> ids = new ArrayList<>(Collections.nCopies(400, null));
        for (int i = 0; i < 100; i++)
        {
            ids.set(i + 200, new SequenceBasedSSTableId(ThreadLocalRandom.current().nextInt(1000000)));
            long ts = System.currentTimeMillis() + ThreadLocalRandom.current().nextLong(10000);
            ids.set(i + 100, new UUIDBasedSSTableId(TimeUUID.Generator.atUnixMillis(ts)));
            ids.set(i, new ULIDBasedSSTableId(ulid.nextValue(ts)));
        }

        List<SSTableId> shuffledIds = new ArrayList<>(ids);
        Collections.shuffle(shuffledIds);
        assertThat(shuffledIds).isNotEqualTo(ids);

        List<SSTableId> sortedIds = new ArrayList<>(shuffledIds);
        sortedIds.sort(SSTableIdFactory.COMPARATOR);
        assertThat(sortedIds).isNotEqualTo(shuffledIds);
        assertThat(sortedIds).isSortedAccordingTo(SSTableIdFactory.COMPARATOR);

        assertThat(sortedIds.subList(0, 100)).containsOnlyNulls();
        assertThat(sortedIds.subList(100, 200)).allMatch(id -> id instanceof SequenceBasedSSTableId);
        assertThat(sortedIds.subList(200, 400)).allMatch(id -> id instanceof UUIDBasedSSTableId || id instanceof ULIDBasedSSTableId);

        assertThat(sortedIds.subList(100, 200)).isSortedAccordingTo(Comparator.comparing(o -> ((SequenceBasedSSTableId) o)));
        assertThat(sortedIds.subList(200, 400)).isSortedAccordingTo(SSTableIdFactory.COMPARATOR);
        assertThat(sortedIds.subList(200, 400).stream().map(id -> {
            if (id instanceof ULIDBasedSSTableId)
            {
                return ((ULIDBasedSSTableId) id).ulid.timestamp();
            }
            else
            {
                return ((UUIDBasedSSTableId) id).uuid.unixMillis();
            }
        })).isSorted();
    }

    @Test
    public void testDefaultFactorySelection()
    {
        try
        {
            byte[] bytes = new byte[16];
            new Random().nextBytes(bytes);

            DatabaseDescriptor.clientInitialization();
            DatabaseDescriptor.getRawConfig().enable_uuid_sstable_identifiers = false;
            CassandraRelevantProperties.SSTABLE_UUID_IMPL.setString("uuid");
            assertThat(SSTableIdFactory.instance.defaultBuilder()).isInstanceOf(SequenceBasedSSTableId.Builder.class);
            assertThat(SSTableIdFactory.instance.fromBytes(ByteBuffer.wrap(bytes))).isInstanceOf(UUIDBasedSSTableId.class);

            DatabaseDescriptor.getRawConfig().enable_uuid_sstable_identifiers = true;
            CassandraRelevantProperties.SSTABLE_UUID_IMPL.setString("uuid");
            assertThat(SSTableIdFactory.instance.defaultBuilder()).isInstanceOf(UUIDBasedSSTableId.Builder.class);
            assertThat(SSTableIdFactory.instance.fromBytes(ByteBuffer.wrap(bytes))).isInstanceOf(UUIDBasedSSTableId.class);

            DatabaseDescriptor.getRawConfig().enable_uuid_sstable_identifiers = true;
            CassandraRelevantProperties.SSTABLE_UUID_IMPL.setString("ulid");
            assertThat(SSTableIdFactory.instance.defaultBuilder()).isInstanceOf(ULIDBasedSSTableId.Builder.class);
            assertThat(SSTableIdFactory.instance.fromBytes(ByteBuffer.wrap(bytes))).isInstanceOf(ULIDBasedSSTableId.class);

            DatabaseDescriptor.getRawConfig().enable_uuid_sstable_identifiers = true;
            CassandraRelevantProperties.SSTABLE_UUID_IMPL.setString("something");
            assertThatExceptionOfType(IllegalArgumentException.class).isThrownBy(SSTableIdFactory.instance::defaultBuilder);
            assertThatExceptionOfType(IllegalArgumentException.class).isThrownBy(() -> SSTableIdFactory.instance.fromBytes(ByteBuffer.wrap(bytes)));

            DatabaseDescriptor.getRawConfig().enable_uuid_sstable_identifiers = true;
            System.getProperties().remove(CassandraRelevantProperties.SSTABLE_UUID_IMPL.getKey());
            assertThat(SSTableIdFactory.instance.defaultBuilder()).isInstanceOf(UUIDBasedSSTableId.Builder.class);
            assertThat(SSTableIdFactory.instance.fromBytes(ByteBuffer.wrap(bytes))).isInstanceOf(UUIDBasedSSTableId.class);
        }
        finally
        {
            DatabaseDescriptor.getRawConfig().enable_uuid_sstable_identifiers = new Config().enable_uuid_sstable_identifiers;
            System.getProperties().remove(CassandraRelevantProperties.SSTABLE_UUID_IMPL.getKey());
        }
    }

    private static <T extends SSTableId> void generatorFuzzTest(SSTableId.Builder<T> builder)
    {
        final int NUM_THREADS = 10, IDS_PER_THREAD = 10;
        Set<SSTableId> ids = new CopyOnWriteArraySet<>();
        Supplier<T> generator = builder.generator(Stream.empty());
        DebuggableThreadPoolExecutor service = DebuggableThreadPoolExecutor.createWithFixedPoolSize("test", NUM_THREADS);
        CyclicBarrier barrier = new CyclicBarrier(NUM_THREADS);
        for (int i = 0; i < NUM_THREADS; i++)
        {
            service.submit(() -> {
                for (int k = 0; k < IDS_PER_THREAD; k++)
                {
                    barrier.await();
                    ids.add(generator.get());
                }
                return null;
            });
        }

        Awaitility.await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> assertThat(service.getCompletedTaskCount()).isEqualTo(NUM_THREADS));
        assertThat(ids).hasSize(NUM_THREADS * IDS_PER_THREAD);
    }
}
