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
package org.apache.cassandra.service.accord;

import java.nio.file.Files;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import com.google.common.collect.ImmutableMap;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import accord.local.Node;
import accord.local.RedundantBefore;
import accord.primitives.Timestamp;
import accord.primitives.TxnId;
import accord.utils.AccordGens;
import accord.utils.DefaultRandom;
import accord.utils.Gen;
import accord.utils.Gens;
import accord.utils.RandomSource;
import org.apache.cassandra.ServerTestUtils;
import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.ParameterizedClass;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.journal.TestParams;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.AccordGenerators;
import org.apache.cassandra.utils.AsymmetricOrdering;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.FBUtilities.Order;
import org.apache.cassandra.utils.StorageCompatibilityMode;
import org.checkerframework.checker.nullness.qual.Nullable;

import static accord.utils.Property.qt;
import static org.assertj.core.api.Assertions.assertThat;

public class AccordJournalTest
{
    @BeforeClass
    public static void setCompatibilityMode() throws IOException
    {
        CassandraRelevantProperties.TEST_STORAGE_COMPATIBILITY_MODE.setEnum(StorageCompatibilityMode.NONE);

        ServerTestUtils.daemonInitialization();
        StorageService.instance.registerMBeans();
        StorageService.instance.setPartitionerUnsafe(Murmur3Partitioner.instance);
        ServerTestUtils.prepareServerNoRegister();

        File directory = new File(Files.createTempDirectory(null));
        directory.deleteRecursiveOnExit();
        DatabaseDescriptor.setAccordJournalDirectory(directory.path());
        StorageService.instance.initServer();
        Keyspace.setInitialized();
    }

    @Test
    public void keySerde()
    {
        DataOutputBuffer buffer = new DataOutputBuffer();
        qt().forAll(keyGen()).check(key ->
        {
            buffer.clear();
            int expectedSize = JournalKey.SUPPORT.serializedSize(1);
            JournalKey.SUPPORT.serialize(key, buffer, 1);
            assertThat(buffer.getLength()).isEqualTo(expectedSize);
            try (DataInputBuffer input = new DataInputBuffer(buffer.unsafeGetBufferAndFlip(), false))
            {
                JournalKey read = JournalKey.SUPPORT.deserialize(input, 1);
                assertThat(read).isEqualTo(key);
            }
        });
    }

    @Test
    public void compareKeys()
    {
        qt().forAll(Gens.lists(keyGen()).ofSizeBetween(2, 100)).check(keys ->
        {
            keys.sort(JournalKey.SUPPORT);

            List<ByteBuffer> buffers = new ArrayList<>(keys.size());
            for (JournalKey k : keys) buffers.add(toBuffer(k));

            for (int i = 0; i < keys.size(); i++)
            {
                JournalKey outerKey = keys.get(i);
                for (int j = 0; j < keys.size(); j++)
                {
                    JournalKey innerKey = keys.get(j);
                    ByteBuffer innerBuffer = buffers.get(j);
                    Order expected = FBUtilities.compare(outerKey, innerKey, JournalKey.SUPPORT);
                    Order actual = FBUtilities.compare(outerKey, innerBuffer, new AsymmetricOrdering<JournalKey, ByteBuffer>()
                    {
                        @Override
                        public int compareAsymmetric(JournalKey left, ByteBuffer right)
                        {
                            return JournalKey.SUPPORT.compareWithKeyAt(left, right, 0, 1);
                        }

                        @Override
                        public int compare(@Nullable JournalKey left, @Nullable JournalKey right)
                        {
                            throw new UnsupportedOperationException();
                        }
                    });
                    assertThat(actual).isEqualTo(expected);
                }
            }
        });
    }

    @Test
    public void mergeKeysTest()
    {
        AccordJournal accordJournal = new AccordJournal(TestParams.INSTANCE);
        try
        {
            accordJournal.start(null);
            Gen<Timestamp> timestampGen = AccordGens.timestamps();
            // TODO: we might benefit from some unification of generators
            Gen<RedundantBefore> redundantBeforeGen = AccordGenerators.redundantBefore(DatabaseDescriptor.getPartitioner());
            RandomSource rng = new DefaultRandom();
            // Probably all redundant befores will be written with the same timestamp?
            Timestamp timestamp = timestampGen.next(rng);
            RedundantBefore expected = RedundantBefore.EMPTY;
            for (int i = 0; i < 10; i++)
            {
                RedundantBefore redundantBefore = redundantBeforeGen.next(rng);
                expected = RedundantBefore.merge(expected, redundantBefore);
                accordJournal.append(timestamp, JournalKey.Type.REDUNDANT_BEFORE, 1, redundantBefore, () -> {});
            }

            AccordJournalValueSerializers.RedundantBeforeAccumulator actual = accordJournal.readAll(timestamp, JournalKey.Type.REDUNDANT_BEFORE, 1);
            Assert.assertEquals(expected, actual.get());
        }
        finally
        {
            accordJournal.shutdown();
        }
    }

    private static ByteBuffer toBuffer(JournalKey k)
    {
        try (DataOutputBuffer buffer = new DataOutputBuffer(JournalKey.SUPPORT.serializedSize(1)))
        {
            JournalKey.SUPPORT.serialize(k, buffer, 1);
            return buffer.unsafeGetBufferAndFlip();
        }
        catch (IOException e)
        {
            throw new UncheckedIOException(e);
        }
    }

    private Gen<JournalKey> keyGen()
    {
        Gen<TxnId> txnIdGen = AccordGens.txnIds();
        return rs -> new JournalKey(txnIdGen.next(rs), JournalKey.Type.COMMAND_DIFF, -1);
    }
}
