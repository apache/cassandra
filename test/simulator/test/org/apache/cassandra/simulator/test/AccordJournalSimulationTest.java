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
package org.apache.cassandra.simulator.test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Optional;
import java.util.Set;
import java.util.zip.Checksum;
import javax.annotation.Nullable;

import com.google.common.collect.ImmutableMap;
import com.google.common.jimfs.Jimfs;

import accord.local.Node;
import accord.primitives.TxnId;
import org.apache.cassandra.concurrent.ExecutorFactory;
import org.apache.cassandra.concurrent.ExecutorPlus;
import org.apache.cassandra.config.AccordSpec;
import org.apache.cassandra.config.Config;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.distributed.test.log.CMSTestBase;
import org.apache.cassandra.harry.sut.TokenPlacementModel;
import org.apache.cassandra.io.filesystem.ListenableFileSystem;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.journal.AsyncCallbacks;
import org.apache.cassandra.journal.Journal;
import org.apache.cassandra.journal.KeySupport;
import org.apache.cassandra.journal.ValueSerializer;
import org.apache.cassandra.schema.*;

import org.junit.Assert;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.ParameterizedClass;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.tcm.AtomicLongBackedProcessor;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.utils.Isolated;
import org.apache.cassandra.utils.concurrent.CountDownLatch;

public class AccordJournalSimulationTest extends SimulationTestBase
{
    @Test
    public void simpleRWTest() throws IOException
    {
        simulate(arr(() -> {
                     ListenableFileSystem fs = new ListenableFileSystem(Jimfs.newFileSystem());
                     File.unsafeSetFilesystem(fs);
                     DatabaseDescriptor.daemonInitialization();
                     DatabaseDescriptor.setCommitLogCompression(new ParameterizedClass("LZ4Compressor", ImmutableMap.of())); //
                     DatabaseDescriptor.setCommitLogWriteDiskAccessMode(Config.DiskAccessMode.standard);
                     DatabaseDescriptor.initializeCommitLogDiskAccessMode();
                     System.out.println("DatabaseDescriptor.getCommitLogWriteDiskAccessMode() = " + DatabaseDescriptor.getCommitLogWriteDiskAccessMode());

                     DatabaseDescriptor.setPartitionerUnsafe(Murmur3Partitioner.instance);
                     long seed = 1L;
                     DatabaseDescriptor.setAccordJournalDirectory("/journal");
                     new File("/journal").createDirectoriesIfNotExists();

                     DatabaseDescriptor.setDumpHeapOnUncaughtException(false);

                     Keyspace.setInitialized();

                     CMSTestBase.CMSSut sut = new CMSTestBase.CMSSut(AtomicLongBackedProcessor::new,
                                                                     false,
                                                                     new FakeSchema(),
                                                                     new TokenPlacementModel.SimpleReplicationFactor(1));

                     State.journal = new Journal<>("AccordJournal",
                                                   new File("/journal"),
                                                   new AccordSpec.JournalSpec(),
                                                   new TestCallbacks(),
                                                   new IdentityKeySerializer(),
                                                   new IdentityValueSerializer());
                 }),
                 () -> check());
    }

    public static void check()
    {
        State.journal.start();
        try
        {
            for (int i = 0; i < 100; i++)
            {
                int finalI = i;
                State.executor.submit(() -> State.journal.asyncWrite("test" + finalI, "test" + finalI, Collections.singleton(1), null));
            }

            State.latch.await();

            for (int i = 0; i < 100; i++)
            {
                System.out.println("Reading " + State.journal.readFirst("test" + i));
                Assert.assertEquals(State.journal.readFirst("test" + i), "test" + i);
            }
        }
        catch (Throwable e)
        {
            e.printStackTrace();
        }
        finally
        {
            State.journal.shutdown();
        }
    }

    public static class TestCallbacks implements AsyncCallbacks<String, String>
    {

        @Override
        public void onWrite(long segment, int position, int size, String key, String value, Object writeContext)
        {
            State.latch.decrement();
        }

        @Override
        public void onWriteFailed(String key, String value, Object writeContext, Throwable cause)
        {
            State.latch.decrement();
        }

        @Override
        public void onFlush(long segment, int position)
        {
        }

        @Override
        public void onFlushFailed(Throwable cause)
        {
            new RuntimeException("Could not flush", cause).printStackTrace();
        }
    }

    @Isolated
    public static class IdentityValueSerializer implements ValueSerializer<String, String>
    {

        @Override
        public int serializedSize(String key, String value, int userVersion)
        {
            return TypeSizes.INT_SIZE + key.length();
        }

        @Override
        public void serialize(String key, String value, DataOutputPlus out, int userVersion) throws IOException
        {
            out.writeInt(key.length());
            out.writeBytes(key);
        }

        @Override
        public String deserialize(String key, DataInputPlus in, int userVersion) throws IOException
        {
            int size = in.readInt();
            byte[] value = new byte[size];
            for (int i = 0; i < size; i++)
                value[i] = in.readByte();

            return new String(value);
        }
    }

    @Isolated
    public static class IdentityKeySerializer implements KeySupport<String>
    {

        @Override
        public int serializedSize(int userVersion)
        {
            return 16;
        }

        @Override
        public void serialize(String key, DataOutputPlus out, int userVersion) throws IOException
        {
            int maxSize = 16 - TypeSizes.INT_SIZE;
            if (key.length() > maxSize)
                throw new IllegalStateException();

            out.writeInt(key.length());
            out.writeBytes(key);
            int remaining = maxSize - key.length();
            for (int i = 0; i < remaining; i++)
                out.writeByte(0);
        }

        @Override
        public String deserialize(DataInputPlus in, int userVersion) throws IOException
        {
            int size = in.readInt();
            byte[] key = new byte[size];
            for (int i = 0; i < size; i++)
                key[i] = in.readByte();

            int maxSize = 16 - TypeSizes.INT_SIZE;
            int remaining = maxSize - size;
            for (int i = 0; i < remaining; i++)
                in.readByte(); // todo assert 0

            return new String(key);
        }

        @Override
        public String deserialize(ByteBuffer buffer, int position, int userVersion)
        {
            int size = buffer.getInt();
            byte[] key = new byte[size];
            for (int i = 0; i < size; i++)
                key[i] = buffer.get();

            int maxSize = 16 - TypeSizes.INT_SIZE;
            int remaining = maxSize - size;
            for (int i = 0; i < remaining; i++)
                buffer.get(); // todo assert 0

            return new String(key);
        }

        @Override
        public void updateChecksum(Checksum crc, String key, int userVersion)
        {
            crc.update(key.getBytes());
        }

        @Override
        public int compareWithKeyAt(String key, ByteBuffer buffer, int position, int userVersion)
        {
            throw new IllegalStateException();
        }

        @Override
        public int compare(String o1, String o2)
        {
            return o1.compareTo(o2);
        }
    }

    @Isolated
    public static class State
    {
        static Journal<String, String> journal;
        static CountDownLatch latch = CountDownLatch.newCountDownLatch(100);
        static ExecutorPlus executor = ExecutorFactory.Global.executorFactory().pooled("name", 10);

        private static TxnId toTxnId(int event)
        {
            return TxnId.fromValues(1, event, 0, new Node.Id(1));
        }
    }

    @Isolated
    public static class FakeSchema implements SchemaProvider
    {

        @Override
        public Set<String> getKeyspaces()
        {
            return Set.of();
        }

        @Override
        public int getNumberOfTables()
        {
            return 0;
        }

        @Override
        public ClusterMetadata submit(SchemaTransformation transformation)
        {
            return null;
        }

        @Override
        public Keyspaces localKeyspaces()
        {
            return null;
        }

        @Override
        public Keyspaces distributedKeyspaces()
        {
            return null;
        }

        @Override
        public Keyspaces distributedAndLocalKeyspaces()
        {
            return null;
        }

        @Override
        public Keyspaces getUserKeyspaces()
        {
            return null;
        }

        @Override
        public void registerListener(SchemaChangeListener listener)
        {

        }

        @Override
        public void unregisterListener(SchemaChangeListener listener)
        {

        }

        @Override
        public SchemaChangeNotifier schemaChangeNotifier()
        {
            return null;
        }

        @Override
        public Optional<TableMetadata> getIndexMetadata(String keyspace, String index)
        {
            return Optional.empty();
        }

        @Override
        public Iterable<TableMetadata> getTablesAndViews(String keyspaceName)
        {
            return null;
        }

        @Nullable
        @Override
        public Keyspace getKeyspaceInstance(String keyspaceName)
        {
            return null;
        }

        @Nullable
        @Override
        public KeyspaceMetadata getKeyspaceMetadata(String keyspaceName)
        {
            return null;
        }

        @Nullable
        @Override
        public TableMetadata getTableMetadata(TableId id)
        {
            return null;
        }

        @Nullable
        @Override
        public TableMetadata getTableMetadata(String keyspace, String table)
        {
            return null;
        }

        @Override
        public void saveSystemKeyspace()
        {

        }
    }
}
