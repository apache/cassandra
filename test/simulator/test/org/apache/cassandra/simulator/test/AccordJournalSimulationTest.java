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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.zip.Checksum;

import com.google.common.collect.ImmutableMap;
import com.google.common.jimfs.Jimfs;

import org.apache.cassandra.concurrent.ExecutorFactory;
import org.apache.cassandra.concurrent.ExecutorPlus;
import org.apache.cassandra.config.AccordSpec;
import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DurationSpec;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.filesystem.ListenableFileSystem;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.journal.Journal;
import org.apache.cassandra.journal.KeySupport;
import org.apache.cassandra.journal.RecordPointer;
import org.apache.cassandra.journal.SegmentCompactor;
import org.apache.cassandra.journal.ValueSerializer;

import org.junit.Assert;
import org.junit.Test;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.ParameterizedClass;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.utils.Isolated;
import org.apache.cassandra.utils.concurrent.CountDownLatch;

public class AccordJournalSimulationTest extends SimulationTestBase
{
    @Test
    public void simpleRWTest()
    {
        simulate(arr(() -> {
                     ListenableFileSystem fs = new ListenableFileSystem(Jimfs.newFileSystem());
                     File.unsafeSetFilesystem(fs);
                     DatabaseDescriptor.daemonInitialization();
                     DatabaseDescriptor.setCommitLogCompression(new ParameterizedClass("LZ4Compressor", ImmutableMap.of()));
                     DatabaseDescriptor.setCommitLogWriteDiskAccessMode(Config.DiskAccessMode.standard);
                     DatabaseDescriptor.initializeCommitLogDiskAccessMode();
                     DatabaseDescriptor.setPartitionerUnsafe(Murmur3Partitioner.instance);
                     DatabaseDescriptor.setAccordJournalDirectory("/journal");
                     new File("/journal").createDirectoriesIfNotExists();

                     DatabaseDescriptor.setDumpHeapOnUncaughtException(false);

                     Keyspace.setInitialized();

                     AccordSpec.JournalSpec spec = new AccordSpec.JournalSpec();
                     spec.flushPeriod = new DurationSpec.IntSecondsBound(1);

                     State.journal = new Journal<>("AccordJournal",
                                                   new File("/journal"),
                                                   spec,
                                                   new IdentityKeySerializer(),
                                                   new IdentityValueSerializer(),
                                                   SegmentCompactor.noop());
                 }),
                 () -> check());
    }

    public static void check()
    {
        State.journal.start();
        try
        {
            final int count = 100;
            for (int i = 0; i < count; i++)
            {
                int finalI = i;
                State.executor.submit(() -> {
                    RecordPointer ptr = State.journal.asyncWrite("test" + finalI, "test" + finalI, Collections.singleton(1));
                    State.journal.onDurable(ptr, State.latch::decrement);
                });
            }

            State.latch.await();

            for (int i = 0; i < count; i++)
            {
                State.logger.debug("Reading {}", i);
                Assert.assertEquals(State.journal.readLast("test" + i), "test" + i);
            }
        }

        catch (InterruptedException e)
        {
            throw new RuntimeException(e);
        }
        finally
        {
            State.journal.shutdown();

            if (!State.thrown.isEmpty())
            {
                AssertionError throwable = new AssertionError("Caught exceptions");
                for (Throwable t: State.thrown)
                    throwable.addSuppressed(t);
                throw throwable;
            }
        }
    }

    @Isolated
    public static class IdentityValueSerializer implements ValueSerializer<String, String>
    {
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
        private final byte aByte = 0xd;
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
                out.writeByte(aByte + i);
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
                Assert.assertEquals(aByte + i, in.readByte());

            return new String(key);
        }

        @Override
        public void serialize(String key, ByteBuffer out, int userVersion) throws IOException
        {
            int maxSize = 16 - TypeSizes.INT_SIZE;
            if (key.length() > maxSize)
                throw new IllegalStateException();

            out.putInt(key.length());
            for (int i = 0 ; i < key.length() ; ++i)
                out.put((byte)key.charAt(i));
            int remaining = maxSize - key.length();
            for (int i = 0; i < remaining; i++)
                out.put((byte) (aByte + i));
        }

        @Override
        public String deserialize(ByteBuffer in, int userVersion)
        {
            int size = in.getInt();
            byte[] key = new byte[size];
            for (int i = 0; i < size; i++)
                key[i] = in.get();

            int maxSize = 16 - TypeSizes.INT_SIZE;
            int remaining = maxSize - size;
            for (int i = 0; i < remaining; i++)
                Assert.assertEquals(aByte + i, in.get());

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
                Assert.assertEquals(aByte + i, buffer.get());

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
        private static final Logger logger = LoggerFactory.getLogger(State.class);
        static Journal<String, String> journal;
        static CountDownLatch latch = CountDownLatch.newCountDownLatch(100);
        static List<Throwable> thrown = new ArrayList<>();
        static ExecutorPlus executor = ExecutorFactory.Global.executorFactory().pooled("name", 10);
    }
}