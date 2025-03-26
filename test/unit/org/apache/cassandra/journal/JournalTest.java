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
package org.apache.cassandra.journal;

import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.UnaryOperator;

import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.ServerTestUtils;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.harry.checker.TestHelper;
import org.apache.cassandra.harry.gen.EntropySource;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.utils.TimeUUID;
import org.apache.cassandra.utils.concurrent.CountDownLatch;

import static org.apache.cassandra.utils.TimeUUID.Generator.nextTimeUUID;
import static org.junit.Assert.assertEquals;

public class JournalTest
{
    private static final Logger logger = LoggerFactory.getLogger(JournalTest.class);

    @BeforeClass
    public static void setUp()
    {
        DatabaseDescriptor.daemonInitialization();
        ServerTestUtils.prepareServer();
    }

    @Test
    public void testSimpleReadWrite() throws Throwable
    {
        File directory = new File(Files.createTempDirectory("JournalTest"));
        directory.deleteRecursiveOnExit();

        Journal<TimeUUID, Long> journal =
        new Journal<>("TestJournal", directory, TestParams.INSTANCE, TimeUUIDKeySupport.INSTANCE, LongSerializer.INSTANCE, SegmentCompactor.noop());

        journal.start();

        TimeUUID id1 = nextTimeUUID();
        TimeUUID id2 = nextTimeUUID();
        TimeUUID id3 = nextTimeUUID();
        TimeUUID id4 = nextTimeUUID();

        journal.blockingWrite(id1, 1L);
        journal.blockingWrite(id2, 2L);
        journal.blockingWrite(id3, 3L);
        journal.blockingWrite(id4, 4L);

        assertEquals(1L, (long) journal.readLast(id1));
        assertEquals(2L, (long) journal.readLast(id2));
        assertEquals(3L, (long) journal.readLast(id3));
        assertEquals(4L, (long) journal.readLast(id4));

        journal.shutdown();
        journal.awaitTermination(10, TimeUnit.SECONDS);
        journal = new Journal<>("TestJournal", directory, TestParams.INSTANCE, TimeUUIDKeySupport.INSTANCE, LongSerializer.INSTANCE, SegmentCompactor.noop());
        journal.start();

        assertEquals(1L, (long) journal.readLast(id1));
        assertEquals(2L, (long) journal.readLast(id2));
        assertEquals(3L, (long) journal.readLast(id3));
        assertEquals(4L, (long) journal.readLast(id4));

        journal.shutdown();
    }

    @Test
    public void sequentialReadWriteTest() throws Throwable
    {
        File directory = new File(Files.createTempDirectory("JournalTest"));
        directory.deleteRecursiveOnExit();

        final Journal<TimeUUID, Long> journal = new Journal<>("TestJournal", directory,
                                                              new TestParams() {
                                                                  @Override
                                                                  public int segmentSize()
                                                                  {
                                                                      return 1024;
                                                                  }
                                                              },
                                                              TimeUUIDKeySupport.INSTANCE,
                                                              LongSerializer.INSTANCE,
                                                              SegmentCompactor.noop());

        journal.start();


        int cycles = 10_000;
        TestHelper.withRandom(rng -> {
            Map<TimeUUID, Long> written = new ConcurrentHashMap<>();
            for (int j = 0; j < cycles; j++)
            {
                if (j > 0 && j % 1000 == 0)
                    logger.info("Progress: {}/{}", j, cycles);
                TimeUUID uuid = nextTimeUUID();
                long v = rng.next();
                journal.blockingWrite(uuid, v);
                written.put(uuid, v);
            }

            int i = 0;
            for (Map.Entry<TimeUUID, Long> e : written.entrySet())
            {
                long expected = e.getValue();
                Long actual = journal.readLast(e.getKey());
                assertEquals(expected, (long) actual);
            }
        });

        journal.shutdown();
    }

    @Test
    public void concurrentReadWriteTest() throws Throwable
    {
        for (int segmentSize : new int[] {1024, 1024 * 1024})
        {
            for (Params.FlushMode mode : Params.FlushMode.values())
            {
                logger.info("Starting a test run for {} with {}b segment size", mode, segmentSize);
                concurrentReadWriteTest(segmentSize, mode);
            }
        }
    }

    public void concurrentReadWriteTest(int segmentSize, Params.FlushMode mode) throws Throwable
    {
        File directory = new File(Files.createTempDirectory("JournalTest"));
        directory.deleteRecursiveOnExit();

        final Journal<TimeUUID, Long> journal =
        new Journal<>("TestJournal", directory, new TestParams() {
            @Override
            public int segmentSize()
            {
                return segmentSize;
            }

            @Override
            public FlushMode flushMode()
            {
                return mode;
            }
        }, TimeUUIDKeySupport.INSTANCE, LongSerializer.INSTANCE, SegmentCompactor.noop());

        journal.start();

        AtomicLong completedWrites = new AtomicLong();
        AtomicLong completedReads = new AtomicLong();

        int cycles = 10_000;
        TestHelper.withRandom(rng -> {
            List<Thread> threads = new ArrayList<>();

            Map<TimeUUID, Long> written = new ConcurrentHashMap<>();
            Queue<TimeUUID> recentlyWritten = new ConcurrentLinkedQueue<>();
            int writers = 10;
            CountDownLatch writersDone = CountDownLatch.newCountDownLatch(writers);
            AtomicReference<Throwable> accumulate = new AtomicReference<>();
            for (int i = 0; i < writers; i++)
            {
                EntropySource perThread = rng.derive();
                Thread writer = new Thread(run(() -> {
                    for (int j = 0; j < cycles; j++)
                    {
                        if (j > 0 && j % 500 == 0)
                            logger.info("Progress: {}/{}", j, cycles);

                        TimeUUID uuid = nextTimeUUID();
                        long v = perThread.next();
                        journal.blockingWrite(uuid, v);
                        written.put(uuid, v);
                        recentlyWritten.add(uuid);
                        completedWrites.incrementAndGet();
                        if (accumulate.get() != null)
                            return;
                    }
                    writersDone.decrement();
                }, accumulate));
                threads.add(writer);
                if (i % 2 == 0)
                {
                    Thread recentReader = new Thread(run(() -> {
                        while (accumulate.get() == null && (writersDone.count() > 0 || !recentlyWritten.isEmpty()))
                        {
                            TimeUUID uuid = recentlyWritten.poll();
                            if (uuid != null)
                            {
                                long expected = written.get(uuid);
                                Long actual = journal.readLast(uuid);
                                assertEquals(expected, (long) actual);
                            }
                            completedReads.incrementAndGet();
                        }
                    }, accumulate));
                    threads.add(recentReader);
                }
                else
                {
                    Thread allReader = new Thread(run(() -> {
                        while (accumulate.get() == null && writersDone.count() > 0)
                        {
                            for (Map.Entry<TimeUUID, Long> e : written.entrySet())
                            {
                                long expected = e.getValue();
                                Long actual = journal.readLast(e.getKey());
                                assertEquals(expected, (long) actual);
                                completedReads.incrementAndGet();
                            }
                        }
                    }, accumulate));
                    threads.add(allReader);
                }
            }
            for (Thread thread : threads)
                thread.start();
            for (Thread thread : threads)
                thread.join();
            if (accumulate.get() != null)
                throw accumulate.get();
        });

        System.out.println(String.format("Finished %d reads, %d writes", completedReads.get(), completedWrites.get()));
        journal.shutdown();
        journal.awaitTermination(30, TimeUnit.SECONDS);
        System.gc();
        System.gc();
    }

    // TODO: wasteful long serializer
    private Runnable run(Runnable r, AtomicReference<Throwable> report)
    {
        return () -> {
            try
            {
                r.run();
            }
            catch (Throwable t)
            {
                report.updateAndGet(new UnaryOperator<Throwable>()
                {
                    public Throwable apply(Throwable throwable)
                    {
                        if (throwable == null)
                            return t;
                        throwable.addSuppressed(t);
                        return t;
                    }
                });
            }
        };
    }

    static class LongSerializer implements ValueSerializer<TimeUUID, Long>
    {
        static final LongSerializer INSTANCE = new LongSerializer();

        public int serializedSize(TimeUUID key, Long value, int userVersion)
        {
            return Long.BYTES;
        }

        public void serialize(TimeUUID key, Long value, DataOutputPlus out, int userVersion) throws IOException
        {
            out.writeLong(value);
        }

        public Long deserialize(TimeUUID key, DataInputPlus in, int userVersion) throws IOException
        {
            return in.readLong();
        }
    }
}
