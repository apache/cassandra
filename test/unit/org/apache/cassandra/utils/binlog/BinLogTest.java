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

package org.apache.cassandra.utils.binlog;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.ChronicleQueueBuilder;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.queue.RollCycles;
import net.openhft.chronicle.wire.WireOut;
import org.apache.cassandra.Util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class BinLogTest
{
    public static Path tempDir() throws Exception
    {
        File f = File.createTempFile("foo", "bar");
        f.delete();
        f.mkdir();
        return Paths.get(f.getPath());
    }

    private static final String testString = "ry@nlikestheyankees";
    private static final String testString2 = testString + "1";

    private BinLog binLog;
    private Path path;

    @Before
    public void setUp() throws Exception
    {
        path = tempDir();
        binLog = new BinLog(path, RollCycles.TEST_SECONDLY, 10, 1024 * 1024 * 128);
        binLog.start();
    }

    @After
    public void tearDown() throws Exception
    {
        if (binLog != null)
        {
            binLog.stop();
        }
        for (File f : path.toFile().listFiles())
        {
            f.delete();
        }
    }

    @Test(expected = NullPointerException.class)
    public void testConstructorNullPath() throws Exception
    {
        new BinLog(null, RollCycles.TEST_SECONDLY, 1, 1);
    }

    @Test(expected = NullPointerException.class)
    public void testConstructorNullRollCycle() throws Exception
    {
        new BinLog(tempDir(), null, 1, 1);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testConstructorZeroWeight() throws Exception
    {
        new BinLog(tempDir(), RollCycles.TEST_SECONDLY, 0, 1);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testConstructorLogSize() throws Exception
    {
        new BinLog(tempDir(), RollCycles.TEST_SECONDLY, 1, 0);
    }

    /**
     * Check that we can start and stop the bin log and that it releases resources held by any subsequent appended
     * records
     */
    @Test
    public void testBinLogStartStop() throws Exception
    {
        Semaphore blockBinLog = new Semaphore(1);
        AtomicInteger releaseCount = new AtomicInteger();
        binLog.put(new BinLog.ReleaseableWriteMarshallable()
        {
            protected void release()
            {
                releaseCount.incrementAndGet();
            }

            public void writeMarshallable(WireOut wire)
            {
                try
                {
                    blockBinLog.acquire();
                }
                catch (InterruptedException e)
                {
                    throw new RuntimeException(e);
                }
            }
        });
        binLog.put(new BinLog.ReleaseableWriteMarshallable()
        {

            public void writeMarshallable(WireOut wire)
            {

            }

            protected void release()
            {
                releaseCount.incrementAndGet();
            }
        });
        Thread.sleep(1000);
        assertEquals(2, releaseCount.get());
        Thread t = new Thread(() -> {
            try
            {
                binLog.stop();
            }
            catch (InterruptedException e)
            {
                throw new AssertionError(e);
            }
        });
        t.start();
        t.join(60 * 1000);
        assertEquals(t.getState(), Thread.State.TERMINATED);

        Util.spinAssertEquals(2, releaseCount::get, 60);
        Util.spinAssertEquals(Thread.State.TERMINATED, binLog.binLogThread::getState, 60);
     }

    /**
     * Check that the finalizer releases any stragglers in the queue
     */
    @Test
    public void testBinLogFinalizer() throws Exception
    {
        binLog.stop();
        Semaphore released = new Semaphore(0);
        binLog.sampleQueue.put(new BinLog.ReleaseableWriteMarshallable()
        {
            protected void release()
            {
                released.release();
            }

            public void writeMarshallable(WireOut wire)
            {

            }
        });
        binLog = null;

        for (int ii = 0; ii < 30; ii++)
        {
            System.gc();
            System.runFinalization();
            Thread.sleep(100);
            if (released.tryAcquire())
                return;
        }
        fail("Finalizer never released resources");
    }

    /**
     * Test that put blocks and unblocks and creates records
     */
    @Test
    public void testPut() throws Exception
    {
        binLog.put(record(testString));
        binLog.put(record(testString2));

        Util.spinAssertEquals(2, () -> readBinLogRecords(path).size(), 60);
        List<String> records = readBinLogRecords(path);
        assertEquals(testString, records.get(0));
        assertEquals(testString2, records.get(1));


        //Prevent the bin log thread from making progress
        Semaphore blockBinLog = new Semaphore(0);
        //Get notified when the bin log thread has blocked and definitely won't batch drain tasks
        Semaphore binLogBlocked = new Semaphore(0);
        try
        {
            binLog.put(new BinLog.ReleaseableWriteMarshallable()
            {
                protected void release()
                {
                }

                public void writeMarshallable(WireOut wire)
                {
                    //Notify the bing log thread is about to block
                    binLogBlocked.release();
                    try
                    {
                        //Block the bin log thread so it doesn't process more tasks
                        blockBinLog.acquire();
                    }
                    catch (InterruptedException e)
                    {
                        throw new RuntimeException(e);
                    }
                }
            });

            //Wait for the bin log thread to block so it doesn't batch drain
            Util.spinAssertEquals(true, binLogBlocked::tryAcquire, 60);

            //Now fill the queue up to capacity and it shouldn't block
            for (int ii = 0; ii < 10; ii++)
            {
                binLog.put(record(testString));
            }

            //Thread to block on the full queue
            Thread t = new Thread(() ->
                                  {
                                      try
                                      {
                                          binLog.put(record(testString));
                                          //Should be able to do it again after unblocking
                                          binLog.put(record(testString));
                                      }
                                      catch (InterruptedException e)
                                      {
                                          throw new AssertionError(e);
                                      }
                                  });
            t.start();
            Thread.sleep(500);
            //If the thread is not terminated then it is probably blocked on the queue
            assertTrue(t.getState() != Thread.State.TERMINATED);
        }
        finally
        {
            blockBinLog.release();
        }

        //Expect all the records to eventually be there including one from the blocked thread
        Util.spinAssertEquals(15, () -> readBinLogRecords(path).size(), 60);
    }

    @Test
    public void testOffer() throws Exception
    {
        assertTrue(binLog.offer(record(testString)));
        assertTrue(binLog.offer(record(testString2)));

        Util.spinAssertEquals(2, () -> readBinLogRecords(path).size(), 60);
        List<String> records = readBinLogRecords(path);
        assertEquals(testString, records.get(0));
        assertEquals(testString2, records.get(1));

        //Prevent the bin log thread from making progress
        Semaphore blockBinLog = new Semaphore(0);
        //Get notified when the bin log thread has blocked and definitely won't batch drain tasks
        Semaphore binLogBlocked = new Semaphore(0);
        try
        {
            assertTrue(binLog.offer(new BinLog.ReleaseableWriteMarshallable()
            {
                protected void release()
                {
                }

                public void writeMarshallable(WireOut wire)
                {
                    //Notify the bing log thread is about to block
                    binLogBlocked.release();
                    try
                    {
                        //Block the bin log thread so it doesn't process more tasks
                        blockBinLog.acquire();
                    }
                    catch (InterruptedException e)
                    {
                        throw new RuntimeException(e);
                    }
                }
            }));

            //Wait for the bin log thread to block so it doesn't batch drain
            Util.spinAssertEquals(true, binLogBlocked::tryAcquire, 60);

            //Now fill the queue up to capacity and it should always accept
            for (int ii = 0; ii < 10; ii++)
            {
                assertTrue(binLog.offer(record(testString)));
            }

            //it shoudl reject this record since it is full
            assertFalse(binLog.offer(record(testString)));
        }
        finally
        {
            blockBinLog.release();
        }
        Util.spinAssertEquals(13, () -> readBinLogRecords(path).size(), 60);
        assertTrue(binLog.offer(record(testString)));
        Util.spinAssertEquals(14, () -> readBinLogRecords(path).size(), 60);
    }

    /**
     * Set a very small segment size so on rolling the segments are always deleted
     */
    @Test
    public void testCleanupOnOversize() throws Exception
    {
        tearDown();
        binLog = new BinLog(path, RollCycles.TEST_SECONDLY, 10000, 1);
        binLog.start();
        for (int ii = 0; ii < 5; ii++)
        {
            binLog.put(record(String.valueOf(ii)));
            Thread.sleep(1001);
        }
        List<String> records = readBinLogRecords(path);
        System.out.println("Records found are " + records);
        assertTrue(records.size() < 5);
    }

    @Test(expected = IllegalStateException.class)
    public void testNoReuse() throws Exception
    {
        binLog.stop();
        binLog.start();
    }

    @Test
    public void testOfferAfterStop() throws Exception
    {
        binLog.stop();
        assertFalse(binLog.offer(record(testString)));
    }

    @Test
    public void testPutAfterStop() throws Exception
    {
        binLog.stop();
        binLog.put(record(testString));
        assertEquals(null, binLog.sampleQueue.poll());
    }

    /**
     * Test for a bug where files were deleted but the space was not reclaimed when tracking so
     * all log segemnts were incorrectly deleted when rolled.
     */
    @Test
    public void testTrucationReleasesLogSpace() throws Exception
    {
        StringBuilder sb = new StringBuilder();
        for (int ii = 0; ii < 1024 * 1024 * 2; ii++)
        {
            sb.append('a');
        }

        String queryString = sb.toString();

        //This should fill up the log so when it rolls in the future it will always delete the rolled segment;
        for (int ii = 0; ii < 129; ii++)
        {
            binLog.put(record(queryString));
        }

        for (int ii = 0; ii < 2; ii++)
        {
            Thread.sleep(2000);
            binLog.put(record(queryString));
        }

        Util.spinAssertEquals(2, () -> readBinLogRecords(path).size(), 60);
    }

    static BinLog.ReleaseableWriteMarshallable record(String text)
    {
        return new BinLog.ReleaseableWriteMarshallable()
        {
            protected void release()
            {
                //Do nothing
            }

            public void writeMarshallable(WireOut wire)
            {
                wire.write("text").text(text);
            }
        };
    }

    List<String> readBinLogRecords(Path path)
    {
        List<String> records = new ArrayList<String>();
        try (ChronicleQueue queue = ChronicleQueueBuilder.single(path.toFile()).rollCycle(RollCycles.TEST_SECONDLY).build())
        {
            ExcerptTailer tailer = queue.createTailer();
            while (true)
            {
                if (!tailer.readDocument(wire ->
                                         {
                                             records.add(wire.read("text").text());
                                         }))
                {
                    return records;
                }
            }
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }
    }
}
