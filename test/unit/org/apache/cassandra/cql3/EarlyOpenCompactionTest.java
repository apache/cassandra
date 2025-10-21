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

package org.apache.cassandra.cql3;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.hamcrest.Matchers;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class EarlyOpenCompactionTest extends CQLTester
{
    private static final int NUM_PARTITIONS = 1000;
    private static final int NUM_ROWS_PER_PARTITION = 100;
    private static final int VALUE_SIZE = 1000; // ~1KB per row
    private static final int VERIFICATION_THREADS = 4;

    private final AtomicBoolean stopVerification = new AtomicBoolean(false);
    private final AtomicInteger verificationErrors = new AtomicInteger(0);
    private final Random random = new Random();
    private ExecutorService executor;

    @After
    public void cleanupAfter() throws Throwable
    {
        stopVerification.set(true);
        if (executor != null)
        {
            executor.shutdownNow();
            executor.awaitTermination(1, TimeUnit.MINUTES);
        }
        DatabaseDescriptor.setSSTablePreemptiveOpenIntervalInMiB(50);
    }

    @Test
    public void testEarlyOpenDuringCompaction() throws Throwable
    {
        // Create a table with a simple schema
        createTable("CREATE TABLE %s (" +
                   "pk int, " +
                   "ck int, " +
                   "data text, " +
                   "PRIMARY KEY (pk, ck)" +
                   ")");

        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        disableCompaction();
        
        // Insert data to create multiple SSTables
        System.out.println("Inserting test data...");
        for (int i = 0; i < NUM_PARTITIONS; i++)
        {
            for (int j = 0; j < NUM_ROWS_PER_PARTITION; j++)
            {
                String value = randomString(VALUE_SIZE);
                execute("INSERT INTO %s (pk, ck, data) VALUES (?, ?, ?)", i, j, value);
            }
            
            // Flush from time to time to get 10 sstables
            if (i > 0 && i % Math.max(1, NUM_PARTITIONS / 10) == 0)
            {
                flush();
            }
        }
        
        // Final flush to ensure all data is written
        flush();
        
        // Verify we have multiple SSTables
        int sstableCount = cfs.getLiveSSTables().size();
        assertTrue("Expected multiple SSTables, got: " + sstableCount, sstableCount > 1);
        
        // Start verification threads
        System.out.println("Starting verification threads...");
        executor = Executors.newFixedThreadPool(VERIFICATION_THREADS);
        List<Future<?>> futures = new ArrayList<>();
        
        for (int i = 0; i < VERIFICATION_THREADS; i++)
        {
            futures.add(executor.submit(new VerificationTask()));
        }
        
        // Wait a bit to ensure verification is running
        Thread.sleep(1000);
        
        // Set early open interval to 1MiB to trigger early open during compaction
        System.out.println("Setting early open interval to 1MiB...");
        DatabaseDescriptor.setSSTablePreemptiveOpenIntervalInMiB(1);
        // Slow down compaction to give the verifier time to fail.
        DatabaseDescriptor.setCompactionThroughputMebibytesPerSec(10);
        
        // Trigger compaction and await its completion
        System.out.println("Starting compaction...");
        cfs.enableAutoCompaction(true);

        // Let verification run for a while during and after compaction
        System.out.println("Verifying data during and after compaction...");
        Thread.sleep(1000);
        
        // Stop verification
        stopVerification.set(true);
        
        // Wait for verification to complete
        for (Future<?> future : futures)
        {
            try
            {
                future.get(10, TimeUnit.SECONDS);
            }
            catch (Exception e)
            {
                System.err.println("Verification task failed: " + e);
                e.printStackTrace();
            }
        }
        
        // Verify no errors occurred during verification
        int errors = verificationErrors.get();
        assertEquals("Found " + errors + " verification errors. Check logs for details.", 0, errors);
        
        System.out.println("Test completed successfully");
    }
    
    private class VerificationTask implements Runnable
    {
        @Override
        public void run()
        {
            try
            {
                Random localRandom = new Random(Thread.currentThread().getId());
                
                while (!stopVerification.get() && !Thread.currentThread().isInterrupted())
                {
                    // Randomly choose between point query and partition range query
                    if (localRandom.nextBoolean())
                    {
                        // Point query
                        int pk = localRandom.nextInt(NUM_PARTITIONS * 110 / 100); // 10% chance outside
                        int ck = localRandom.nextInt(NUM_ROWS_PER_PARTITION * 110 / 100); // 10% chance outside
                        
                        try
                        {
                            Assert.assertEquals(pk < NUM_PARTITIONS && ck < NUM_ROWS_PER_PARTITION ? 1 : 0,
                                                execute("SELECT data FROM %s WHERE pk = ? AND ck = ?", pk, ck).size());
                        }
                        catch (Throwable t)
                        {
                            verificationErrors.incrementAndGet();
                            System.err.println("Point query failed for pk=" + pk + ", ck=" + ck + ": " + t);
                            t.printStackTrace();
                        }
                    }
                    else
                    {
                        // Partition range query
                        int pk = localRandom.nextInt(NUM_PARTITIONS);
                        
                        try
                        {
                            Assert.assertThat(execute("SELECT data FROM %s WHERE token(pk) <= token(?) AND token(pk) >= token(?)", pk, pk).size(),
                                              Matchers.greaterThanOrEqualTo(NUM_ROWS_PER_PARTITION));
                        }
                        catch (Throwable t)
                        {
                            verificationErrors.incrementAndGet();
                            System.err.println("Range query failed for pk in (" + pk + ", " + (pk + 1) + ", " + (pk + 2) + "): " + t);
                            t.printStackTrace();
                        }
                    }
                    
                    // Add a small delay to prevent overwhelming the system
                    Thread.yield();
                }
            }
            catch (Throwable t)
            {
                verificationErrors.incrementAndGet();
                System.err.println("Verification task failed: " + t);
                t.printStackTrace();
            }
        }
    }
    
    private String randomString(int length)
    {
        StringBuilder sb = new StringBuilder(length);
        for (int i = 0; i < length; i++)
        {
            sb.append((char)('a' + random.nextInt(26)));
        }
        return sb.toString();
    }
}
