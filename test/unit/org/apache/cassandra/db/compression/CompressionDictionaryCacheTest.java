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

package org.apache.cassandra.db.compression;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.github.luben.zstd.ZstdDictTrainer;
import org.apache.cassandra.config.DatabaseDescriptor;

import static org.apache.cassandra.db.compression.CompressionDictionary.DictId;
import static org.apache.cassandra.db.compression.CompressionDictionary.Kind;
import static org.assertj.core.api.Assertions.assertThat;

public class CompressionDictionaryCacheTest
{
    private static final String TEST_PATTERN = "The quick brown fox jumps over the lazy dog. ";

    private CompressionDictionaryCache cache;
    private ZstdCompressionDictionary testDict1;
    private ZstdCompressionDictionary testDict2;
    private ZstdCompressionDictionary testDict3;

    @BeforeClass
    public static void setUpClass()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    @Before
    public void setUp()
    {
        cache = new CompressionDictionaryCache();
        testDict1 = createTestDictionary(1);
        testDict2 = createTestDictionary(2);
        testDict3 = createTestDictionary(3);
    }

    @After
    public void tearDown()
    {
        if (cache != null)
        {
            cache.close();
        }

        // Close dictionaries if not already closed
        closeQuietly(testDict1);
        closeQuietly(testDict2);
        closeQuietly(testDict3);
    }

    // Basic cache operations tests

    @Test
    public void testGetCurrentInitiallyNull()
    {
        assertThat(cache.getCurrent())
        .as("Current dictionary should be null initially")
        .isNull();
    }

    @Test
    public void testAddAndGet()
    {
        cache.add(testDict1);

        CompressionDictionary retrieved = cache.get(testDict1.dictId());
        assertThat(retrieved)
        .as("Should retrieve the same dictionary instance")
        .isSameAs(testDict1);
    }

    @Test
    public void testGetNonExistentDictionary()
    {
        DictId nonExistentId = new DictId(Kind.ZSTD, 999);
        assertThat(cache.get(nonExistentId))
        .as("Should return null for non-existent dictionary")
        .isNull();
    }

    @Test
    public void testAddMultipleDictionaries()
    {
        cache.add(testDict1);
        cache.add(testDict2);
        cache.add(testDict3);

        assertThat(cache.get(testDict1.dictId())).isSameAs(testDict1);
        assertThat(cache.get(testDict2.dictId())).isSameAs(testDict2);
        assertThat(cache.get(testDict3.dictId())).isSameAs(testDict3);
    }

    @Test
    public void testSetCurrentWithNewerDictionary()
    {
        cache.add(testDict1);
        cache.add(testDict2);

        assertThat(cache.getCurrent())
        .as("Should update to newer dictionary")
        .isSameAs(testDict2);

        // Both should be in cache
        assertThat(cache.get(testDict1.dictId())).isSameAs(testDict1);
        assertThat(cache.get(testDict2.dictId())).isSameAs(testDict2);
    }

    @Test
    public void testSetCurrentWithOlderDictionary()
    {
        cache.add(testDict2);
        cache.add(testDict1); // older dictionary

        assertThat(cache.getCurrent())
        .as("Should keep newer dictionary as current")
        .isSameAs(testDict2);

        // Both should be in cache
        assertThat(cache.get(testDict1.dictId())).isSameAs(testDict1);
        assertThat(cache.get(testDict2.dictId())).isSameAs(testDict2);
    }

    @Test
    public void testSetCurrentWithSameIdDictionary()
    {
        ZstdCompressionDictionary sameDictCopy = createTestDictionary(2);

        cache.add(testDict2);
        cache.add(sameDictCopy);

        // Should not update since ID is the same (not newer)
        assertThat(cache.getCurrent())
        .as("Should keep original dictionary as current")
        .isSameAs(testDict2);

        sameDictCopy.selfRef().close();
    }

    @Test
    public void testSetCurrentWithNull()
    {
        cache.add(testDict1);
        cache.add(null);

        // Should not change current dictionary
        assertThat(cache.getCurrent())
        .as("Should keep existing dictionary as current")
        .isSameAs(testDict1);
    }

    @Test
    public void testCacheClose()
    {
        cache.add(testDict1);
        cache.add(testDict2);

        assertThat(cache.getCurrent())
        .as("Current should not be null before close")
        .isNotNull();
        assertThat(cache.get(testDict1.dictId()))
        .as("Cache should contain dict1 before close")
        .isNotNull();

        cache.close();

        assertThat(cache.getCurrent())
        .as("Current should be null after close")
        .isNull();
        assertThat(cache.get(testDict1.dictId()))
        .as("Cache should not contain dict1 after close")
        .isNull();
        assertThat(cache.get(testDict2.dictId()))
        .as("Cache should not contain dict2 after close")
        .isNull();
    }

    @Test
    public void testCloseIdempotent()
    {
        cache.add(testDict1);

        // Close multiple times should not cause issues
        cache.close();
        cache.close();
        cache.close();

        assertThat(cache.getCurrent())
        .as("Current should remain null")
        .isNull();
        assertThat(cache.get(testDict1.dictId()))
        .as("Cache should remain empty")
        .isNull();
    }

    @Test
    public void testConcurrentAccess() throws InterruptedException
    {
        int threadCount = 10;
        int operationsPerThread = 100;
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(threadCount);
        AtomicInteger successCount = new AtomicInteger(0);
        AtomicReference<Exception> errorRef = new AtomicReference<>();

        // Pre-populate cache
        cache.add(testDict1);
        cache.add(testDict2);

        for (int i = 0; i < threadCount; i++)
        {
            executor.submit(() -> {
                try
                {
                    startLatch.await();

                    for (int j = 0; j < operationsPerThread; j++)
                    {
                        // Mix of read operations
                        CompressionDictionary current = cache.getCurrent();
                        cache.get(testDict1.dictId());
                        cache.get(testDict2.dictId());

                        // Verify consistency
                        if (current != null && current.dictId().equals(testDict2.dictId()))
                        {
                            successCount.incrementAndGet();
                        }
                    }
                }
                catch (Exception e)
                {
                    errorRef.set(e);
                }
                finally
                {
                    doneLatch.countDown();
                }
            });
        }

        startLatch.countDown(); // Start all threads
        assertThat(doneLatch.await(10, TimeUnit.SECONDS))
        .as("Threads should complete within timeout")
        .isTrue();

        executor.shutdown();

        assertThat(errorRef.get())
        .as("No errors should occur during concurrent access")
        .isNull();
        assertThat(successCount.get())
        .as("Should have successful read operations")
        .isGreaterThan(0);
    }

    @Test
    public void testConcurrentSetCurrent() throws InterruptedException
    {
        int threadCount = 5;
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(threadCount);

        // Create multiple dictionaries with different IDs
        ZstdCompressionDictionary[] dicts = new ZstdCompressionDictionary[threadCount];
        for (int i = 0; i < threadCount; i++)
        {
            dicts[i] = createTestDictionary(100 + i); // High IDs to ensure newer
        }

        for (int i = 0; i < threadCount; i++)
        {
            ZstdCompressionDictionary dict = dicts[i];
            executor.submit(() -> {
                try
                {
                    startLatch.await();
                    cache.add(dict);
                }
                catch (Exception e)
                {
                    // Ignore - testing thread safety
                }
                finally
                {
                    doneLatch.countDown();
                }
            });
        }

        startLatch.countDown();
        assertThat(doneLatch.await(5, TimeUnit.SECONDS))
        .as("Threads should complete within timeout")
        .isTrue();

        executor.shutdown();

        // Verify that a current dictionary was set and it's one of our test dictionaries
        CompressionDictionary current = cache.getCurrent();
        assertThat(current)
        .as("A current dictionary should be set")
        .isNotNull();
        assertThat(current.dictId().id)
        .as("Current dictionary should be one of the test dictionaries")
        .isBetween(100L, 100L + threadCount);

        // Clean up
        for (ZstdCompressionDictionary dict : dicts)
        {
            closeQuietly(dict);
        }
    }

    @Test
    public void testGetCurrentRefreshesCacheEntry() throws InterruptedException
    {
        int expireSeconds = 1;
        try (CompressionDictionaryCache shortLivedCache = new CompressionDictionaryCache(10, expireSeconds))
        {
            shortLivedCache.add(testDict1);
            assertThat(shortLivedCache.getCurrent())
            .as("Current dictionary should be set")
            .isSameAs(testDict1);
            assertThat(shortLivedCache.get(testDict1.dictId()))
            .as("Dictionary should be in cache")
            .isNotNull();

            // Access getCurrent() repeatedly for slightly longer than the expiration time
            int iterations = expireSeconds * 2 + 1;
            for (int i = 0; i < iterations; i++)
            {
                Thread.sleep(900); // Sleep a bit less than expireSeconds between accesses

                CompressionDictionary current = shortLivedCache.getCurrent();
                assertThat(current)
                .as("Current dictionary should remain accessible after %d seconds", i + 1)
                .isSameAs(testDict1);

                assertThat(shortLivedCache.get(testDict1.dictId()))
                .as("Dictionary should still be in cache after %d seconds", i + 1)
                .isNotNull();
            }

            assertThat(shortLivedCache.getCurrent())
            .as("Current dictionary should remain accessible due to getCurrent() refreshing the cache")
            .isSameAs(testDict1);
        }
    }

    @Test
    public void testGetCurrentReturnsNullAfterExpiration() throws InterruptedException
    {
        int expireSeconds = 1;
        try (CompressionDictionaryCache shortLivedCache = new CompressionDictionaryCache(10, expireSeconds))
        {
            shortLivedCache.add(testDict1);
            assertThat(shortLivedCache.getCurrent())
            .as("Current dictionary should be set")
            .isSameAs(testDict1);

            // Wait for expiration without accessing getCurrent()
            Thread.sleep((expireSeconds + 1) * 1000);

            // Current should now return null since the entry expired and we only store the DictId
            assertThat(shortLivedCache.getCurrent())
            .as("Current dictionary should be null after expiration when not accessed")
            .isNull();
        }
    }

    private static ZstdCompressionDictionary createTestDictionary(long id)
    {
        try
        {
            // Create simple dictionary
            ZstdDictTrainer trainer = new ZstdDictTrainer(10 * 1024, 1024, 3);

            // Add samples
            byte[] sample = TEST_PATTERN.getBytes();
            for (int i = 0; i < 100; i++)
            {
                trainer.addSample(sample);
            }

            byte[] dictBytes = trainer.trainSamples();
            DictId dictId = new DictId(Kind.ZSTD, id);

            return new ZstdCompressionDictionary(dictId, dictBytes);
        }
        catch (Exception e)
        {
            throw new RuntimeException("Failed to create test dictionary", e);
        }
    }

    private static void closeQuietly(ZstdCompressionDictionary resource)
    {
        if (resource != null)
        {
            try
            {
                resource.selfRef().release();
            }
            catch (Exception e)
            {
                // Ignore
            }
        }
    }
}
