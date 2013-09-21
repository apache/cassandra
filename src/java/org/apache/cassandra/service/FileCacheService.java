/**
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
package org.apache.cassandra.service;

import java.util.Queue;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.io.util.RandomAccessReader;
import org.apache.cassandra.metrics.FileCacheMetrics;

public class FileCacheService
{
    private static final Logger logger = LoggerFactory.getLogger(FileCacheService.class);

    private static final long MEMORY_USAGE_THRESHOLD = DatabaseDescriptor.getFileCacheSizeInMB() * 1024L * 1024L;
    private static final int AFTER_ACCESS_EXPIRATION = 512; // in millis

    public static FileCacheService instance = new FileCacheService();

    private final Cache<String, Queue<RandomAccessReader>> cache;
    private final FileCacheMetrics metrics = new FileCacheMetrics();
    public final Callable<Queue<RandomAccessReader>> cacheForPathCreator = new Callable<Queue<RandomAccessReader>>()
    {
        @Override
        public Queue<RandomAccessReader> call() throws Exception
        {
            return new ConcurrentLinkedQueue<RandomAccessReader>();
        }
    };

    protected FileCacheService()
    {
        cache = CacheBuilder.<String, Queue<RandomAccessReader>>newBuilder()
                            .expireAfterAccess(AFTER_ACCESS_EXPIRATION, TimeUnit.MILLISECONDS)
                            .concurrencyLevel(DatabaseDescriptor.getConcurrentReaders())
                            .removalListener(new RemovalListener<String, Queue<RandomAccessReader>>()
                            {
                                @Override
                                public void onRemoval(RemovalNotification<String, Queue<RandomAccessReader>> notification)
                                {
                                    Queue<RandomAccessReader> cachedInstances = notification.getValue();

                                    if (cachedInstances == null)
                                        return;

                                    for (RandomAccessReader reader : cachedInstances)
                                        reader.deallocate();
                                }
                            })
                            .build();
    }

    public RandomAccessReader get(String path)
    {
        metrics.requests.mark();

        Queue<RandomAccessReader> instances = getCacheFor(path);

        if (instances == null)
            return null;

        RandomAccessReader result = instances.poll();

        if (result != null)
            metrics.hits.mark();

        return result;
    }

    private Queue<RandomAccessReader> getCacheFor(String path)
    {
        try
        {
            return cache.get(path, cacheForPathCreator);
        }
        catch (ExecutionException e)
        {
            // if something bad happened, let's just carry on and return null
            // as dysfunctional queue should not interrupt normal operation
            logger.debug("Exception fetching cache", e);
        }

        return null;
    }

    public void put(RandomAccessReader instance)
    {
        // This wouldn't be precise sometimes when CRAR is used because
        // there is a way for users to dynamically change the size of the buffer,
        // but we don't expect that to happen frequently in production.
        // Doing accounting this way also allows us to avoid atomic CAS operation on read path.
        long memoryUsage = (cache.size() + 1) * instance.getBufferSize();

        if (memoryUsage >= MEMORY_USAGE_THRESHOLD)
            instance.deallocate();
        else
            getCacheFor(instance.getPath()).add(instance);
    }

    public void invalidate(String path)
    {
        cache.invalidate(path);
    }

    public long sizeInBytes()
    {
        long n = 0;
        for (Queue<RandomAccessReader> queue : cache.asMap().values())
            for (RandomAccessReader reader : queue)
                n += reader.getBufferSize();
        return n;
    }
}
