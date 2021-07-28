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

package org.apache.cassandra.db.compaction;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.util.concurrent.Uninterruptibles;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.Util;
import org.apache.cassandra.config.DatabaseDescriptor;


import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;

public class ValidationExecutorTest
{

    CompactionManager.ValidationExecutor validationExecutor;

    @Before
    public void setup()
    {
        DatabaseDescriptor.clientInitialization();
        // required for static initialization of CompactionManager
        DatabaseDescriptor.setConcurrentCompactors(2);
        DatabaseDescriptor.setConcurrentValidations(2);

        // shutdown the singleton CompactionManager to ensure MBeans are unregistered
        CompactionManager.instance.forceShutdown();
    }

    @After
    public void tearDown()
    {
        if (null != validationExecutor)
            validationExecutor.shutdownNow();
    }

    @Test
    public void testQueueOnValidationSubmission() throws InterruptedException
    {
        CountDownLatch taskBlocked = new CountDownLatch(1);
        AtomicInteger threadsAvailable = new AtomicInteger(DatabaseDescriptor.getConcurrentValidations());
        CountDownLatch taskComplete = new CountDownLatch(5);
        validationExecutor = new CompactionManager.ValidationExecutor();

        ExecutorService testExecutor = Executors.newSingleThreadExecutor();
        for (int i=0; i< 5; i++)
            testExecutor.submit(() -> {
                threadsAvailable.decrementAndGet();
                validationExecutor.submit(new Task(taskBlocked, taskComplete));
            });

        // wait for all tasks to be submitted & check that the excess ones were queued
        while (threadsAvailable.get() > 0)
            TimeUnit.MILLISECONDS.sleep(10);

        // getActiveTaskCount() relies on getActiveCount() which gives an approx number so we poll it
        Util.spinAssertEquals(2, () -> validationExecutor.getActiveTaskCount(), 1);
        assertEquals(3, validationExecutor.getPendingTaskCount());

        taskBlocked.countDown();
        taskComplete.await(10, TimeUnit.SECONDS);
        validationExecutor.shutdownNow();
    }

    @Test
    public void testAdjustPoolSize()
    {
        // adjusting the pool size should dynamically set core and max pool
        // size to DatabaseDescriptor::getConcurrentValidations

        validationExecutor = new CompactionManager.ValidationExecutor();

        int corePoolSize = validationExecutor.getCorePoolSize();
        int maxPoolSize = validationExecutor.getMaximumPoolSize();

        DatabaseDescriptor.setConcurrentValidations(corePoolSize * 2);
        validationExecutor.adjustPoolSize();
        assertThat(validationExecutor.getCorePoolSize()).isEqualTo(corePoolSize * 2);
        assertThat(validationExecutor.getMaximumPoolSize()).isEqualTo(maxPoolSize * 2);
        validationExecutor.shutdownNow();
    }

    private static class Task implements Runnable
    {
        private final CountDownLatch blocked;
        private final CountDownLatch complete;

        Task(CountDownLatch blocked, CountDownLatch complete)
        {
            this.blocked = blocked;
            this.complete = complete;
        }

        public void run()
        {
            Uninterruptibles.awaitUninterruptibly(blocked);
            complete.countDown();
        }
    }
}
