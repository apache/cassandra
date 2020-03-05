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

package org.apache.cassandra.transport;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.ConsistencyLevel;

public class IdleDisconnectTest extends CQLTester
{
    private static final long TIMEOUT = 2000L;

    @BeforeClass
    public static void setUp()
    {
        requireNetwork();
        DatabaseDescriptor.setNativeTransportIdleTimeout(TIMEOUT);
    }

    @Test
    public void testIdleDisconnect() throws Throwable
    {
        DatabaseDescriptor.setNativeTransportIdleTimeout(TIMEOUT);
        try (SimpleClient client = new SimpleClient(nativeAddr.getHostAddress(), nativePort))
        {
            long start = System.currentTimeMillis();
            client.connect(false, false);
            Assert.assertTrue(client.channel.isOpen());
            CompletableFuture.runAsync(() -> {
                while (!Thread.currentThread().isInterrupted() && client.channel.isOpen());
            }).get(30, TimeUnit.SECONDS);
            Assert.assertFalse(client.channel.isOpen());
            Assert.assertTrue(System.currentTimeMillis() - start >= TIMEOUT);
        }
    }

    @Test
    public void testIdleDisconnectProlonged() throws Throwable
    {
        long sleepTime = 1000;
        try (SimpleClient client = new SimpleClient(nativeAddr.getHostAddress(), nativePort))
        {
            long start = System.currentTimeMillis();
            client.connect(false, false);
            Assert.assertTrue(client.channel.isOpen());
            Thread.sleep(sleepTime);
            client.execute("SELECT * FROM system.peers", ConsistencyLevel.ONE);
            CompletableFuture.runAsync(() -> {
                while (!Thread.currentThread().isInterrupted() && client.channel.isOpen());
            }).get(30, TimeUnit.SECONDS);
            Assert.assertFalse(client.channel.isOpen());
            Assert.assertTrue(System.currentTimeMillis() - start >= TIMEOUT + sleepTime);
        }
    }
}
