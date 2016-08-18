/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */
package org.apache.cassandra.net;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import com.codahale.metrics.Timer;

import org.apache.cassandra.io.util.DataInputPlus.DataInputStreamPlus;
import org.apache.cassandra.io.util.DataOutputStreamPlus;
import org.apache.cassandra.io.util.WrappedDataOutputStreamPlus;
import org.caffinitas.ohc.histo.EstimatedHistogram;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.*;

import org.apache.cassandra.config.DatabaseDescriptor;

public class MessagingServiceTest
{
    @BeforeClass
    public static void initDD()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    private final MessagingService messagingService = MessagingService.test();
    private final static long[] bucketOffsets = new EstimatedHistogram(160).getBucketOffsets();

    @Test
    public void testDroppedMessages()
    {
        MessagingService.Verb verb = MessagingService.Verb.READ;

        for (int i = 1; i <= 5000; i++)
            messagingService.incrementDroppedMessages(verb, i, i % 2 == 0);

        List<String> logs = messagingService.getDroppedMessagesLogs();
        assertEquals(1, logs.size());
        assertEquals("READ messages were dropped in last 5000 ms: 2500 for internal timeout and 2500 for cross node timeout. Mean internal dropped latency: 2730 ms and Mean cross-node dropped latency: 2731 ms", logs.get(0));
        assertEquals(5000, (int)messagingService.getDroppedMessages().get(verb.toString()));

        logs = messagingService.getDroppedMessagesLogs();
        assertEquals(0, logs.size());

        for (int i = 0; i < 2500; i++)
            messagingService.incrementDroppedMessages(verb, i, i % 2 == 0);

        logs = messagingService.getDroppedMessagesLogs();
        assertEquals("READ messages were dropped in last 5000 ms: 1250 for internal timeout and 1250 for cross node timeout. Mean internal dropped latency: 2277 ms and Mean cross-node dropped latency: 2278 ms", logs.get(0));
        assertEquals(7500, (int)messagingService.getDroppedMessages().get(verb.toString()));
    }

    private static void addDCLatency(long sentAt, long now) throws IOException
    {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (DataOutputStreamPlus out = new WrappedDataOutputStreamPlus(baos))
        {
            out.writeInt((int) sentAt);
        }
        DataInputStreamPlus in = new DataInputStreamPlus(new ByteArrayInputStream(baos.toByteArray()));
        MessageIn.readTimestamp(InetAddress.getLocalHost(), in, now);
    }

    @Test
    public void testDCLatency() throws Exception
    {
        int latency = 100;

        ConcurrentHashMap<String, Timer> dcLatency = MessagingService.instance().metrics.dcLatency;
        dcLatency.clear();

        long now = System.currentTimeMillis();
        long sentAt = now - latency;

        assertNull(dcLatency.get("datacenter1"));
        addDCLatency(sentAt, now);
        assertNotNull(dcLatency.get("datacenter1"));
        assertEquals(1, dcLatency.get("datacenter1").getCount());
        long expectedBucket = bucketOffsets[Math.abs(Arrays.binarySearch(bucketOffsets, TimeUnit.MILLISECONDS.toNanos(latency))) - 1];
        assertEquals(expectedBucket, dcLatency.get("datacenter1").getSnapshot().getMax());
    }

    @Test
    public void testNegativeDCLatency() throws Exception
    {
        // if clocks are off should just not track anything
        int latency = -100;

        ConcurrentHashMap<String, Timer> dcLatency = MessagingService.instance().metrics.dcLatency;
        dcLatency.clear();

        long now = System.currentTimeMillis();
        long sentAt = now - latency;

        assertNull(dcLatency.get("datacenter1"));
        addDCLatency(sentAt, now);
        assertNull(dcLatency.get("datacenter1"));
    }
}
