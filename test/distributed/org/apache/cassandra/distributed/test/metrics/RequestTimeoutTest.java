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

package org.apache.cassandra.distributed.test.metrics;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.impl.CoordinatorHelper;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.exceptions.WriteTimeoutException;
import org.apache.cassandra.service.paxos.Paxos;
import org.apache.cassandra.transport.Dispatcher;

/**
 * Native-transport / request deadline coverage. On newer branches this class also covers
 * verb-drop timeout classification; 4.1 currently hosts the CASSANDRA-20691 CAS queue-deadline case.
 */
public class RequestTimeoutTest extends TestBaseImpl
{
    public static final int COORDINATOR = 1;
    private static Cluster CLUSTER;

    @BeforeClass
    public static void init() throws IOException
    {
        CLUSTER = Cluster.build(1).start();
        init(CLUSTER);
        CLUSTER.schemaChange(withKeyspace("CREATE TABLE %s.tbl (pk int PRIMARY KEY, v int)"));
    }

    @AfterClass
    public static void cleanup()
    {
        if (CLUSTER != null)
            CLUSTER.close();
    }

    /**
     * CASSANDRA-20691: Paxos CAS must use {@link Dispatcher.RequestTime#computeDeadline(long)} so that
     * time spent in the native-transport queue counts against the client deadline. A deadline based only
     * on {@code startedAtNanos} would still have verb headroom here.
     */
    @Test
    public void casInsertTimesOutWhenPastNativeTransportDeadline()
    {
        for (Config.PaxosVariant variant : new Config.PaxosVariant[]{ Config.PaxosVariant.v1, Config.PaxosVariant.v2 })
        {
            withPaxos(variant);
            for (Config.CQLStartTime startTime : Config.CQLStartTime.values())
            {
                Config.PaxosVariant capturedVariant = variant;
                Config.CQLStartTime capturedStartTime = startTime;
                CLUSTER.get(COORDINATOR).runOnInstance(() -> assertCasTimesOutWhenEnqueuedPastNativeTimeout(capturedVariant, capturedStartTime));
            }
        }
    }

    public static void assertCasTimesOutWhenEnqueuedPastNativeTimeout(Config.PaxosVariant variant, Config.CQLStartTime cqlStartTime)
    {
        Paxos.setPaxosVariant(variant);

        long previousNative = DatabaseDescriptor.getNativeTransportTimeout(TimeUnit.MILLISECONDS);
        long previousWrite = DatabaseDescriptor.getWriteRpcTimeout(TimeUnit.MILLISECONDS);
        long previousCas = DatabaseDescriptor.getCasContentionTimeout(TimeUnit.MILLISECONDS);
        Config.CQLStartTime previousStart = DatabaseDescriptor.getCQLStartTime();
        try
        {
            DatabaseDescriptor.setNativeTransportTimeout(12_000, TimeUnit.MILLISECONDS);
            // Generous verb timeouts so only the native-transport/client deadline can expire the request.
            DatabaseDescriptor.setWriteRpcTimeout(60_000);
            DatabaseDescriptor.setCasContentionTimeout(60_000);
            DatabaseDescriptor.setCQLStartTime(cqlStartTime);

            long now = System.nanoTime();
            // Enqueued longer than native_transport_timeout ago; start time is "now".
            Dispatcher.RequestTime requestTime = new Dispatcher.RequestTime(now - TimeUnit.SECONDS.toNanos(30), now);

            try
            {
                CoordinatorHelper.unsafeExecuteInternal("INSERT INTO " + KEYSPACE + ".tbl (pk, v) VALUES (" + now + ", 1) IF NOT EXISTS",
                                                        ConsistencyLevel.SERIAL,
                                                        ConsistencyLevel.ONE,
                                                        requestTime);
                throw new AssertionError("Expected WriteTimeoutException for " + variant + " / " + cqlStartTime);
            }
            catch (WriteTimeoutException expected)
            {
                // expected (CasWriteTimeoutException is a subclass)
            }
        }
        finally
        {
            DatabaseDescriptor.setNativeTransportTimeout(previousNative, TimeUnit.MILLISECONDS);
            DatabaseDescriptor.setWriteRpcTimeout(previousWrite);
            DatabaseDescriptor.setCasContentionTimeout(previousCas);
            DatabaseDescriptor.setCQLStartTime(previousStart);
        }
    }

    private static void withPaxos(Config.PaxosVariant variant)
    {
        CLUSTER.forEach(i -> i.runOnInstance(() -> Paxos.setPaxosVariant(variant)));
    }
}
