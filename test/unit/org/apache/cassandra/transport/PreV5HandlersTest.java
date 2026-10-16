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

import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Session;
import io.netty.util.ResourceLeakDetector;
import org.apache.cassandra.cql3.CQLTester;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;

public class PreV5HandlersTest extends CQLTester
{
    private static final ProtocolVersion VERSION = ProtocolVersion.V4;
    private ResourceLeakDetector.Level originalLeakLevel;
    private ListAppender<ILoggingEvent> leakReports;
    private Logger leakLogger;

    @Before
    public void setUp()
    {
        originalLeakLevel = ResourceLeakDetector.getLevel();
        ResourceLeakDetector.setLevel(ResourceLeakDetector.Level.PARANOID);

        leakLogger = (Logger) LoggerFactory.getLogger(ResourceLeakDetector.class);
        leakReports = new ListAppender<>();
        leakReports.start();
        leakLogger.addAppender(leakReports);

        requireNetwork();
    }

    @After
    public void tearDown()
    {
        ResourceLeakDetector.setLevel(originalLeakLevel);
        leakLogger.detachAppender(leakReports);
    }

    /**
     * Regression test for CASSANDRA-21766 to ensure that pre-V5 clients can disconnect while requests are in flight
     * without causing resource leaks. This test will create a pre-V5 connection, send multiple requests, and then
     * close the connection while requests are still being processed. The test will monitor for any resource leaks
     * that may occur due to the disconnection.
     */
    @Test
    public void testPreV5ClientDisconnectWithRequestsInFlight() throws Throwable
    {
        // ~8 x 16KiB rows per response, so each response body is a "normal" (> 32KiB) pooled allocation, which
        // Netty never puts in a thread-local cache
        createTable("CREATE TABLE %s (k int, c int, v blob, PRIMARY KEY (k, c))");
        ByteBuffer blob = ByteBuffer.wrap(new byte[16 * 1024]);
        for (int c = 0; c < 8; c++)
            execute("INSERT INTO %s (k, c, v) VALUES (0, ?, ?)", c, blob);
        String select = String.format("SELECT * FROM %s.%s WHERE k = 0", KEYSPACE, currentTable());

        Cluster cluster = sessionNet(VERSION).getCluster();
        int cycles = 50;
        int queriesPerCycle = 200;
        // Run multiple cycles of queries and enqueue bunch of queries per cycle, then close the session while
        // queries are still in flight to simulate a client disconnecting with requests in flight.
        for (int i = 0; i < cycles; i++)
        {
            Session session = cluster.connect();
            List<ResultSetFuture> inFlight = new ArrayList<>(queriesPerCycle);
            for (int j = 0; j < queriesPerCycle; j++)
                inFlight.add(session.executeAsync(select));
            session.closeAsync().force().get(10, TimeUnit.SECONDS);
            inFlight.forEach(f -> f.cancel(true));
        }

        // let the server finish the requests queued by the disconnected sessions
        Thread.sleep(Duration.ofSeconds(10).toMillis());

        // Netty reports a leak on a later allocation, once the leaked ByteBuf wrapper has been garbage collected
        for (int i = 0; i < 20 && leakReports.list.isEmpty(); i++)
        {
            System.gc();
            Thread.sleep(100);
            for (int j = 0; j < 100; j++)
                CBUtil.allocator.directBuffer(64 * 1024).release();
        }

        List<String> leaks = new ArrayList<>();
        leakReports.list.stream()
                .filter(l -> l.getFormattedMessage().startsWith("LEAK:"))
                .forEach(l -> leaks.add(l.getFormattedMessage()));
        if (!leaks.isEmpty())
            logger.error("First of {} leak reports:\n{}", leaks.size(), leaks.get(0));
        assertEquals("Netty LEAK reports", 0, leaks.size());
    }
}
