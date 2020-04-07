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

import java.util.LinkedList;
import java.util.List;

import org.junit.Before;
import org.junit.Test;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.PooledByteBufAllocator;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.metrics.ClientRequestSizeMetrics;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Ensures we properly account for bytes read from and to clients
 */
public class ClientRequestSizeMetricsHandlerTest extends CQLTester
{
    private ClientRequestSizeMetricsHandler handler;
    private ByteBufAllocator alloc;
    private ByteBuf buf;
    private List<Object> result;
    private long totalBytesReadStart;
    private long totalBytesWrittenStart;

    private long totalBytesReadHistoCount;
    private long totalBytesWrittenHistoCount;

    @Before
    public void setUp()
    {
        handler = new ClientRequestSizeMetricsHandler();
        alloc = PooledByteBufAllocator.DEFAULT;
        buf = alloc.buffer(1024);
        result = new LinkedList<>();
        buf.writeInt(1);

        totalBytesReadStart = ClientRequestSizeMetrics.totalBytesRead.getCount();
        totalBytesWrittenStart = ClientRequestSizeMetrics.totalBytesWritten.getCount();

        totalBytesReadHistoCount = ClientRequestSizeMetrics.bytesReadPerQueryHistogram.getCount();
        totalBytesWrittenHistoCount = ClientRequestSizeMetrics.bytesWrittenPerQueryHistogram.getCount();
    }

    @Test
    public void testReadAndWriteMetricsAreRecordedDuringNativeRequests() throws Throwable
    {
        executeNet("SELECT * from system.peers");

        assertThat(ClientRequestSizeMetrics.totalBytesRead.getCount()).isGreaterThan(totalBytesReadStart);
        assertThat(ClientRequestSizeMetrics.totalBytesWritten.getCount()).isGreaterThan(totalBytesWrittenStart);
        assertThat(ClientRequestSizeMetrics.bytesReadPerQueryHistogram.getCount()).isGreaterThan(totalBytesReadStart);
        assertThat(ClientRequestSizeMetrics.bytesWrittenPerQueryHistogram.getCount()).isGreaterThan(totalBytesWrittenStart);
    }

    /**
     * Ensures we work with the right metrics within the ClientRequestSizeMetricsHandler
     */
    @Test
    public void testBytesRead()
    {
        int beforeRefCount = buf.refCnt();
        handler.decode(null, buf, result);

        assertThat(ClientRequestSizeMetrics.totalBytesRead.getCount()).isEqualTo(totalBytesReadStart + Integer.BYTES);
        assertThat(ClientRequestSizeMetrics.bytesReadPerQueryHistogram.getCount()).isEqualTo(totalBytesReadHistoCount + 1);

        // make sure we didn't touch the write metrics
        assertThat(ClientRequestSizeMetrics.totalBytesWritten.getCount()).isEqualTo(totalBytesWrittenStart);
        assertThat(ClientRequestSizeMetrics.bytesWrittenPerQueryHistogram.getCount()).isEqualTo(totalBytesWrittenHistoCount);

        // we should have incremented the reference count (netty ByteBuf requirement)
        assertThat(buf.refCnt()).isEqualTo(beforeRefCount + 1);
    }

    @Test
    public void testBytesWritten()
    {
        int beforeRefCount = buf.refCnt();
        handler.encode(null, buf, result);

        assertThat(ClientRequestSizeMetrics.totalBytesWritten.getCount()).isEqualTo(totalBytesWrittenStart + Integer.BYTES);
        assertThat(ClientRequestSizeMetrics.bytesWrittenPerQueryHistogram.getCount()).isEqualTo(totalBytesWrittenHistoCount + 1);

        // make sure we didn't touch the read metrics
        assertThat(ClientRequestSizeMetrics.totalBytesRead.getCount()).isEqualTo(totalBytesReadStart);
        assertThat(ClientRequestSizeMetrics.bytesReadPerQueryHistogram.getCount()).isEqualTo(totalBytesReadHistoCount);

        assertThat(buf.refCnt()).isEqualTo(beforeRefCount + 1);
    }
}
