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

package org.apache.cassandra.service;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Assert;
import org.junit.Test;

import org.apache.cassandra.exceptions.ReadTimeoutException;
import org.apache.cassandra.net.AsyncOneResponse;
import org.apache.cassandra.service.DataResolver.RepairMergeListener;

import static java.lang.String.format;
import static org.apache.cassandra.db.ConsistencyLevel.QUORUM;
import static org.apache.cassandra.locator.InetAddressAndPort.getByAddress;
import static org.junit.Assert.assertEquals;

public class RepairMergeListenerTest
{
    @Test
    public void testRepairMergeListenerTimeout() throws Exception
    {
        InetAddress[] sources = new InetAddress[]{
        getByAddress(new byte[]{ (byte) 172, 19, 0, 5 }).address,
        getByAddress(new byte[]{ (byte) 172, 19, 0, 6 }).address,
        getByAddress(new byte[]{ (byte) 172, 19, 0, 7 }).address
        };

        List<AsyncOneResponse<?>> repairResults = new ArrayList<AsyncOneResponse<?>>()
        {{
            add(new AsyncOneResponse<>());
            add(new AsyncOneResponse<>());
            add(new AsyncOneResponse<>());
        }};

        DataResolver dataResolver = new DataResolver(null, null, QUORUM, 3, System.nanoTime(), false);

        final AtomicInteger runRepairs = new AtomicInteger(0);

        RepairMergeListener listener = dataResolver.new RepairMergeListener(sources, repairResults, QUORUM)
        {
            void waitForRepairResults() throws TimeoutException
            {
                finishedRepairs.add(1);
                runRepairs.addAndGet(finishedRepairs.intValue());
                throw new TimeoutException();
            }
        };

        try
        {
            listener.close();
            Assert.fail("It should throw ReadTimeoutException.");
        }
        catch (ReadTimeoutException ex)
        {
            assertEquals(QUORUM, ex.consistency);
            assertEquals(repairResults.size(), ex.blockFor);
            assertEquals(1, runRepairs.get());
            assertEquals(runRepairs.get(), ex.received);
            assertEquals(format("Operation timed out - received only %s responses.", 1), ex.getMessage());
        }
    }
}
