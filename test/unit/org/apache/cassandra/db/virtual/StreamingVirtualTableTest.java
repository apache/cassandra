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
package org.apache.cassandra.db.virtual;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.UUID;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.streaming.SessionInfo;
import org.apache.cassandra.streaming.StreamManager;
import org.apache.cassandra.streaming.StreamOperation;
import org.apache.cassandra.streaming.StreamResultFuture;
import org.apache.cassandra.streaming.StreamSession;
import org.apache.cassandra.streaming.StreamState;
import org.apache.cassandra.streaming.StreamingState;
import org.assertj.core.util.Throwables;

public class StreamingVirtualTableTest extends CQLTester
{
    private static final String KS_NAME = "vts";
    private static String TABLE_NAME;

    @BeforeClass
    public static void setup()
    {
        CQLTester.setUpClass();
        StreamingVirtualTable table = new StreamingVirtualTable(KS_NAME);
        TABLE_NAME = table.toString();
        VirtualKeyspaceRegistry.instance.register(new VirtualKeyspace(KS_NAME, ImmutableList.of(table)));
    }

    @Before
    public void clearState()
    {
        StreamManager.instance.clearStates();
    }

    @Test
    public void empty() throws Throwable
    {
        assertEmpty(execute(t("select * from %s")));
    }

    @Test
    public void single() throws Throwable
    {
        StreamingState state = stream();
        assertRows(execute(t("select id, follower, operation, peers, status, progress_percentage, last_updated_at, failure_cause, success_message from %s")),
                   new Object[] { state.getId(), true, "Repair", Collections.emptyList(), "init", 0F, new Date(state.getLastUpdatedAtMillis()), null, null });

        state.phase.start();
        assertRows(execute(t("select id, follower, operation, peers, status, progress_percentage, last_updated_at, failure_cause, success_message from %s")),
                   new Object[] {state.getId(), true, "Repair", Collections.emptyList(), "start", 0F, new Date(state.getLastUpdatedAtMillis()), null, null });

        state.onSuccess(new StreamState(state.getId(), StreamOperation.REPAIR, ImmutableSet.of(new SessionInfo(address(127, 0, 0, 2), 1, address(127, 0, 0, 1), Collections.emptyList(), Collections.emptyList(), StreamSession.State.COMPLETE))));
        assertRows(execute(t("select id, follower, operation, peers, status, progress_percentage, last_updated_at, failure_cause, success_message from %s")),
                   new Object[] { state.getId(), true, "Repair", Arrays.asList(address(127, 0, 0, 2).toString()), "success", 100F, new Date(state.getLastUpdatedAtMillis()), null, null });
    }

    @Test
    public void failed() throws Throwable
    {
        StreamingState state = stream();
        RuntimeException t = new RuntimeException("You failed!");
        state.onFailure(t);
        assertRows(execute(t("select id, follower, peers, status, progress_percentage, last_updated_at, failure_cause, success_message from %s")),
                   new Object[] { state.getId(), true, Collections.emptyList(), "failure", 100F, new Date(state.getLastUpdatedAtMillis()), Throwables.getStackTrace(t), null });
    }

    private static String t(String query)
    {
        return String.format(query, TABLE_NAME);
    }

    private static StreamingState stream()
    {
        StreamResultFuture future = new StreamResultFuture(UUID.randomUUID(), StreamOperation.REPAIR, null, null);
        StreamingState state = new StreamingState(future);
        StreamManager.instance.putStreamingState(state);
        future.addEventListener(state);
        return state;
    }

    private static InetSocketAddress address(int a, int b, int c, int d)
    {
        try
        {
            return new InetSocketAddress(InetAddress.getByAddress(new byte[] {(byte) a, (byte) b, (byte) c, (byte) d}), 42);
        }
        catch (UnknownHostException e)
        {
            throw new AssertionError(e);
        }
    }
}