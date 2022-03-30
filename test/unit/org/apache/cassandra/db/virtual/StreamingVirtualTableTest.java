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

import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.streaming.ProgressInfo;
import org.apache.cassandra.streaming.SessionInfo;
import org.apache.cassandra.streaming.StreamCoordinator;
import org.apache.cassandra.streaming.StreamEvent;
import org.apache.cassandra.streaming.StreamManager;
import org.apache.cassandra.streaming.StreamOperation;
import org.apache.cassandra.streaming.StreamResultFuture;
import org.apache.cassandra.streaming.StreamSession;
import org.apache.cassandra.streaming.StreamState;
import org.apache.cassandra.streaming.StreamSummary;
import org.apache.cassandra.streaming.StreamingChannel;
import org.apache.cassandra.streaming.StreamingState;
import org.apache.cassandra.utils.FBUtilities;
import org.assertj.core.util.Throwables;

import static org.apache.cassandra.utils.TimeUUID.Generator.nextTimeUUID;

public class StreamingVirtualTableTest extends CQLTester
{
    private static final String KS_NAME = "vts";
    private static final InetAddressAndPort PEER1 = address(127, 0, 0, 1);
    private static final InetAddressAndPort PEER2 = address(127, 0, 0, 2);
    private static final InetAddressAndPort PEER3 = address(127, 0, 0, 3);
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
        StreamingState state = stream(true);
        assertRows(execute(t("select id, follower, operation, peers, status, progress_percentage, last_updated_at, failure_cause, success_message from %s")),
                   new Object[] { state.id(), true, "Repair", Collections.emptyList(), "init", 0F, new Date(state.lastUpdatedAtMillis()), null, null });

        state.phase.start();
        assertRows(execute(t("select id, follower, operation, peers, status, progress_percentage, last_updated_at, failure_cause, success_message from %s")),
                   new Object[] { state.id(), true, "Repair", Collections.emptyList(), "start", 0F, new Date(state.lastUpdatedAtMillis()), null, null });

        state.handleStreamEvent(new StreamEvent.SessionPreparedEvent(state.id(), new SessionInfo(PEER2, 1, PEER1, Collections.emptyList(), Collections.emptyList(), StreamSession.State.PREPARING)));

        state.onSuccess(new StreamState(state.id(), StreamOperation.REPAIR, ImmutableSet.of(new SessionInfo(PEER2, 1, PEER1, Collections.emptyList(), Collections.emptyList(), StreamSession.State.COMPLETE))));
        assertRows(execute(t("select id, follower, operation, peers, status, progress_percentage, last_updated_at, failure_cause, success_message from %s")),
                   new Object[] { state.id(), true, "Repair", Arrays.asList(address(127, 0, 0, 2).toString()), "success", 100F, new Date(state.lastUpdatedAtMillis()), null, null });
    }

    @Test
    public void progressInitiator() throws Throwable
    {
        progress(false);
    }

    @Test
    public void progressFollower() throws Throwable
    {
        progress(true);
    }

    public void progress(boolean follower) throws Throwable
    {
        StreamingState state = stream(follower);
        StreamResultFuture future = state.future();
        state.phase.start();

        SessionInfo s1 = new SessionInfo(PEER2, 0, FBUtilities.getBroadcastAddressAndPort(), Arrays.asList(streamSummary()), Arrays.asList(streamSummary(), streamSummary()), StreamSession.State.PREPARING);
        SessionInfo s2 = new SessionInfo(PEER3, 0, FBUtilities.getBroadcastAddressAndPort(), Arrays.asList(streamSummary()), Arrays.asList(streamSummary(), streamSummary()), StreamSession.State.PREPARING);

        state.handleStreamEvent(new StreamEvent.SessionPreparedEvent(state.id(), s1));
        state.handleStreamEvent(new StreamEvent.SessionPreparedEvent(state.id(), s2));

        long bytesToReceive = 0, bytesToSend = 0;
        long filesToReceive = 0, filesToSend = 0;
        for (SessionInfo s : Arrays.asList(s1, s2))
        {
            bytesToReceive += s.getTotalSizeToReceive();
            bytesToSend += s.getTotalSizeToSend();
            filesToReceive += s.getTotalFilesToReceive();
            filesToSend += s.getTotalFilesToSend();
        }
        assertRows(execute(t("select id, follower, peers, status, progress_percentage, bytes_to_receive, bytes_received, bytes_to_send, bytes_sent, files_to_receive, files_received, files_to_send, files_sent from %s")),
                   new Object[] { state.id(), follower, Arrays.asList(PEER2.toString(), PEER3.toString()), "start", 0F, bytesToReceive, 0L, bytesToSend, 0L, filesToReceive, 0L, filesToSend, 0L });

        // update progress
        long bytesReceived = 0, bytesSent = 0;
        for (SessionInfo s : Arrays.asList(s1, s2))
        {
            long in = s.getTotalFilesToReceive() - 1;
            long inBytes = s.getTotalSizeToReceive() - in;
            long out = s.getTotalFilesToSend() - 1;
            long outBytes = s.getTotalSizeToSend() - out;
            state.handleStreamEvent(new StreamEvent.ProgressEvent(state.id(), new ProgressInfo((InetAddressAndPort) s.peer, 0, "0", ProgressInfo.Direction.IN, inBytes, inBytes)));
            state.handleStreamEvent(new StreamEvent.ProgressEvent(state.id(), new ProgressInfo((InetAddressAndPort) s.peer, 0, "0", ProgressInfo.Direction.OUT, outBytes, outBytes)));
            bytesReceived += inBytes;
            bytesSent += outBytes;
        }

        assertRows(execute(t("select id, follower, peers, status, bytes_to_receive, bytes_received, bytes_to_send, bytes_sent, files_to_receive, files_received, files_to_send, files_sent from %s")),
                   new Object[] { state.id(), follower, Arrays.asList(PEER2.toString(), PEER3.toString()), "start", bytesToReceive, bytesReceived, bytesToSend, bytesSent, filesToReceive, 2L, filesToSend, 2L });

        // finish
        for (SessionInfo s : Arrays.asList(s1, s2))
        {
            // complete the rest
            for (long i = 1; i < s.getTotalFilesToReceive(); i++)
                state.handleStreamEvent(new StreamEvent.ProgressEvent(state.id(), new ProgressInfo((InetAddressAndPort) s.peer, 0, Long.toString(i), ProgressInfo.Direction.IN, 1, 1)));
            for (long i = 1; i < s.getTotalFilesToSend(); i++)
                state.handleStreamEvent(new StreamEvent.ProgressEvent(state.id(), new ProgressInfo((InetAddressAndPort) s.peer, 0, Long.toString(i), ProgressInfo.Direction.OUT, 1, 1)));
        }

        assertRows(execute(t("select id, follower, peers, status, progress_percentage, bytes_to_receive, bytes_received, bytes_to_send, bytes_sent, files_to_receive, files_received, files_to_send, files_sent from %s")),
                   new Object[] { state.id(), follower, Arrays.asList(PEER2.toString(), PEER3.toString()), "start", 99F, bytesToReceive, bytesToReceive, bytesToSend, bytesToSend, filesToReceive, filesToReceive, filesToSend, filesToSend });

        state.onSuccess(future.getCurrentState());
        assertRows(execute(t("select id, follower, peers, status, progress_percentage, last_updated_at, failure_cause, success_message from %s")),
                   new Object[] { state.id(), follower, Arrays.asList(PEER2.toString(), PEER3.toString()), "success", 100F, new Date(state.lastUpdatedAtMillis()), null, null });
    }

    private static StreamSummary streamSummary()
    {
        int files = ThreadLocalRandom.current().nextInt(2, 10);
        return new StreamSummary(TableId.fromUUID(UUID.randomUUID()), files, files * 1024);
    }

    @Test
    public void failed() throws Throwable
    {
        StreamingState state = stream(true);
        RuntimeException t = new RuntimeException("You failed!");
        state.onFailure(t);
        assertRows(execute(t("select id, follower, peers, status, progress_percentage, last_updated_at, failure_cause, success_message from %s")),
                   new Object[] { state.id(), true, Collections.emptyList(), "failure", 100F, new Date(state.lastUpdatedAtMillis()), Throwables.getStackTrace(t), null });
    }

    private static String t(String query)
    {
        return String.format(query, TABLE_NAME);
    }

    private static StreamingState stream(boolean follower)
    {
        StreamResultFuture future = new StreamResultFuture(nextTimeUUID(), StreamOperation.REPAIR, new StreamCoordinator(StreamOperation.REPAIR, 0, StreamingChannel.Factory.Global.streamingFactory(), follower, false, null, null) {
            // initiator requires active sessions exist, else the future becomes success right away.
            @Override
            public synchronized boolean hasActiveSessions()
            {
                return true;
            }
        });
        StreamingState state = new StreamingState(future);
        if (follower) StreamManager.instance.putFollowerStream(future);
        else StreamManager.instance.putInitiatorStream(future);
        StreamManager.instance.putStreamingState(state);
        future.addEventListener(state);
        return state;
    }

    private static InetAddressAndPort address(int a, int b, int c, int d)
    {
        try
        {
            return InetAddressAndPort.getByAddress(new byte[] {(byte) a, (byte) b, (byte) c, (byte) d});
        }
        catch (UnknownHostException e)
        {
            throw new AssertionError(e);
        }
    }
}