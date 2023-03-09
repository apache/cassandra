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
package org.apache.cassandra.streaming;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.junit.BeforeClass;
import org.junit.After;
import org.junit.Test;

import org.junit.Assert;
import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.Util;
import org.apache.cassandra.concurrent.ScheduledExecutors;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.streaming.CassandraOutgoingFile;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.TestChannel;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.streaming.async.NettyStreamingChannel;
import org.apache.cassandra.streaming.async.NettyStreamingConnectionFactory;
import org.apache.cassandra.streaming.messages.OutgoingStreamMessage;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.concurrent.Ref;

import static org.apache.cassandra.net.MessagingService.current_version;
import static org.apache.cassandra.utils.TimeUUID.Generator.nextTimeUUID;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class StreamTransferTaskTest
{
    public static final String KEYSPACE1 = "StreamTransferTaskTest";
    public static final String CF_STANDARD = "Standard1";

    static final StreamingChannel.Factory FACTORY = new NettyStreamingConnectionFactory()
    {
        @Override
        public NettyStreamingChannel create(InetSocketAddress to, int messagingVersion, StreamingChannel.Kind kind)
        {
            return new NettyStreamingChannel(new TestChannel(), kind);
        }
    };

    @BeforeClass
    public static void defineSchema() throws ConfigurationException
    {
        SchemaLoader.prepareServer();
        SchemaLoader.createKeyspace(KEYSPACE1,
                                    KeyspaceParams.simple(1),
                                    SchemaLoader.standardCFMD(KEYSPACE1, CF_STANDARD));
    }

    @After
    public void tearDown()
    {
        ColumnFamilyStore cfs = Keyspace.open(KEYSPACE1).getColumnFamilyStore(CF_STANDARD);
        cfs.clearUnsafe();
    }

    @Test
    public void testScheduleTimeout() throws Exception
    {
        InetAddressAndPort peer = FBUtilities.getBroadcastAddressAndPort();
        StreamSession session = new StreamSession(StreamOperation.BOOTSTRAP, peer, FACTORY, null, current_version, false, 0, nextTimeUUID(), PreviewKind.ALL);
        session.init(new StreamResultFuture(nextTimeUUID(), StreamOperation.OTHER, nextTimeUUID(), PreviewKind.NONE));
        ColumnFamilyStore cfs = Keyspace.open(KEYSPACE1).getColumnFamilyStore(CF_STANDARD);

        // create two sstables
        for (int i = 0; i < 2; i++)
        {
            SchemaLoader.insertData(KEYSPACE1, CF_STANDARD, i, 1);
            Util.flush(cfs);
        }

        // create streaming task that streams those two sstables
        session.state(StreamSession.State.PREPARING);
        StreamTransferTask task = new StreamTransferTask(session, cfs.metadata.id);
        for (SSTableReader sstable : cfs.getLiveSSTables())
        {
            List<Range<Token>> ranges = new ArrayList<>();
            ranges.add(new Range<>(sstable.getFirst().getToken(), sstable.getLast().getToken()));
            task.addTransferStream(new CassandraOutgoingFile(StreamOperation.BOOTSTRAP, sstable.selfRef(), sstable.getPositionsForRanges(ranges), ranges, 1));
        }
        assertEquals(14, task.getTotalNumberOfFiles());

        // if file sending completes before timeout then the task should be canceled.
        session.state(StreamSession.State.STREAMING);
        Future f = task.scheduleTimeout(0, 0, TimeUnit.NANOSECONDS);
        f.get();

        // when timeout runs on second file, task should be completed
        f = task.scheduleTimeout(1, 10, TimeUnit.MILLISECONDS);
        task.complete(1);
        try
        {
            f.get();
            Assert.assertTrue(false);
        }
        catch (CancellationException ex)
        {
        }

        assertEquals(StreamSession.State.WAIT_COMPLETE, session.state());

        // when all streaming are done, time out task should not be scheduled.
        assertNull(task.scheduleTimeout(1, 1, TimeUnit.SECONDS));
    }

    @Test
    public void testFailSessionDuringTransferShouldNotReleaseReferences() throws Exception
    {
        InetAddressAndPort peer = FBUtilities.getBroadcastAddressAndPort();
        StreamCoordinator streamCoordinator = new StreamCoordinator(StreamOperation.BOOTSTRAP, 1, new NettyStreamingConnectionFactory(), false, false, null, PreviewKind.NONE);
        StreamResultFuture future = StreamResultFuture.createInitiator(nextTimeUUID(), StreamOperation.OTHER, Collections.<StreamEventHandler>emptyList(), streamCoordinator);
        StreamSession session = new StreamSession(StreamOperation.BOOTSTRAP, peer, FACTORY, null, current_version, false, 0, null, PreviewKind.NONE);
        session.init(future);
        ColumnFamilyStore cfs = Keyspace.open(KEYSPACE1).getColumnFamilyStore(CF_STANDARD);

        // create two sstables
        for (int i = 0; i < 2; i++)
        {
            SchemaLoader.insertData(KEYSPACE1, CF_STANDARD, i, 1);
            Util.flush(cfs);
        }

        // create streaming task that streams those two sstables
        StreamTransferTask task = new StreamTransferTask(session, cfs.metadata.id);
        List<Ref<SSTableReader>> refs = new ArrayList<>(cfs.getLiveSSTables().size());
        for (SSTableReader sstable : cfs.getLiveSSTables())
        {
            List<Range<Token>> ranges = new ArrayList<>();
            ranges.add(new Range<>(sstable.getFirst().getToken(), sstable.getLast().getToken()));
            Ref<SSTableReader> ref = sstable.selfRef();
            refs.add(ref);
            task.addTransferStream(new CassandraOutgoingFile(StreamOperation.BOOTSTRAP, ref, sstable.getPositionsForRanges(ranges), ranges, 1));
        }
        assertEquals(14, task.getTotalNumberOfFiles());

        //add task to stream session, so it is aborted when stream session fails
        session.transfers.put(TableId.generate(), task);

        //make a copy of outgoing file messages, since task is cleared when it's aborted
        Collection<OutgoingStreamMessage> files = new LinkedList<>(task.streams.values());

        //simulate start transfer
        for (OutgoingStreamMessage file : files)
        {
            file.startTransfer();
        }

        //fail stream session mid-transfer
        session.onError(new Exception("Fake exception")).get(5, TimeUnit.SECONDS);

        //make sure reference was not released
        for (Ref<SSTableReader> ref : refs)
        {
            assertEquals(1, ref.globalCount());
        }

        //wait for stream to abort asynchronously
        int tries = 10;
        while (ScheduledExecutors.nonPeriodicTasks.getActiveTaskCount() > 0)
        {
            if(tries < 1)
                throw new RuntimeException("test did not complete in time");
            Thread.sleep(10);
            tries--;
        }

        //simulate finish transfer
        for (OutgoingStreamMessage file : files)
        {
            file.finishTransfer();
        }

        //now reference should be released
        for (Ref<SSTableReader> ref : refs)
        {
            assertEquals(0, ref.globalCount());
        }
    }
}
