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

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.junit.BeforeClass;
import org.junit.Test;

import junit.framework.Assert;
import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.config.KSMetaData;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.locator.SimpleStrategy;
import org.apache.cassandra.utils.FBUtilities;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class StreamTransferTaskTest
{
    public static final String KEYSPACE1 = "StreamTransferTaskTest";
    public static final String CF_STANDARD = "Standard1";

    @BeforeClass
    public static void defineSchema() throws ConfigurationException
    {
        SchemaLoader.prepareServer();
        SchemaLoader.createKeyspace(KEYSPACE1,
                                    SimpleStrategy.class,
                                    KSMetaData.optsWithRF(1),
                                    SchemaLoader.standardCFMD(KEYSPACE1, CF_STANDARD));
    }

    @Test
    public void testScheduleTimeout() throws Exception
    {
        String ks = KEYSPACE1;
        String cf = "Standard1";

        InetAddress peer = FBUtilities.getBroadcastAddress();
        StreamSession session = new StreamSession(peer, peer, null, 0, true);
        ColumnFamilyStore cfs = Keyspace.open(ks).getColumnFamilyStore(cf);

        // create two sstables
        for (int i = 0; i < 2; i++)
        {
            SchemaLoader.insertData(ks, cf, i, 1);
            cfs.forceBlockingFlush();
        }

        // create streaming task that streams those two sstables
        StreamTransferTask task = new StreamTransferTask(session, cfs.metadata.cfId);
        for (SSTableReader sstable : cfs.getSSTables())
        {
            List<Range<Token>> ranges = new ArrayList<>();
            ranges.add(new Range<>(sstable.first.getToken(), sstable.last.getToken()));
            task.addTransferFile(sstable, sstable.selfRef(), 1, sstable.getPositionsForRanges(ranges), 0);
        }
        assertEquals(2, task.getTotalNumberOfFiles());

        // if file sending completes before timeout then the task should be canceled.
        Future f = task.scheduleTimeout(0, 0, TimeUnit.NANOSECONDS);
        f.get();

        // when timeout runs on second file, task should be completed
        f = task.scheduleTimeout(1, 1, TimeUnit.MILLISECONDS);
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
}
