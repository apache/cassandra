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
package org.apache.cassandra.db.compaction;

import static org.apache.cassandra.db.transform.DuplicateRowCheckerTest.assertCommandIssued;
import static org.apache.cassandra.db.transform.DuplicateRowCheckerTest.iter;
import static org.apache.cassandra.db.transform.DuplicateRowCheckerTest.makeRow;
import static org.junit.Assert.*;

import java.net.InetAddress;
import java.util.*;

import org.junit.Test;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.db.rows.*;
import org.apache.cassandra.io.sstable.ISSTableScanner;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.net.*;
import org.apache.cassandra.utils.FBUtilities;

public class CompactionIteratorTest extends CQLTester
{
    @Test
    public void duplicateRowsTest() throws Throwable
    {
        System.setProperty("cassandra.diagnostic_snapshot_interval_nanos", "0");
        // Create a table and insert some data. The actual rows read in the test will be synthetic
        // but this creates an sstable on disk to be snapshotted.
        createTable("CREATE TABLE %s (pk text, ck1 int, ck2 int, v int, PRIMARY KEY (pk, ck1, ck2))");
        for (int i = 0; i < 10; i++)
            execute("insert into %s (pk, ck1, ck2, v) values (?, ?, ?, ?)", "key", i, i, i);
        flush();

        DatabaseDescriptor.setSnapshotOnDuplicateRowDetection(true);
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        CFMetaData metadata = getCurrentColumnFamilyStore().metadata;

        final HashMap<InetAddress, MessageOut> sentMessages = new HashMap<>();
        IMessageSink sink = new IMessageSink()
        {
            public boolean allowOutgoingMessage(MessageOut message, int id, InetAddress to)
            {
                sentMessages.put(to, message);
                return false;
            }

            public boolean allowIncomingMessage(MessageIn message, int id)
            {
                return false;
            }
        };
        MessagingService.instance().addMessageSink(sink);

        // no duplicates
        sentMessages.clear();
        iterate(cfs, iter(metadata,
                          false,
                          makeRow(metadata,0, 0),
                          makeRow(metadata,0, 1),
                          makeRow(metadata,0, 2)));
        assertCommandIssued(sentMessages, false);

        // now test with a duplicate row and see that we issue a snapshot command
        sentMessages.clear();
        iterate(cfs, iter(metadata,
                          false,
                          makeRow(metadata, 0, 0),
                          makeRow(metadata, 0, 1),
                          makeRow(metadata, 0, 1)));
        assertCommandIssued(sentMessages, true);
    }

    private void iterate(ColumnFamilyStore cfs, UnfilteredPartitionIterator partitions)
    {

        try (CompactionController controller = new CompactionController(getCurrentColumnFamilyStore(), Integer.MAX_VALUE);
             ISSTableScanner scanner = scanner(cfs, partitions);
             CompactionIterator iter = new CompactionIterator(OperationType.COMPACTION,
                                                              Collections.singletonList(scanner),
                                                              controller, FBUtilities.nowInSeconds(), null))
        {
            while (iter.hasNext())
            {
                try (UnfilteredRowIterator partition = iter.next())
                {
                    partition.forEachRemaining(u -> {});
                }
            }
        }
    }

    private ISSTableScanner scanner(final ColumnFamilyStore cfs, final UnfilteredPartitionIterator partitions)
    {

        return new ISSTableScanner()
        {
            public long getLengthInBytes() { return 0; }

            public long getCurrentPosition() { return 0; }

            public String getBackingFiles() { return cfs.getLiveSSTables().iterator().next().toString(); }

            public boolean isForThrift() { return false; }

            public CFMetaData metadata() { return cfs.metadata; }

            public void close() { }

            public boolean hasNext() { return partitions.hasNext(); }

            public UnfilteredRowIterator next() { return partitions.next(); }
        };
    }
}
