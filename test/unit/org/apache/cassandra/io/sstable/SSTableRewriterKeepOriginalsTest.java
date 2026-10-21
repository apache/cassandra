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

package org.apache.cassandra.io.sstable;

import java.util.HashSet;
import java.util.Set;

import org.junit.Test;

import org.apache.cassandra.Util;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.RowUpdateBuilder;
import org.apache.cassandra.db.compaction.AbstractCompactionStrategy;
import org.apache.cassandra.db.compaction.CompactionController;
import org.apache.cassandra.db.compaction.CompactionIterator;
import org.apache.cassandra.db.compaction.OperationType;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.io.sstable.format.SSTableFormat;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;

import static org.apache.cassandra.db.compaction.OperationType.COMPACTION;
import static org.apache.cassandra.utils.TimeUUID.Generator.nextTimeUUID;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * keepOriginals=true must retain the original sstables even when early open is in play:
 * moveStarts() used to obsolete fully-covered originals unconditionally (only the bulk
 * obsoleteOriginals() call honored the flag), so a full-range rewrite with an online
 * transaction deleted every original after the last reference was released.
 *
 * Isolated in its own class deliberately: a kept original intentionally leaves accounting
 * residue in the column family (its bytes remain counted while no longer in the live set),
 * which would poison the shared-fixture assertions of SSTableRewriterTest; per-class test
 * JVMs make that residue harmless here.
 */
public class SSTableRewriterKeepOriginalsTest extends SSTableWriterTestBase
{
    @Test
    public void keepOriginalsSurvivesEarlyOpen() throws Exception
    {
        Keyspace keyspace = Keyspace.open(KEYSPACE);
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(CF);
        truncate(cfs);

        for (int j = 0; j < 100; j++)
        {
            new RowUpdateBuilder(cfs.metadata(), j, String.valueOf(j))
                .clustering("0")
                .add("val", ByteBufferUtil.EMPTY_BYTE_BUFFER)
                .build()
                .apply();
        }
        Util.flush(cfs);
        Set<SSTableReader> sstables = new HashSet<>(cfs.getLiveSSTables());
        assertEquals(1, sstables.size());
        SSTableReader original = sstables.iterator().next();
        File originalData = original.descriptor.fileFor(SSTableFormat.Components.DATA);
        long nowInSec = FBUtilities.nowInSeconds();
        try (AbstractCompactionStrategy.ScannerList scanners = cfs.getCompactionStrategyManager().getScanners(sstables);
             LifecycleTransaction txn = cfs.getTracker().tryModify(sstables, OperationType.UNKNOWN);
             SSTableRewriter writer = SSTableRewriter.constructKeepingOriginals(txn, true, 1000);
             CompactionController controller = new CompactionController(cfs, sstables, cfs.gcBefore(nowInSec));
             CompactionIterator ci = new CompactionIterator(COMPACTION, scanners.scanners, controller, nowInSec, nextTimeUUID()))
        {
            writer.switchWriter(getWriter(cfs, original.descriptor.directory, txn));
            while (ci.hasNext())
                writer.append(ci.next());
            writer.finish();
        }
        LifecycleTransaction.waitForDeletions();

        assertTrue("keepOriginals=true must retain the original sstable through an online " +
                   "early-open rewrite, but its data file was deleted",
                   originalData.exists());
    }
}
