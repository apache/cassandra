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
package org.apache.cassandra.db.commitlog;

import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.RowUpdateBuilder;
import org.apache.cassandra.db.commitlog.CommitLogSegment.Allocation;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.db.marshal.AsciiType;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.exceptions.CDCWriteException;
import org.apache.cassandra.schema.KeyspaceParams;
import org.assertj.core.api.Assertions;

import static org.apache.cassandra.utils.ByteBufferUtil.bytes;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class CommitLogMetricsTest
{
    private static final String KEYSPACE1 = "clmtest";
    private static final String STANDARD1 = "std1";

    @BeforeClass
    public static void setup()
    {
        DatabaseDescriptor.daemonInitialization();
        SchemaLoader.prepareServer();
        SchemaLoader.createKeyspace(KEYSPACE1,
                                    KeyspaceParams.simple(1),
                                    SchemaLoader.standardCFMD(KEYSPACE1,
                                                              STANDARD1,
                                                              0,
                                                              AsciiType.instance,
                                                              BytesType.instance));
        DatabaseDescriptor.setCommitLogSync(Config.CommitLogSync.batch);
        DatabaseDescriptor.setCommitLogSegmentSize(1); //changing commitlog_segment_size_in_mb to 1mb
        CompactionManager.instance.disableAutoCompaction();
    }

    @Test
    /*
     * 'pending' metrics gets updated in `maybeWaitForSync` methods. We build a Mock service to check it.
     */
    public void testPendingTasks()
    {
        MockCommitLogService commitLogService = new MockCommitLogService()
        {
            protected void maybeWaitForSync(CommitLogSegment.Allocation alloc)
            {
                pending.incrementAndGet();
            }
        };
        long initialPendingTasks = commitLogService.getPendingTasks();
        commitLogService.finishWriteFor(null);
        long currentPendingTasks = commitLogService.getPendingTasks();

        assertEquals(initialPendingTasks + 1, currentPendingTasks);
    }
    
    @Test
    /*
     * Iterate adding to the commit log until we trigger a new segment creation.
     */
    public void testTotalCommitLogSize() throws Exception
    {
        long initialCLSize = CommitLog.instance.metrics.totalCommitLogSize.getValue();

        Keyspace ks = Keyspace.open(KEYSPACE1);
        ColumnFamilyStore cfs = ks.getColumnFamilyStore(STANDARD1);
        Mutation rm1 = new RowUpdateBuilder(cfs.metadata(),
                                            0,
                                            "k").clustering("bytes")
                                                .add("val",
                                                     ByteBuffer.allocate(DatabaseDescriptor.getCommitLogSegmentSize() / 10))
                                                .build();
        long latestCLSize = 0;
        for (int i = 0; i < 100 && latestCLSize <= initialCLSize; i++)
        {
            CommitLog.instance.add(rm1);
            latestCLSize = CommitLog.instance.metrics.totalCommitLogSize.getValue();
        }
        
        Assertions.assertThat(latestCLSize).isGreaterThan(initialCLSize);
    }

    @Test
    public void testCompletedNWatingCommitTasks() throws Exception
    {
        long initialCompletedTasks = CommitLog.instance.metrics.completedTasks.getValue();
        long initialWaitingOnCommitTasks = CommitLog.instance.metrics.waitingOnCommit.getCount();

        Keyspace ks = Keyspace.open(KEYSPACE1);
        ColumnFamilyStore cfs = ks.getColumnFamilyStore(STANDARD1);
        Mutation rm1 = new RowUpdateBuilder(cfs.metadata(), 0, "k").clustering("bytes")
                                                                   .add("val", bytes("this is a string"))
                                                                   .build();
        CommitLog.instance.add(rm1);
        
        long latestCompletedTasks = CommitLog.instance.metrics.completedTasks.getValue();
        long latestWaitingOnCommitTasks = CommitLog.instance.metrics.waitingOnCommit.getCount();
        assertTrue(latestCompletedTasks > initialCompletedTasks);
        assertTrue(latestWaitingOnCommitTasks > initialWaitingOnCommitTasks);
    }
    
    @Test
    /*
     * We build around a Mock service where `awaitAvailableSegment` updates the metric. 
     */
    public void testWaitingOnSegmentAllocation()
    {
        Function<CommitLog, AbstractCommitLogSegmentManager> origCLSMP = DatabaseDescriptor.getCommitLogSegmentMgrProvider();
        DatabaseDescriptor.setCommitLogSegmentMgrProvider(c -> new MockCommitLogSegmentMgr(c, DatabaseDescriptor.getCommitLogLocation()));
        
        MockCommitLogService commitLogService = new MockCommitLogService();
        long initialCount = commitLogService.commitLog.metrics.waitingOnSegmentAllocation.getCount();
        commitLogService.commitLog.add(null);
        long latestCount = commitLogService.commitLog.metrics.waitingOnSegmentAllocation.getCount();
        
        DatabaseDescriptor.setCommitLogSegmentMgrProvider(origCLSMP);
        Assertions.assertThat(latestCount).isGreaterThan(initialCount);
    }
    
    // See CommitLogTest for the 'oversizedMutations' test
    
    public static class MockCommitLogService extends AbstractCommitLogService
    {
        MockCommitLogService()
        {
            super(new MockCommitLog(), "This is not a real commit log", 1000, true);
            lastSyncedAt = 0;
        }

        protected void maybeWaitForSync(CommitLogSegment.Allocation alloc)
        {
            // noop
        }
    }
    
    private static class MockCommitLogSegmentMgr extends CommitLogSegmentManagerStandard {

        public MockCommitLogSegmentMgr(CommitLog commitLog, String storageDirectory)
        {
            super(commitLog, storageDirectory);
        }
        
        @Override
        public Allocation allocate(Mutation mutation, int size)
        {
            awaitAvailableSegment(null);
            return null;
        }

        @Override
        void awaitAvailableSegment(CommitLogSegment currentAllocatingFrom)
        {
            commitLog.metrics.waitingOnSegmentAllocation.update(1, TimeUnit.SECONDS);
        }
    }
    
    private static class MockCommitLog extends CommitLog
    {
        MockCommitLog()
        {
            super(null);
        }

        @Override
        public CommitLogPosition add(Mutation mutation) throws CDCWriteException
        {
            segmentManager.allocate(mutation, -1);
            return CommitLogPosition.NONE;
        }
    }
}