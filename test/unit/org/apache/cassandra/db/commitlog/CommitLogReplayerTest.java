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


import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.slf4j.LoggerFactory;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.AppenderBase;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.utils.concurrent.Future;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.metadata.StatsMetadata;
import org.apache.cassandra.utils.EstimatedHistogram;
import org.apache.cassandra.utils.streamhist.TombstoneHistogram;
import org.jboss.byteman.contrib.bmunit.BMRule;
import org.jboss.byteman.contrib.bmunit.BMRules;
import org.jboss.byteman.contrib.bmunit.BMUnitRunner;

import static org.junit.Assert.assertTrue;

import org.apache.cassandra.utils.ByteBufferUtil;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(BMUnitRunner.class)
public class CommitLogReplayerTest
{
    @BeforeClass
    public static void before()
    {
        DatabaseDescriptor.daemonInitialization();
    }
    private static CommitLogReplayer newReplayer()
    {
        return new CommitLogReplayer(CommitLog.instance, CommitLogPosition.NONE, null, CommitLogReplayer.ReplayFilter.create());
    }

    private static Future<Integer> initiateMutation(CommitLogReplayer.MutationInitiator mutationInitiator, CommitLogReplayer replayer, String segment)
    {
        CommitLogDescriptor descriptor = mock(CommitLogDescriptor.class);
        when(descriptor.fileName()).thenReturn(segment);
        return mutationInitiator.initiateMutation(mock(Mutation.class), descriptor, 0, 0, replayer);
    }

    @Test
    @BMRules(rules = { @BMRule(name = "Fail applying mutation",
            targetClass = "org.apache.cassandra.concurrent.Stage",
            targetMethod = "submit",
            action = "return org.apache.cassandra.utils.concurrent.ImmediateFuture.failure(new RuntimeException(\"mutation failed\"));") } )
    public void testTrackingSegmentsWhenMutationFails()
    {
        CommitLogReplayer replayer = newReplayer();
        String failedSegment = "failedSegment";
        Future<Integer> mutationFuture = initiateMutation(new CommitLogReplayer.MutationInitiator(), replayer, failedSegment);
        Assert.assertThrows(ExecutionException.class, () -> mutationFuture.get());
        Assert.assertTrue(!replayer.getSegmentWithFailedMutations().isEmpty());
        Assert.assertTrue(replayer.getSegmentWithFailedMutations().contains(failedSegment));
    }

    /**
     * When the mutation is applied successfully, the returned future must succeed with the submitted result and no
     * segment must be recorded as failed.
     */
    @Test
    @BMRules(rules = { @BMRule(name = "Succeed applying mutation",
            targetClass = "org.apache.cassandra.concurrent.Stage",
            targetMethod = "submit",
            action = "return org.apache.cassandra.utils.concurrent.ImmediateFuture.success(Integer.valueOf(42));") } )
    public void testSuccessfulMutationCompletesFutureAndTracksNoFailedSegment() throws Exception
    {
        CommitLogReplayer.MutationInitiator mutationInitiator = new CommitLogReplayer.MutationInitiator()
        {
            @Override
            protected void onFailedMutation(String keyspace, Collection<TableId> tableIds)
            {
                Assert.fail("onFailedMutation must not be called when the mutation succeeds");
            }
        };
        CommitLogReplayer replayer = newReplayer();
        Future<Integer> mutationFuture = initiateMutation(mutationInitiator, replayer, "segment");

        Assert.assertEquals(Integer.valueOf(42), mutationFuture.get(5, TimeUnit.SECONDS));
        Assert.assertTrue(replayer.getSegmentWithFailedMutations().isEmpty());
    }

    /**
     * If the overridable onFailedMutation hook throws, the returned future must still complete and the segment must
     * still be recorded as failed. Otherwise the replayer would wait forever in FBUtilities.waitOnFuture.
     */
    @Test
    @BMRules(rules = { @BMRule(name = "Fail applying mutation",
            targetClass = "org.apache.cassandra.concurrent.Stage",
            targetMethod = "submit",
            action = "return org.apache.cassandra.utils.concurrent.ImmediateFuture.failure(new RuntimeException(\"mutation failed\"));") } )
    public void testFutureCompletesWhenOnFailedMutationHookThrows() throws Exception
    {
        CommitLogReplayer.MutationInitiator mutationInitiator = new CommitLogReplayer.MutationInitiator()
        {
            @Override
            protected void onFailedMutation(String keyspace, Collection<TableId> tableIds)
            {
                throw new IllegalStateException("hook failed");
            }
        };
        CommitLogReplayer replayer = newReplayer();
        String failedSegment = "failedSegment";
        Future<Integer> mutationFuture = initiateMutation(mutationInitiator, replayer, failedSegment);

        Assert.assertTrue("returned future must complete even if onFailedMutation throws", mutationFuture.awaitUninterruptibly(5, TimeUnit.SECONDS));
        Assert.assertTrue(mutationFuture.isDone());
        Assert.assertFalse("future must be failed, not successful", mutationFuture.isSuccess());
        Throwable cause = mutationFuture.cause();
        Assert.assertTrue("expected the original RuntimeException but got " + cause, cause != null && cause.getClass() == RuntimeException.class);
        Assert.assertEquals("mutation failed", cause.getMessage());
        Assert.assertTrue("segment must be recorded as failed even if the hook throws",
                          replayer.getSegmentWithFailedMutations().contains(failedSegment));
    }

    /**
     * The original failure cause must be propagated unchanged, Errors included (not wrapped in a RuntimeException).
     */
    @Test
    @BMRules(rules = { @BMRule(name = "Fail applying mutation with an Error",
            targetClass = "org.apache.cassandra.concurrent.Stage",
            targetMethod = "submit",
            action = "return org.apache.cassandra.utils.concurrent.ImmediateFuture.failure(new java.lang.AssertionError(\"boom\"));") } )
    public void testOriginalErrorIsPropagatedWhenMutationFails()
    {
        Future<Integer> mutationFuture = initiateMutation(new CommitLogReplayer.MutationInitiator(), newReplayer(), "failedSegment");

        Assert.assertTrue(mutationFuture.isDone());
        Throwable cause = mutationFuture.cause();
        Assert.assertTrue("expected the original AssertionError but got " + cause, cause instanceof AssertionError);
        Assert.assertEquals("boom", cause.getMessage());
    }

    /**
     * A failure with no cause (e.g. CancellationException) must not be replaced by a RuntimeException wrapping null.
     */
    @Test
    @BMRules(rules = { @BMRule(name = "Fail applying mutation with a cancellation",
            targetClass = "org.apache.cassandra.concurrent.Stage",
            targetMethod = "submit",
            action = "return org.apache.cassandra.utils.concurrent.ImmediateFuture.failure(new java.util.concurrent.CancellationException(\"cancelled\"));") } )
    public void testCauseIsPreservedWhenFailureHasNoCause()
    {
        Future<Integer> mutationFuture = initiateMutation(new CommitLogReplayer.MutationInitiator(), newReplayer(), "failedSegment");

        Assert.assertTrue(mutationFuture.isDone());
        Throwable cause = mutationFuture.cause();
        Assert.assertTrue("expected CancellationException but got " + cause, cause instanceof CancellationException);
    }

    /**
     * Test that when there are few skipped SSTables (<= 100), all are logged in debug mode.
     * This tests the fix for CNDB-15157.
     */
    @Test
    public void testPersistedIntervalsLogsAllSSTablesWhenFew()
    {
        ch.qos.logback.classic.Logger logger = (ch.qos.logback.classic.Logger) LoggerFactory.getLogger(CommitLogReplayer.class);
        InMemoryAppender appender = new InMemoryAppender();
        logger.addAppender(appender);
        Level originalLevel = logger.getLevel();
        logger.setLevel(Level.DEBUG);

        try
        {
            // Create 50 mock SSTables with different host IDs (so they'll be skipped)
            List<SSTableReader> sstables = new ArrayList<>();
            UUID differentHostId = UUID.randomUUID();
            for (int i = 0; i < 50; i++)
            {
                SSTableReader reader = mock(SSTableReader.class);
                StatsMetadata metadata = createMinimalStatsMetadata(differentHostId);
                when(reader.getSSTableMetadata()).thenReturn(metadata);
                when(reader.getFilename()).thenReturn("sstable-" + i + ".db");
                sstables.add(reader);
            }

            UUID localhostId = UUID.randomUUID();
            CommitLogReplayer.persistedIntervals(sstables, null, localhostId);

            // Verify that debug log contains all sstables (not truncated)
            List<ILoggingEvent> debugEvents = appender.getEventsForLevel(Level.DEBUG);
            boolean foundFullList = false;
            for (ILoggingEvent event : debugEvents)
            {
                String message = event.getFormattedMessage();
                if (message.contains("Ignored commitLogIntervals from the following sstables:"))
                {
                    foundFullList = true;
                    // Should NOT contain the "showing first" message when count is <= 100
                    Assert.assertFalse("Should not limit logging when <= 100 SSTables",
                                     message.contains("showing first"));
                    break;
                }
            }
            assertTrue("Should have logged the full sstables list", foundFullList);
        }
        finally
        {
            logger.detachAppender(appender);
            logger.setLevel(originalLevel);
        }
    }

    /**
     * Test that when there are many skipped SSTables (> 100), only the first 100 are logged.
     * This tests the fix for CNDB-15157.
     */
    @Test
    public void testPersistedIntervalsLimitsLoggingWhenMany()
    {
        ch.qos.logback.classic.Logger logger = (ch.qos.logback.classic.Logger) LoggerFactory.getLogger(CommitLogReplayer.class);
        InMemoryAppender appender = new InMemoryAppender();
        logger.addAppender(appender);
        Level originalLevel = logger.getLevel();
        logger.setLevel(Level.TRACE); // Need TRACE level to capture the full list message

        try
        {
            // Create 150 mock SSTables with different host IDs (so they'll be skipped)
            List<SSTableReader> sstables = new ArrayList<>();
            UUID differentHostId = UUID.randomUUID();
            for (int i = 0; i < 150; i++)
            {
                SSTableReader reader = mock(SSTableReader.class);
                StatsMetadata metadata = createMinimalStatsMetadata(differentHostId);
                when(reader.getSSTableMetadata()).thenReturn(metadata);
                when(reader.getFilename()).thenReturn("sstable-" + i + ".db");
                sstables.add(reader);
            }

            UUID localhostId = UUID.randomUUID();
            CommitLogReplayer.persistedIntervals(sstables, null, localhostId);

            // Verify that debug log is limited
            List<ILoggingEvent> debugEvents = appender.getEventsForLevel(Level.DEBUG);
            boolean foundLimitedList = false;
            boolean foundTraceMessage = false;
            for (ILoggingEvent event : debugEvents)
            {
                String message = event.getFormattedMessage();
                if (message.contains("Ignored commitLogIntervals from 150 sstables (showing first 100)"))
                {
                    foundLimitedList = true;
                }
                if (message.contains("Use TRACE level to see all 150 skipped sstables"))
                {
                    foundTraceMessage = true;
                }
            }
            assertTrue("Should have logged limited sstables list", foundLimitedList);
            assertTrue("Should have logged message about TRACE level", foundTraceMessage);

            // Verify trace log has full list
            List<ILoggingEvent> traceEvents = appender.getEventsForLevel(Level.TRACE);
            boolean foundFullListInTrace = false;
            for (ILoggingEvent event : traceEvents)
            {
                String message = event.getFormattedMessage();
                if (message.contains("Full list of ignored sstables:"))
                {
                    foundFullListInTrace = true;
                    break;
                }
            }
            assertTrue("Should have logged full list at TRACE level", foundFullListInTrace);
        }
        finally
        {
            logger.detachAppender(appender);
            logger.setLevel(originalLevel);
        }
    }

    /**
     * Creates a minimal StatsMetadata with only the fields needed for testing persistedIntervals.
     */
    private static StatsMetadata createMinimalStatsMetadata(UUID originatingHostId)
    {
        return new StatsMetadata(new EstimatedHistogram(150, true),
                                new EstimatedHistogram(150, true),
                                IntervalSet.empty(),
                                0L, // minTimestamp
                                0L, // maxTimestamp
                                Long.MAX_VALUE, // minLocalDeletionTime
                                Long.MAX_VALUE, // maxLocalDeletionTime
                                Integer.MAX_VALUE, // minTTL
                                Integer.MAX_VALUE, // maxTTL
                                0.0, // compressionRatio
                                TombstoneHistogram.createDefault(), // estimatedTombstoneDropTime
                                0, // sstableLevel
                                null, // clusteringTypes
                                null, // coveredClustering
                                false, // hasLegacyCounterShards
                                0L, // repairedAt
                                0L, // totalColumnsSet
                                0L, // totalRows
                                0.0, // tokenSpaceCoverage
                                originatingHostId,
                                null, // pendingRepair
                                false, // isTransient
                                false,    //boolean hasPartitionLevelDeletions,
                                ByteBufferUtil.EMPTY_BYTE_BUFFER,    //ByteBuffer firstKey,
                                ByteBufferUtil.EMPTY_BYTE_BUFFER,    //ByteBuffer lastKey,
                                Collections.emptyMap(), // maxColumnValueLengths
                                null); // zeroCopyMetadata
    }

    private static class InMemoryAppender extends AppenderBase<ILoggingEvent>
    {
        private final List<ILoggingEvent> events = new ArrayList<>();

        private InMemoryAppender()
        {
            start();
        }

        @Override
        protected synchronized void append(ILoggingEvent event)
        {
            events.add(event);
        }

        public synchronized List<ILoggingEvent> getEventsForLevel(Level level)
        {
            List<ILoggingEvent> result = new ArrayList<>();
            for (ILoggingEvent event : events)
            {
                if (event.getLevel() == level)
                {
                    result.add(event);
                }
            }
            return result;
        }
    }
}
