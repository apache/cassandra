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

package org.apache.cassandra.config;

import java.util.concurrent.TimeUnit;

import accord.primitives.TxnId;
import accord.utils.Invariants;
import com.fasterxml.jackson.annotation.JsonIgnore;
import org.apache.cassandra.journal.Params;
import org.apache.cassandra.service.consensus.TransactionalMode;

import static accord.primitives.Routable.Domain.Range;
import static org.apache.cassandra.config.AccordSpec.QueueShardModel.THREAD_POOL_PER_SHARD;
import static org.apache.cassandra.config.AccordSpec.QueueSubmissionModel.SYNC;

public class AccordSpec
{
    public volatile boolean enabled = false;

    public volatile String journal_directory;

    public volatile boolean enable_journal_compaction = true;

    /**
     * Enables the virtual Accord debug-only keyspace with tables
     * that expose internal state to aid the developers working
     * on Accord implementation.
     * <p/>
     * These tables can and will change and/or go away at any point,
     * including in a minor release, are not to be considered part of the API,
     * and are NOT to be relied on for anything.
     * <p/>
     * Only enable this keyspace if you are working on Accord and
     * need to debug an issue with Accord implementation, or if an Accord
     * developer asked you to.
     */
    public boolean enable_virtual_debug_only_keyspace = false;

    public enum QueueShardModel
    {
        /**
         * Same number of threads as queue shards, but the shard lock is held only while managing the queue,
         * so that submitting threads may queue load/save work.
         *
         * The global READ and WRITE stages are used for IO.
         */
        THREAD_PER_SHARD,

        /**
         * Same number of threads as shards, and the shard lock is held for the duration of serving requests.
         * The global READ and WRITE stages are used for IO.
         */
        THREAD_PER_SHARD_SYNC_QUEUE,

        /**
         * More threads than shards. Threads update transaction state as well as performing IO, minimising context switching.
         * Fewer shards is generally better, until queue-contention is encountered.
         */
        THREAD_POOL_PER_SHARD,

        /**
         * More threads than shards. Threads update transaction state only, relying on READ and WRITE stages for IO.
         * Fewer shards is generally better, until queue-contention is encountered.
         */
        THREAD_POOL_PER_SHARD_EXCLUDES_IO,
    }

    public enum QueueSubmissionModel
    {
        /**
         * The queue workers and all submissions require ownership of the lock.
         */
        SYNC,

        /**
         * The queue workers and some submissions require ownership of the lock.
         * That is, if the lock is available on submission we take it; if it is not we try to guarantee that
         * another thread will witness the work submission promptly, but if we cannot we wait for the lock
         * to ensure work is scheduled.
         */
        SEMI_SYNC,

        /**
         * The queue workers only require ownership of the lock, submissions happens fully asynchronously.
         */
        ASYNC,

        /**
         * The queue is backed by submission to a single-threaded plain executor.
         * This implementation does not honur the sharding model option.
         *
         * Note: this isn't intended to be used by real clusters.
         */
        EXEC_ST
    }

    public QueueShardModel queue_shard_model = THREAD_POOL_PER_SHARD;
    public QueueSubmissionModel queue_submission_model = SYNC;

    /**
     * The number of queue (and cache) shards.
     */
    public volatile OptionaldPositiveInt queue_shard_count = OptionaldPositiveInt.UNDEFINED;

    /**
     * The target number of command stores to create per topology shard.
     * This determines the amount of execution parallelism possible for a given table/shard on the host.
     * More shards means more parallelism, but more state.
     *
     * TODO (expected): make this a table property
     * TODO (expected): adjust this by proportion of ring
     */
    public volatile OptionaldPositiveInt command_store_shard_count = OptionaldPositiveInt.UNDEFINED;

    public volatile OptionaldPositiveInt max_queued_loads = OptionaldPositiveInt.UNDEFINED;
    public volatile OptionaldPositiveInt max_queued_range_loads = OptionaldPositiveInt.UNDEFINED;

    public DataStorageSpec.LongMebibytesBound cache_size = null;
    public DataStorageSpec.LongMebibytesBound working_set_size = null;
    public boolean shrink_cache_entries_before_eviction = true;

    // TODO (expected): we should be able to support lower recover delays, at least for txns
    public volatile DurationSpec.IntMillisecondsBound recover_delay = new DurationSpec.IntMillisecondsBound(5000);
    public volatile DurationSpec.IntMillisecondsBound range_syncpoint_recover_delay = new DurationSpec.IntMillisecondsBound("5m");
    public String slowPreAccept = "30ms <= p50*2 <= 100ms";
    public String slowRead = "30ms <= p50*2 <= 100ms";

    public long recoveryDelayFor(TxnId txnId, TimeUnit unit)
    {
        if (txnId.isSyncPoint() && txnId.is(Range))
            return range_syncpoint_recover_delay.to(unit);
        return recover_delay.to(unit);
    }

    /**
     * When a barrier transaction is requested how many times to repeat attempting the barrier before giving up
     */
    public int barrier_retry_attempts = 5;

    /**
     * When a barrier transaction fails how long the initial backoff should be before being increased
     * as part of exponential backoff on each attempt
     */
    public DurationSpec.IntMillisecondsBound barrier_retry_inital_backoff_millis = new DurationSpec.IntMillisecondsBound("1s");

    public DurationSpec.IntMillisecondsBound barrier_max_backoff = new DurationSpec.IntMillisecondsBound("10m");

    public DurationSpec.IntMillisecondsBound range_syncpoint_timeout = new DurationSpec.IntMillisecondsBound("2m");

    public volatile DurationSpec.IntSecondsBound fast_path_update_delay = new DurationSpec.IntSecondsBound("60m");

    public volatile DurationSpec.IntSecondsBound gc_delay = new DurationSpec.IntSecondsBound("5m");
    public volatile int shard_durability_target_splits = 128;
    public volatile DurationSpec.IntSecondsBound durability_txnid_lag = new DurationSpec.IntSecondsBound(5);
    public volatile DurationSpec.IntSecondsBound shard_durability_cycle = new DurationSpec.IntSecondsBound(15, TimeUnit.MINUTES);
    public volatile DurationSpec.IntSecondsBound global_durability_cycle = new DurationSpec.IntSecondsBound(10, TimeUnit.MINUTES);
    public volatile DurationSpec.IntSecondsBound default_durability_retry_delay = new DurationSpec.IntSecondsBound(10, TimeUnit.SECONDS);
    public volatile DurationSpec.IntSecondsBound max_durability_retry_delay = new DurationSpec.IntSecondsBound(10, TimeUnit.MINUTES);

    public enum TransactionalRangeMigration
    {
        auto, explicit
    }

    /**
     * Defines the behavior of range migration opt-in when changing transactional settings on a table. In auto,
     * all ranges are marked as migrating and no additional user action is needed aside from running repairs. In
     * explicit, no ranges are marked as migrating, and the user needs to explicitly mark ranges as migrating to
     * the target transactional mode via nodetool.
     */
    public volatile TransactionalRangeMigration range_migration = TransactionalRangeMigration.auto;

    /**
     * default transactional mode for tables created by this node when no transactional mode has been specified in the DDL
     */
    public TransactionalMode default_transactional_mode = TransactionalMode.off;
    public boolean ephemeralReadEnabled = true;
    public boolean state_cache_listener_jfr_enabled = true;
    public final JournalSpec journal = new JournalSpec();
    public final MinEpochRetrySpec minEpochSyncRetry = new MinEpochRetrySpec();

    public static class MinEpochRetrySpec extends RetrySpec
    {
        public MinEpochRetrySpec()
        {
            maxAttempts = new MaxAttempt(3);
        }
    }

    public static class JournalSpec implements Params
    {
        public int segmentSize = 32 << 20;
        public FailurePolicy failurePolicy = FailurePolicy.STOP;
        public FlushMode flushMode = FlushMode.PERIODIC;
        public volatile DurationSpec flushPeriod; // pulls default from 'commitlog_sync_period'
        public DurationSpec periodicFlushLagBlock = new DurationSpec.IntMillisecondsBound("1500ms");
        public DurationSpec.IntMillisecondsBound compactionPeriod = new DurationSpec.IntMillisecondsBound("60000ms");
        private volatile long flushCombinedBlockPeriod = Long.MIN_VALUE;

        public void setFlushPeriod(DurationSpec newFlushPeriod)
        {
            flushPeriod = newFlushPeriod;
            flushCombinedBlockPeriod = Long.MIN_VALUE;
        }

        public void setPeriodicFlushLagBlock(DurationSpec newPeriodicFlushLagBlock)
        {
            periodicFlushLagBlock = newPeriodicFlushLagBlock;
            flushCombinedBlockPeriod = Long.MIN_VALUE;
        }

        @Override
        public int segmentSize()
        {
            return segmentSize;
        }

        @Override
        public FailurePolicy failurePolicy()
        {
            return failurePolicy;
        }

        @Override
        public FlushMode flushMode()
        {
            return flushMode;
        }

        @Override
        public boolean enableCompaction()
        {
            return DatabaseDescriptor.getAccord().enable_journal_compaction;
        }

        @Override
        public long compactionPeriod(TimeUnit unit)
        {
            return compactionPeriod.to(unit);
        }

        @JsonIgnore
        @Override
        public long flushPeriod(TimeUnit units)
        {
            return flushPeriod.to(units);
        }

        @JsonIgnore
        @Override
        public long periodicBlockPeriod(TimeUnit units)
        {
            long nanos = flushCombinedBlockPeriod;
            if (nanos >= 0)
                return units.convert(nanos, TimeUnit.NANOSECONDS);

            long flushPeriodNanos = flushPeriod(TimeUnit.NANOSECONDS);
            Invariants.checkState(flushPeriodNanos > 0);
            nanos = periodicFlushLagBlock.to(TimeUnit.NANOSECONDS) + flushPeriodNanos;
            // it is possible for this to race and cache the wrong value after an update
            flushCombinedBlockPeriod = nanos;
            return nanos;
        }

        /**
         * This is required by the journal, but we don't have multiple versions, so block it from showing up, so we don't need to worry about maintaining it
         */
        @JsonIgnore
        @Override
        public int userVersion()
        {
            /*
             * NOTE: when accord journal version gets bumped, expose it via yaml.
             * This way operators can force previous version on upgrade, temporarily,
             * to allow easier downgrades if something goes wrong.
             */
            return 1;
        }
    }
}
