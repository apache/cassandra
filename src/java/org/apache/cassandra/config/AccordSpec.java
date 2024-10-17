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
        SYNC, SEMI_SYNC, ASYNC
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

    public DataStorageSpec.LongMebibytesBound cache_size = null;
    public DataStorageSpec.LongMebibytesBound working_set_size = null;

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
    public boolean ephemeralReadEnabled = false;
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
        public DurationSpec.IntMillisecondsBound flushPeriod; // pulls default from 'commitlog_sync_period'
        public DurationSpec.IntMillisecondsBound periodicFlushLagBlock = new DurationSpec.IntMillisecondsBound("1500ms");
        public DurationSpec.IntMillisecondsBound compactionPeriod = new DurationSpec.IntMillisecondsBound("60000ms");

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
        public int compactionPeriodMillis()
        {
            return compactionPeriod.toMilliseconds();
        }

        @JsonIgnore
        @Override
        public int flushPeriodMillis()
        {
            return flushPeriod == null ? DatabaseDescriptor.getCommitLogSyncPeriod()
                                       : flushPeriod.toMilliseconds();
        }

        @JsonIgnore
        @Override
        public int periodicFlushLagBlock()
        {
            return periodicFlushLagBlock.toMilliseconds();
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
