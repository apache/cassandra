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

import com.fasterxml.jackson.annotation.JsonIgnore;
import org.apache.cassandra.journal.Params;
import org.apache.cassandra.service.consensus.TransactionalMode;

public class AccordSpec
{
    public volatile boolean enabled = false;

    public volatile String journal_directory;

    public volatile OptionaldPositiveInt shard_count = OptionaldPositiveInt.UNDEFINED;

    public volatile DurationSpec.IntSecondsBound progress_log_schedule_delay = new DurationSpec.IntSecondsBound(1);

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

    public DurationSpec.IntMillisecondsBound range_barrier_timeout = new DurationSpec.IntMillisecondsBound("2m");

    public volatile DurationSpec fast_path_update_delay = new DurationSpec.IntSecondsBound(5);

    public volatile DurationSpec schedule_durability_frequency = new DurationSpec.IntSecondsBound(5);
    public volatile DurationSpec durability_txnid_lag = new DurationSpec.IntSecondsBound(5);
    public volatile DurationSpec shard_durability_cycle = new DurationSpec.IntMinutesBound(1);
    public volatile DurationSpec global_durability_cycle = new DurationSpec.IntMinutesBound(10);

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

    public static class JournalSpec implements Params
    {
        public int segmentSize = 32 << 20;
        public FailurePolicy failurePolicy = FailurePolicy.STOP;
        public FlushMode flushMode = FlushMode.BATCH;
        public DurationSpec.IntMillisecondsBound flushPeriod; // pulls default from 'commitlog_sync_period'
        public DurationSpec.IntMillisecondsBound periodicFlushLagBlock = new DurationSpec.IntMillisecondsBound("1500ms");

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
