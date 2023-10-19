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

import org.apache.cassandra.db.ConsistencyLevel;

public class CMSSpec
{
    public volatile DurationSpec.LongMillisecondsBound await_timeout = new DurationSpec.LongMillisecondsBound("120000ms");
    public volatile int default_max_retries = 10;
    public volatile DurationSpec.IntMillisecondsBound default_retry_backoff = new DurationSpec.IntMillisecondsBound("50ms");

    /**
     * Specify how often a snapshot of the cluster metadata must be taken.
     * <p>The frequency is express in epochs. A frequency of 100, for example, means that a snapshot will be taken every time
     * the epoch is a multiple of 100.</p>
     * <p>Taking a snapshot will also seal a period (e.g. cluster metadata partition). Therefore the snapshot frequency also determine the size of the
     * {@code system.local_metadata_log} and {@code cluster_metadata.distributed_metadata_log} tables partitions.</p>
     */
    public volatile int metadata_snapshot_frequency = 100;

    public volatile boolean log_out_of_token_range_requests = true;
    public volatile boolean reject_out_of_token_range_requests = true;

    public boolean unsafe_tcm_mode = false;

    public final ProgressBarrierSpec progress_barrier = new ProgressBarrierSpec();

    public static class ProgressBarrierSpec {
        public volatile ConsistencyLevel default_consistency_level = ConsistencyLevel.EACH_QUORUM;

        /**
         * For the purposes of progress barrier we only support ALL, EACH_QUORUM, QUORUM, LOCAL_QUORUM, ANY, and ONE.
         * <p>
         * We will still try all consistency levels above the lowest acceptable, and only fall back to it if we can not
         * collect enough nodes.
         */
        public volatile ConsistencyLevel min_consistency_level = ConsistencyLevel.EACH_QUORUM;

        public volatile DurationSpec.LongMillisecondsBound timeout = new DurationSpec.LongMillisecondsBound("1h");
        public volatile DurationSpec.LongMillisecondsBound backoff = new DurationSpec.LongMillisecondsBound("1s");
    }
}
