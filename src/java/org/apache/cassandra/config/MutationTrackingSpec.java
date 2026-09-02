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

import org.apache.cassandra.exceptions.ConfigurationException;

public class MutationTrackingSpec
{
    public boolean enabled = false;
    public String journal_directory;
    /**
     * Whether the background reconciliation process is enabled
     */
    public volatile boolean background_reconciliation_enabled = true;
    /**
     * The interval in which the backgroun reconciliation process runs
     */
    public volatile DurationSpec.LongMillisecondsBound background_reconciliation_interval = new DurationSpec.LongMillisecondsBound("1s");
    /**
     * Whether the mutation journal's segment-dropping/promotion compactor is enabled
     */
    public volatile boolean journal_compaction_enabled = true;
    /**
     * Limits how many segments eligible for dropping are processed in a single compaction pass. Any remainder
     * is picked up by a subsequent trigger or periodic tick. {@code Integer.MAX_VALUE} disables the limit,
     * relying entirely on segment selection to bound the work of a single pass.
     */
    public volatile int journal_compaction_max_segments = Integer.MAX_VALUE;
    /**
     * When the on-disk mutation journal grows beyond this size, out-of-band promotion of already
     * durably-reconciled but still-unrepaired sstables to repaired is triggered so the journal segments they hold
     * can be released and reclaimed (CASSANDRA-21406). {@code null} disables the mechanism. The default is 256MiB.
     */
    public volatile DataStorageSpec.LongBytesBound journal_promotion_threshold = new DataStorageSpec.LongBytesBound("256MiB");

    /**
     * @return the on-disk mutation journal size (in bytes) beyond which out-of-band promotion of durably-reconciled
     * unrepaired sstables is triggered, or 0 if the mechanism is disabled.
     */
    public long getJournalPromotionThresholdBytes()
    {
        DataStorageSpec.LongBytesBound threshold = journal_promotion_threshold;
        return threshold == null ? 0 : threshold.toBytes();
    }

    public void validate()
    {
        if (journal_compaction_max_segments <= 0)
            throw new ConfigurationException("Invalid value for mutation_tracking.journal_compaction_max_segments \""
                                             + journal_compaction_max_segments + "\". Value must be a positive integer");
    }
}
