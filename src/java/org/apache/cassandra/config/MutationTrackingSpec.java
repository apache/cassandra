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
     * Whether unrepaired sstables whose mutations have all reconciled are promoted to repaired in the background
     */
    public volatile boolean reconciled_sstable_promotion_enabled = true;
    /**
     * The interval at which reconciled sstables are promoted to repaired.
     *
     * This also bounds how long coordinator log offsets accumulate, since they are now removed only at promotion
     * rather than incrementally at compaction.
     */
    public volatile DurationSpec.LongMillisecondsBound reconciled_sstable_promotion_interval = new DurationSpec.LongMillisecondsBound("60s");
}
