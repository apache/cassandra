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

package org.apache.cassandra.service.throttler.dynamic;

public class ThrottlingOptions
{
    // TODO: need to have the hardcoded values to configurable in cassandra.yaml
    // Also, adjust the default values based on the POC
    public boolean enabled = true;
    public long cpu_threshold_cur = 35;
    public long cpu_threshold_one_minute = 35;
    public long nr_throttling_threshold_cur = 1;
    public long nr_throttling_threshold_one_minute = 1;
    public long pending_reads_threshold_cur = 0;
    public long pending_reads_threshold_one_minute = 0;
    public int pending_mutations_thresholdCur = 0;
    public int pending_mutations_threshold_one_minute = 0;
    public double percentage_of_traffice_to_throttling = 0.1;
    public int more_aggressive_throttling_after_in_sec = 1 * 60; // 1 minutes
    public int reset_after_no_throttling_seen_in_sec = 15 * 60; // 15 minutes
    public double aggressive_throttling_latency_ratio = 4;
}
