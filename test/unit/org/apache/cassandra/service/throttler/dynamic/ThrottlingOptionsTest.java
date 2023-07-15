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

import org.junit.Assert;
import org.junit.Test;

public class ThrottlingOptionsTest
{
    @Test
    public void testThrottlingOptions()
    {
        ThrottlingOptions throttlingOptions = new ThrottlingOptions();
        Assert.assertTrue(throttlingOptions.enabled);
        Assert.assertEquals(35, throttlingOptions.cpu_threshold_cur);
        Assert.assertEquals(35, throttlingOptions.cpu_threshold_one_minute);
        Assert.assertEquals(1, throttlingOptions.nr_throttling_threshold_cur);
        Assert.assertEquals(1, throttlingOptions.nr_throttling_threshold_one_minute);
        Assert.assertEquals(0, throttlingOptions.pending_reads_threshold_cur);
        Assert.assertEquals(0, throttlingOptions.pending_reads_threshold_one_minute);
        Assert.assertEquals(0, throttlingOptions.pending_mutations_threshold_cur);
        Assert.assertEquals(0, throttlingOptions.pending_mutations_threshold_one_minute);
        Assert.assertEquals(0.1, throttlingOptions.percentage_of_traffice_to_throttling, 0.0);
        Assert.assertEquals(1 * 60, throttlingOptions.more_aggressive_throttling_after_in_sec = 1 * 60);
        Assert.assertEquals(15 * 60, throttlingOptions.reset_after_no_throttling_seen_in_sec = 15 * 60);
        Assert.assertEquals(4, throttlingOptions.aggressive_throttling_latency_ratio, 0.0);
    }
}
