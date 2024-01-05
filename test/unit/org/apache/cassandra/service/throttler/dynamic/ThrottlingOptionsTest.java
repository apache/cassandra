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
        Assert.assertFalse(throttlingOptions.isEnabled());
        Assert.assertEquals(35, throttlingOptions.getCpuThresholdCur());
        Assert.assertEquals(35, throttlingOptions.getCpuThresholdOneMinute());
        Assert.assertEquals(0, throttlingOptions.getPendingReadsThresholdCur());
        Assert.assertEquals(0, throttlingOptions.getPendingReadsThresholdOneMinute());
        Assert.assertEquals(0, throttlingOptions.getPendingMutationsThresholdCur());
        Assert.assertEquals(0, throttlingOptions.getPendingMutationsThresholdOneMinute());
        Assert.assertEquals(0.1, throttlingOptions.getPercentageOfTrafficToThrottling(), 0.0);
        Assert.assertEquals(1 * 60, throttlingOptions.getMoreAggressiveThrottlingAfterInSec());
        Assert.assertEquals(15 * 60, throttlingOptions.getResetAfterNoThrottlingSeenInSec());
        Assert.assertEquals(4, throttlingOptions.getAggressiveThrottlingQpsRatio(), 0.0);
        Assert.assertEquals(4, throttlingOptions.getAggressiveThrottlingLatencyRatio(), 0.0);
        Assert.assertEquals("system.*|pingless", throttlingOptions.getIgnoreKeyspacesRegex());
        Assert.assertEquals("", throttlingOptions.getHardBlockCoordReadsTablesRegex());
        Assert.assertEquals("", throttlingOptions.getHardBlockCoordWritesTablesRegex());
        Assert.assertEquals("", throttlingOptions.getHardBlockReplicaReadsTablesRegex());
        Assert.assertEquals("", throttlingOptions.getHardBlockReplicaWritesTablesRegex());
    }
}
