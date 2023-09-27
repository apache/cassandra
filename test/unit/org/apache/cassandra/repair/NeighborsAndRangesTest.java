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

package org.apache.cassandra.repair;

import java.util.Collections;
import java.util.List;
import java.util.Set;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.junit.Assert;
import org.junit.Test;

import org.apache.cassandra.locator.InetAddressAndPort;

import static org.apache.cassandra.repair.RepairCoordinator.NeighborsAndRanges;

public class NeighborsAndRangesTest extends AbstractRepairTest
{
    /**
     * For non-forced repairs, common ranges should be passed through as-is
     */
    @Test
    public void filterCommonIncrementalRangesNotForced()
    {
        CommonRange cr = new CommonRange(PARTICIPANTS, Collections.emptySet(), ALL_RANGES);
        NeighborsAndRanges nr = new NeighborsAndRanges(false, PARTICIPANTS, Collections.singletonList(cr));
        List<CommonRange> expected = Lists.newArrayList(cr);
        List<CommonRange> actual = nr.filterCommonRanges(null, null);

        Assert.assertEquals(expected, actual);
    }

    @Test
    public void forceFilterCommonIncrementalRanges()
    {
        CommonRange cr1 = new CommonRange(Sets.newHashSet(PARTICIPANT1, PARTICIPANT2), Collections.emptySet(), Sets.newHashSet(RANGE1));
        CommonRange cr2 = new CommonRange(Sets.newHashSet(PARTICIPANT1, PARTICIPANT2, PARTICIPANT3), Collections.emptySet(), Sets.newHashSet(RANGE3));
        CommonRange cr3 = new CommonRange(Sets.newHashSet(PARTICIPANT2, PARTICIPANT3), Collections.emptySet(), Sets.newHashSet(RANGE2));
        Set<InetAddressAndPort> liveEndpoints = Sets.newHashSet(PARTICIPANT2, PARTICIPANT3); // PARTICIPANT1 is excluded
        List<CommonRange> initial = Lists.newArrayList(cr1, cr2, cr3);
        List<CommonRange> expected = Lists.newArrayList(new CommonRange(Sets.newHashSet(PARTICIPANT2), Collections.emptySet(), Sets.newHashSet(RANGE1), true),
                                                        new CommonRange(Sets.newHashSet(PARTICIPANT2, PARTICIPANT3), Collections.emptySet(), Sets.newHashSet(RANGE3), true),
                                                        new CommonRange(Sets.newHashSet(PARTICIPANT2, PARTICIPANT3), Collections.emptySet(), Sets.newHashSet(RANGE2), false));

        NeighborsAndRanges nr = new NeighborsAndRanges(true, liveEndpoints, initial);
        List<CommonRange> actual = nr.filterCommonRanges(null, null);

        Assert.assertEquals(expected, actual);
    }
}
