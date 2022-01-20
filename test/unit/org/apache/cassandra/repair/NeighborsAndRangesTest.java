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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.junit.Assert;
import org.junit.Test;

import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.repair.messages.RepairOption;
import org.apache.cassandra.utils.FBUtilities;

import static org.apache.cassandra.repair.RepairRunnable.NeighborsAndRanges;
import static org.apache.cassandra.repair.RepairRunnable.createNeighbordAndRangesForOfflineService;
import static org.apache.cassandra.repair.messages.RepairOption.parse;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class NeighborsAndRangesTest extends AbstractRepairTest
{
    @Test
    public void testCreateNeighbordAndRangesForOfflineService()
    {
        Map<String, String> options = new HashMap<>();

        // no hosts
        assertThatThrownBy(() -> create(options)).hasMessageContaining("There should be at least 1 host");
        options.put(RepairOption.HOSTS_KEY, FBUtilities.getJustBroadcastAddress().getHostAddress());

        // no ranges
        assertThatThrownBy(() -> create(options)).hasMessageContaining("Token ranges must be specified");
        options.put(RepairOption.RANGES_KEY, "0:10,11:20,21:30");

        // no neighor, because the only host is local
        assertThatThrownBy(() -> create(options)).hasMessageContaining("There should be at least 1 neighbor");

        // with proper neighbors
        options.put(RepairOption.HOSTS_KEY, "127.0.99.1,127.0.99.2," + FBUtilities.getJustBroadcastAddress().getHostAddress());
        RepairRunnable.NeighborsAndRanges neighborsAndRanges = create(options);

        assertThat(neighborsAndRanges.shouldExcludeDeadParticipants).isFalse();
        assertThat(neighborsAndRanges.participants).hasSize(2); // excluded local host
        assertThat(neighborsAndRanges.commonRanges).hasSize(1); // all in one common range
        assertThat(neighborsAndRanges.filterCommonRanges("ks", new String[] { "cf"})).isSameAs(neighborsAndRanges.commonRanges);
    }

    private static RepairRunnable.NeighborsAndRanges create(Map<String, String> options)
    {
        RepairOption option = parse(options, Murmur3Partitioner.instance);
        return createNeighbordAndRangesForOfflineService(option);
    }

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
