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

package org.apache.cassandra.dht.tokenallocator;

import java.util.Collection;
import java.util.List;

import com.google.common.collect.Lists;

import org.apache.cassandra.dht.Token;
import org.apache.cassandra.utils.OutputHandler;
import org.assertj.core.api.Assertions;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class OfflineTokenAllocatorTestUtils
{
    static final OutputHandler FAIL_ON_WARN_OUTPUT = new SystemOutputImpl();

    static void assertTokensAndNodeCount(int numTokens, int nodeCount, List<OfflineTokenAllocator.FakeNode> nodes)
    {
        assertEquals(nodeCount, nodes.size());
        Collection<Token> allTokens = Lists.newArrayList();
        for (OfflineTokenAllocator.FakeNode node : nodes)
        {
            Assertions.assertThat(node.tokens()).hasSize(numTokens);
            Assertions.assertThat(allTokens).doesNotContainAnyElementsOf(node.tokens());
            allTokens.addAll(node.tokens());
        }
    }

    static int[] makeRackCountArray(int nodes, int racks)
    {
        assert nodes > 0;
        assert racks > 0;
        // Distribute nodes among the racks in round-robin fashion in the order the user is supposed to start them.
        int[] rackCounts = new int[racks];
        int rack = 0;
        for (int node = 0; node < nodes; node++)
        {
            rackCounts[rack]++;
            if (++rack == racks)
                rack = 0;
        }
        return rackCounts;
    }

    static class SystemOutputImpl extends OutputHandler.SystemOutput
    {
        final int rf;
        final int racks;

        SystemOutputImpl()
        {
            super(true, true);
            rf = racks = 1;
        }

        SystemOutputImpl(int rf, int racks)
        {
            super(true, true);
            this.rf = rf;
            this.racks = racks;
        }

        @Override
        public void warn(String msg)
        {
            // We can only guarantee that ownership stdev won't increase above the warn threshold for racks==1 or racks==rf
            if (racks == 1 || racks == rf)
                fail(msg);
            else
                super.warn(msg);
        }

        @Override
        public void warn(Throwable th, String msg)
        {
            // We can only guarantee that ownership stdev won't increase above the warn threshold for racks==1 or racks==rf
            if (racks == 1 || racks == rf)
                fail(msg);
            else
                super.warn(th, msg);
        }
    }
}