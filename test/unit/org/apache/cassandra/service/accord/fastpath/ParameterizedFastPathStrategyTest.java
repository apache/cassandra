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

package org.apache.cassandra.service.accord.fastpath;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.common.collect.ImmutableMap;
import org.junit.Assert;
import org.junit.Test;

import accord.local.Node;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.service.accord.fastpath.ParameterizedFastPathStrategy.WeightedDc;

import static java.util.Collections.emptySet;
import static org.apache.cassandra.service.accord.AccordTestUtils.id;
import static org.apache.cassandra.service.accord.AccordTestUtils.idList;
import static org.apache.cassandra.service.accord.AccordTestUtils.idSet;
import static org.apache.cassandra.service.accord.fastpath.FastPathParsingTest.pfs;
import static org.junit.Assert.assertEquals;

public class ParameterizedFastPathStrategyTest
{
    private static final List<Node.Id> NODES = idList(1, 2, 3, 4, 5, 6);
    private static final Map<Node.Id, String> DCS_2;
    private static final Map<Node.Id, String> DCS_3;

    static
    {
        ImmutableMap.Builder<Node.Id, String> builder = ImmutableMap.builder();
        builder.put(id(1), "DC1");
        builder.put(id(2), "DC1");
        builder.put(id(3), "DC1");
        builder.put(id(4), "DC2");
        builder.put(id(5), "DC2");
        builder.put(id(6), "DC2");
        DCS_2 = builder.build();

        builder = ImmutableMap.builder();
        builder.put(id(1), "DC1");
        builder.put(id(2), "DC1");
        builder.put(id(3), "DC2");
        builder.put(id(4), "DC2");
        builder.put(id(5), "DC3");
        builder.put(id(6), "DC3");
        DCS_3 = builder.build();
    }

    @Test
    public void noDCPreference()
    {
        assertEquals(idSet(1, 2, 3, 4, 5, 6), pfs(6).calculateFastPath(NODES, emptySet(), DCS_2));
        assertEquals(idSet(1, 2, 3, 4, 5), pfs(5).calculateFastPath(NODES, emptySet(), DCS_2));
        assertEquals(idSet(1, 2, 3, 4), pfs(4).calculateFastPath(NODES, emptySet(), DCS_2));
        assertEquals(idSet(1, 2, 3, 4), pfs(3).calculateFastPath(NODES, emptySet(), DCS_2));
    }

    @Test
    public void noDCPreferenceUnavailables()
    {
        assertEquals(idSet(1, 2, 3, 4, 5, 6), pfs(6).calculateFastPath(NODES, idSet(4), DCS_2));
        assertEquals(idSet(1, 2, 3, 4, 5), pfs(5).calculateFastPath(NODES, idSet(1, 6), DCS_2));
        assertEquals(idSet(2, 3, 4, 5), pfs(4).calculateFastPath(NODES, idSet(1, 6), DCS_2));
    }


    @Test
    public void dcPreference()
    {
        assertEquals(idSet(1, 2, 3, 4, 5, 6), pfs(6, "DC1", "DC2").calculateFastPath(NODES, idSet(), DCS_3));
        assertEquals(idSet(1, 2, 3, 4), pfs(4, "DC1", "DC2").calculateFastPath(NODES, idSet(), DCS_3));
        assertEquals(idSet(1, 2, 5, 6), pfs(4, "DC1", "DC3").calculateFastPath(NODES, idSet(), DCS_3));
    }

    @Test
    public void dcPreferenceUnavailables()
    {
        assertEquals(idSet(1, 2, 3, 4, 5), pfs(5, "DC1", "DC2").calculateFastPath(NODES, idSet(2, 4, 6), DCS_3));
        assertEquals(idSet(1, 2, 3, 5, 6), pfs(5, "DC1", "DC3").calculateFastPath(NODES, idSet(2, 4, 6), DCS_3));
        assertEquals(idSet(1, 3, 4, 5, 6), pfs(5, "DC2", "DC3").calculateFastPath(NODES, idSet(2, 4, 6), DCS_3));
    }

    private static WeightedDc wdc(String dc, int weight, boolean auto)
    {
        return new WeightedDc(dc, weight, auto);
    }

    private static void assertCFE(int size, String... dcs)
    {
        try
        {
            pfs(size, dcs);
            Assert.fail("expected ConfigurationException");
        }
        catch (ConfigurationException ex)
        {
            // expected
        }
    }

    private static void assertPFS(ParameterizedFastPathStrategy actual, int size, WeightedDc... dcs)
    {
        Map<String, WeightedDc> dcMap = new HashMap<>();
        for (WeightedDc dc : dcs)
        {
            Assert.assertFalse(dcMap.containsKey(dc.name));
            dcMap.put(dc.name, dc);
        }
        ParameterizedFastPathStrategy expected = new ParameterizedFastPathStrategy(size, ImmutableMap.copyOf(dcMap));
        Assert.assertEquals(expected, actual);
    }


    @Test
    public void dcParsingTest()
    {
        assertCFE(5, "DC1", "DC2:1");
        assertCFE(5, "DC1:-1", "DC2:1");
        assertCFE(5, "DC1", "DC1");
    }

    @Test
    public void listParsingTest()
    {
        assertPFS(pfs(4, "DC1", "DC2", "DC3"), 4, wdc("DC1", 0, true), wdc("DC2", 1, true), wdc("DC3", 2, true));
        assertPFS(pfs(4, "DC2", "DC3", "DC1"), 4, wdc("DC2", 0, true), wdc("DC3", 1, true), wdc("DC1", 2, true));
    }

    @Test
    public void weightParsingTest()
    {
        assertPFS(pfs(4, "DC1:0", "DC2:0", "DC3:1"), 4, wdc("DC1", 0, false), wdc("DC2", 0, false), wdc("DC3", 1, false));
        assertPFS(pfs(4, "DC2:100", "DC3:200", "DC1:300"), 4, wdc("DC2", 100, false), wdc("DC3", 200, false), wdc("DC1", 300, false));
    }
}
