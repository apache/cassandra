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

package org.apache.cassandra.repair.messages;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.repair.RepairParallelism;
import org.apache.cassandra.streaming.PreviewKind;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.matchers.JUnitMatchers.containsString;

public class RepairOptionTest
{
    @Test
    public void testParseOptions()
    {
        IPartitioner partitioner = Murmur3Partitioner.instance;
        Token.TokenFactory tokenFactory = partitioner.getTokenFactory();

        // parse with empty options
        RepairOption option = RepairOption.parse(new HashMap<String, String>(), partitioner);
        assertTrue(option.getParallelism() == RepairParallelism.SEQUENTIAL);

        assertFalse(option.isPrimaryRange());
        assertFalse(option.isIncremental());

        // parse everything except hosts (hosts cannot be combined with data centers)
        Map<String, String> options = new HashMap<>();
        options.put(RepairOption.PARALLELISM_KEY, "parallel");
        options.put(RepairOption.PRIMARY_RANGE_KEY, "false");
        options.put(RepairOption.INCREMENTAL_KEY, "false");
        options.put(RepairOption.RANGES_KEY, "0:10,11:20,21:30");
        options.put(RepairOption.COLUMNFAMILIES_KEY, "cf1,cf2,cf3");
        options.put(RepairOption.DATACENTERS_KEY, "dc1,dc2,dc3");

        option = RepairOption.parse(options, partitioner);
        assertTrue(option.getParallelism() == RepairParallelism.PARALLEL);
        assertFalse(option.isPrimaryRange());
        assertFalse(option.isIncremental());

        Set<Range<Token>> expectedRanges = new HashSet<>(3);
        expectedRanges.add(new Range<>(tokenFactory.fromString("0"), tokenFactory.fromString("10")));
        expectedRanges.add(new Range<>(tokenFactory.fromString("11"), tokenFactory.fromString("20")));
        expectedRanges.add(new Range<>(tokenFactory.fromString("21"), tokenFactory.fromString("30")));
        assertEquals(expectedRanges, option.getRanges());

        Set<String> expectedCFs = new HashSet<>(3);
        expectedCFs.add("cf1");
        expectedCFs.add("cf2");
        expectedCFs.add("cf3");
        assertEquals(expectedCFs, option.getColumnFamilies());

        Set<String> expectedDCs = new HashSet<>(3);
        expectedDCs.add("dc1");
        expectedDCs.add("dc2");
        expectedDCs.add("dc3");
        assertEquals(expectedDCs, option.getDataCenters());

        // expect an error when parsing with hosts as well
        options.put(RepairOption.HOSTS_KEY, "127.0.0.1,127.0.0.2,127.0.0.3");
        assertParseThrowsIllegalArgumentExceptionWithMessage(options, "Cannot combine -dc and -hosts options");

        // remove data centers to proceed with testing parsing hosts
        options.remove(RepairOption.DATACENTERS_KEY);
        option = RepairOption.parse(options, partitioner);

        Set<String> expectedHosts = new HashSet<>(3);
        expectedHosts.add("127.0.0.1");
        expectedHosts.add("127.0.0.2");
        expectedHosts.add("127.0.0.3");
        assertEquals(expectedHosts, option.getHosts());
    }

    @Test
    public void testPrWithLocalParseOptions()
    {
        DatabaseDescriptor.daemonInitialization();

        Map<String, String> options = new HashMap<>();
        options.put(RepairOption.PARALLELISM_KEY, "parallel");
        options.put(RepairOption.PRIMARY_RANGE_KEY, "true");
        options.put(RepairOption.INCREMENTAL_KEY, "false");
        options.put(RepairOption.COLUMNFAMILIES_KEY, "cf1,cf2,cf3");
        options.put(RepairOption.DATACENTERS_KEY, "datacenter1");

        RepairOption option = RepairOption.parse(options, Murmur3Partitioner.instance);
        assertTrue(option.isPrimaryRange());

        Set<String> expectedDCs = new HashSet<>(3);
        expectedDCs.add("datacenter1");
        assertEquals(expectedDCs, option.getDataCenters());
    }

    @Test
    public void testPullRepairParseOptions()
    {
        Map<String, String> options = new HashMap<>();

        options.put(RepairOption.PULL_REPAIR_KEY, "true");
        assertParseThrowsIllegalArgumentExceptionWithMessage(options, "Pull repair can only be performed between two hosts");

        options.put(RepairOption.HOSTS_KEY, "127.0.0.1,127.0.0.2,127.0.0.3");
        assertParseThrowsIllegalArgumentExceptionWithMessage(options, "Pull repair can only be performed between two hosts");

        options.put(RepairOption.HOSTS_KEY, "127.0.0.1,127.0.0.2");
        assertParseThrowsIllegalArgumentExceptionWithMessage(options, "Token ranges must be specified when performing pull repair");

        options.put(RepairOption.RANGES_KEY, "0:10");
        RepairOption option = RepairOption.parse(options, Murmur3Partitioner.instance);
        assertTrue(option.isPullRepair());
    }

    @Test
    public void testForceOption() throws Exception
    {
        RepairOption option;
        Map<String, String> options = new HashMap<>();

        // default value
        option = RepairOption.parse(options, Murmur3Partitioner.instance);
        assertFalse(option.isForcedRepair());

        // explicit true
        options.put(RepairOption.FORCE_REPAIR_KEY, "true");
        option = RepairOption.parse(options, Murmur3Partitioner.instance);
        assertTrue(option.isForcedRepair());

        // explicit false
        options.put(RepairOption.FORCE_REPAIR_KEY, "false");
        option = RepairOption.parse(options, Murmur3Partitioner.instance);
        assertFalse(option.isForcedRepair());
    }

    @Test
    public void testOptimiseStreams()
    {
        boolean optFull = DatabaseDescriptor.autoOptimiseFullRepairStreams();
        boolean optInc = DatabaseDescriptor.autoOptimiseIncRepairStreams();
        boolean optPreview = DatabaseDescriptor.autoOptimisePreviewRepairStreams();
        try
        {
            for (PreviewKind previewKind : PreviewKind.values())
                for (boolean inc : new boolean[] {true, false})
                    assertOptimise(previewKind, inc);
        }
        finally
        {
            setOptimise(optFull, optInc, optPreview);
        }
    }

    private void assertHelper(Map<String, String> options, boolean full, boolean inc, boolean preview, boolean expected)
    {
        setOptimise(full, inc, preview);
        assertEquals(expected, RepairOption.parse(options, Murmur3Partitioner.instance).optimiseStreams());
    }

    private void setOptimise(boolean full, boolean inc, boolean preview)
    {
        DatabaseDescriptor.setAutoOptimiseFullRepairStreams(full);
        DatabaseDescriptor.setAutoOptimiseIncRepairStreams(inc);
        DatabaseDescriptor.setAutoOptimisePreviewRepairStreams(preview);
    }

    private void assertOptimise(PreviewKind previewKind, boolean incremental)
    {
        Map<String, String> options = new HashMap<>();
        options.put(RepairOption.PREVIEW, previewKind.toString());
        options.put(RepairOption.INCREMENTAL_KEY, Boolean.toString(incremental));
        for (boolean a : new boolean[]{ true, false })
        {
            for (boolean b : new boolean[]{ true, false })
            {
                if (previewKind.isPreview())
                {
                    assertHelper(options, a, b, true, true);
                    assertHelper(options, a, b, false, false);
                }
                else if (incremental)
                {
                    assertHelper(options, a, true, b, true);
                    assertHelper(options, a, false, b, false);
                }
                else
                {
                    assertHelper(options, true, a, b, true);
                    assertHelper(options, false, a, b, false);
                }
            }
        }
    }

    private void assertParseThrowsIllegalArgumentExceptionWithMessage(Map<String, String> optionsToParse, String expectedErrorMessage)
    {
        try
        {
            RepairOption.parse(optionsToParse, Murmur3Partitioner.instance);
            fail(String.format("Expected RepairOption.parse() to throw an IllegalArgumentException containing the message '%s'", expectedErrorMessage));
        }
        catch (IllegalArgumentException ex)
        {
            assertThat(ex.getMessage(), containsString(expectedErrorMessage));
        }
    }
}
