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

package org.apache.cassandra.distributed.test;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.function.BiConsumer;
import java.util.regex.Pattern;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.api.NodeToolResult;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.tools.nodetool.Status;

import static java.lang.Double.parseDouble;
import static java.util.stream.Collectors.toList;
import static org.apache.cassandra.tools.nodetool.Status.SortOrder.asc;
import static org.apache.cassandra.tools.nodetool.Status.SortOrder.desc;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@Ignore
public abstract class AbstractNodetoolStatusTest extends TestBaseImpl
{
    protected static boolean useVNodes = false;

    protected static Cluster CLUSTER;
    protected static IInvokableInstance NODE_1;

    // Split on 2 or more spaces
    private static final Pattern PATTERN = Pattern.compile("\\s{2,}");

    @BeforeClass
    public static void before() throws IOException
    {
        Cluster.Builder builder = Cluster.build();

        if (useVNodes)
            builder.withVNodes().withTokenCount(16);
        else
            builder.withoutVNodes();

        CLUSTER = init(builder.withRacks(1, 2, 2).start());

        NODE_1 = CLUSTER.get(1);
    }

    @AfterClass
    public static void after()
    {
        if (CLUSTER != null)
            CLUSTER.close();
    }

    @Test
    public void testCommands()
    {
        // test all legal combinations
        for (Status.SortBy sortBy : Status.SortBy.values())
        {
            // sort by tokens when on vnodes is illegal combination
            if (sortBy == Status.SortBy.token && useVNodes)
                continue;

            // will be tested separately below
            if (sortBy == Status.SortBy.host)
                continue;

            for (Status.SortOrder sortOrder : Status.SortOrder.values())
            {
                // test with and without ordering
                assertEquals(0, NODE_1.nodetool("status", "-s", sortBy.name(), "-o", sortOrder.name()));
                assertEquals(0, NODE_1.nodetool("status", "-s", sortBy.name()));
            }
        }

        // test no sorting and no ordering
        assertEquals(0, NODE_1.nodetool("status"));

        // test illegal sort by or order
        NodeToolResult invalidSortBy = NODE_1.nodetoolResult("status", "--sort", "not_an_option");
        invalidSortBy.asserts().failure();
        Assert.assertTrue(invalidSortBy.getStderr().contains("Illegal value for -s/--sort used: 'not_an_option'. Supported values are state, ip, host, load, owns, id, token, rack."));

        NodeToolResult invalidSortOrder = NODE_1.nodetoolResult("status", "--sort", "ip", "-o", "not_an_order");
        invalidSortOrder.asserts().failure();
        Assert.assertTrue(invalidSortOrder.getStderr().contains("Illegal value for -o/--order used: 'not_an_order'. Supported values are asc, desc."));

        // test order alone
        assertEquals(0, NODE_1.nodetool("status", "-o", "asc"));
        assertEquals(0, NODE_1.nodetool("status", "-o", "desc"));

        // test what happens when we use vnodes, but we want to sort by token
        if (useVNodes)
        {
            NodeToolResult result = NODE_1.nodetoolResult("status", "-s", "token", "-o", "desc");
            result.asserts().failure();
            assertTrue(result.getStderr().contains("Error: Can not sort by token when there is not token per node."));
        }

        // test what happens when we want to sort by host, but we do not resolve ips

        NodeToolResult hostResult = NODE_1.nodetoolResult("status", "-s", "host");
        hostResult.asserts().failure();
        assertTrue(hostResult.getStderr().contains("Error: Can not sort by host when there is not -r/--resolve-ip flag used."));

        // test that resolving ips together with host sorting works
        // we are not testing actual hosts ordering because calling nodetool status -r
        // on addresses from 127.0.0.1-4 and having them being resolved to some names
        // means that we would need to have a resolver set up to return these names
        // which is quite impractical
        hostResult = NODE_1.nodetoolResult("status", "-s", "host", "-r");
        hostResult.asserts().success();

        hostResult = NODE_1.nodetoolResult("status", "-s", "host", "-r", "-o", "desc");
        hostResult.asserts().success();

        hostResult = NODE_1.nodetoolResult("status", "-s", "host", "-r", "-o", "asc");
        hostResult.asserts().success();
    }

    @Test
    public void testSortByIp()
    {
        assertOrdering(extractColumn(getOutput("status", "-s", "ip"), 1).stream().map(this::parseInetAddress).collect(toList()), asc);
        assertOrdering(extractColumn(getOutput("status", "-s", "ip", "-o", "asc"), 1).stream().map(this::parseInetAddress).collect(toList()), asc);
        assertOrdering(extractColumn(getOutput("status", "-s", "ip", "-o", "desc"), 1).stream().map(this::parseInetAddress).collect(toList()), desc);
    }

    @Test
    public void testSortByLoad()
    {
        assertOrdering(extractLoads(getOutput("status", "-s", "load")), desc);
        assertOrdering(extractLoads(getOutput("status", "-s", "load", "-o", "asc")), asc);
        assertOrdering(extractLoads(getOutput("status", "-s", "load", "-o", "desc")), desc);
    }

    @Test
    public void testSortByOwns()
    {
        assertOrdering(extractColumn(getOutput("status", "-s", "owns"), 3).stream().map(s -> parseDouble(s.replace("%", ""))).collect(toList()), desc);
        assertOrdering(extractColumn(getOutput("status", "-s", "owns", "-o", "asc"), 3).stream().map(s -> parseDouble(s.replace("%", ""))).collect(toList()), asc);
        assertOrdering(extractColumn(getOutput("status", "-s", "owns", "-o", "desc"), 3).stream().map(s -> parseDouble(s.replace("%", ""))).collect(toList()), desc);
    }

    @Test
    public void testSortByState()
    {
        assertOrdering(extractColumn(getOutput("status", "-s", "state"), 0), desc);
        assertOrdering(extractColumn(getOutput("status", "-s", "state", "-o", "asc"), 0), asc);
        assertOrdering(extractColumn(getOutput("status", "-s", "state", "-o", "desc"), 0), desc);
    }

    protected String getOutput(String... args)
    {
        NodeToolResult result = NODE_1.nodetoolResult(args);
        result.asserts().success();
        return result.getStdout();
    }

    private InetAddressAndPort parseInetAddress(String ip)
    {
        try
        {
            return InetAddressAndPort.getByName(ip);
        }
        catch (UnknownHostException e)
        {
            throw new IllegalArgumentException("Invalid IP address", e);
        }
    }

    protected List<String> extractColumn(String output, int... columnIndex)
    {
        List<String> columnValues = new ArrayList<>();
        String[] lines = output.split("\n");

        // Skip the first five lines as headers
        int skippedLines = 0;
        for (String line : lines)
        {
            if (line.trim().isEmpty())
            {
                continue; // Skip separator lines and empty lines
            }

            // Skip the first five lines as they are headers
            if (skippedLines < 5)
            {
                skippedLines++;
                continue;
            }

            // Use regular expression to extract columns
            // Pattern will match any column with possible varying whitespace
            String[] columns = PATTERN.split(line.trim());  // Split on 2 or more spaces

            // Check if the line has enough columns (avoid index out of bounds errors)
            for (int i = 0; i < columnIndex.length; i++)
            {
                if (columns.length > columnIndex[i])
                {
                    columnValues.add(columns[columnIndex[i]].trim());
                }
            }
        }

        return columnValues;
    }

    private <T extends Comparable<T>> void assertOrdering(List<T> columnOfValues, Status.SortOrder order)
    {
        List<T> sorted = new ArrayList<>(columnOfValues);
        if (order == asc)
        {
            Collections.sort(sorted);
            assertEquals("Not sorted in ascending order", sorted, columnOfValues);
        }
        else
        {
            sorted.sort(Collections.reverseOrder());
            assertEquals("Not sorted in descending order", sorted, columnOfValues);
        }
    }

    protected List<UUID> parseUUIDs(List<String> stringUUIDs)
    {
        List<UUID> result = new ArrayList<>();
        for (String stringUUID : stringUUIDs)
            result.add(UUID.fromString(stringUUID));
        return result;
    }

    protected List<Long> parseLongs(List<String> stringLongs)
    {
        List<Long> result = new ArrayList<>();
        for (String stringLong : stringLongs)
            result.add(Long.parseLong(stringLong));
        return result;
    }

    private List<Long> extractLoads(String output)
    {
        // Extract the load values and handle '?' placement
        List<String> loads = extractColumn(output, 2);
        return loads.stream()
                    .map(load -> {
                        // If load contains '?', assign it a value that will be placed first
                        if (load.contains("?"))
                        {
                            return Long.MIN_VALUE;
                        }
                        return FileUtils.parseFileSize(load);
                    })
                    .collect(toList());
    }

    protected void compareByRacksInternal(String[] args, Status.SortOrder order, BiConsumer<String, Integer> biConsumer)
    {
        String output = getOutput(args);

        List<String> expectedRacks;
        if (order == Status.SortOrder.desc)
            expectedRacks = List.of("rack2", "rack2", "rack1", "rack1");
        else
            expectedRacks = List.of("rack1", "rack1", "rack2", "rack2");

        assertEquals("Rack values are not sorted in " + order.name() + " order",
                     expectedRacks,
                     extractColumn(output, 6));

        int expectedComparision = order == Status.SortOrder.desc ? 1 : -1;

        biConsumer.accept(output, expectedComparision);
    }


    protected void sortByIdInternal(String[] args, Status.SortOrder order, int index)
    {
        String output = getOutput(args);
        List<UUID> ids = parseUUIDs(extractColumn(output, index));
        List<UUID> sortedIds = new ArrayList<>(ids);
        if (order == asc)
        {
            Collections.sort(sortedIds);
            assertEquals("Ids are not sorted in ascending order", sortedIds, ids);
        }
        else
        {
            sortedIds.sort(Collections.reverseOrder());
            assertEquals("Ids are not sorted in descending order", sortedIds, ids);
        }
    }

    protected void sortByTokenInternal(String[] args, Status.SortOrder order)
    {
        String output = getOutput(args);
        List<Long> tokens = parseLongs(extractColumn(output, 5));
        List<Long> sortedTokens = new ArrayList<>(tokens);
        if (order == asc)
        {
            Collections.sort(sortedTokens);
            assertEquals("Tokens are not sorted in ascending order", sortedTokens, tokens);
        }
        else
        {
            sortedTokens.sort(Collections.reverseOrder());
            assertEquals("Tokens are not sorted in descending order", sortedTokens, tokens);
        }
    }
}
