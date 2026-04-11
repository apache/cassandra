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
import java.util.List;
import java.util.UUID;

import org.junit.BeforeClass;
import org.junit.Test;

import static org.apache.cassandra.tools.nodetool.Status.SortOrder.asc;
import static org.apache.cassandra.tools.nodetool.Status.SortOrder.desc;
import static org.junit.Assert.assertEquals;

public class NodeToolStatusWithVNodesTest extends AbstractNodetoolStatusTest
{
    @BeforeClass
    public static void before() throws IOException
    {
        useVNodes = true;
        AbstractNodetoolStatusTest.before();
    }

    @Test
    public void testSortById()
    {
        sortByIdInternal(new String[]{ "status", "-s", "id" }, asc, 5);
        sortByIdInternal(new String[]{ "status", "-s", "id", "-o", "asc" }, asc, 5);
        sortByIdInternal(new String[]{ "status", "-s", "id", "-o", "desc" }, desc, 5);
    }

    @Test
    public void testSortByRack()
    {
        compareByRacksInternal(new String[]{ "status", "-s", "rack" }, asc, this::idComparator);
        compareByRacksInternal(new String[]{ "status", "-s", "rack", "-o", "asc" }, asc, this::idComparator);
        compareByRacksInternal(new String[]{ "status", "-s", "rack", "-o", "desc" }, desc, this::idComparator);
    }

    private void idComparator(String output, int expectedComparision)
    {
        List<UUID> uuids = parseUUIDs(extractColumn(output, 5));
        assertEquals(expectedComparision, uuids.get(0).compareTo(uuids.get(1)));
        assertEquals(expectedComparision, uuids.get(2).compareTo(uuids.get(3)));
    }
}
