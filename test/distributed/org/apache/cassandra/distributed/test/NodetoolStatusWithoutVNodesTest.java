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

import java.util.List;

import org.junit.Test;

import static org.apache.cassandra.tools.nodetool.Status.SortOrder.asc;
import static org.apache.cassandra.tools.nodetool.Status.SortOrder.desc;
import static org.junit.Assert.assertEquals;

public class NodetoolStatusWithoutVNodesTest extends AbstractNodetoolStatusTest
{
    @Test
    public void testSortById()
    {
        sortByIdInternal(new String[]{ "status", "-s", "id" }, asc, 4);
        sortByIdInternal(new String[]{ "status", "-s", "id", "-o", "asc" }, asc, 4);
        sortByIdInternal(new String[]{ "status", "-s", "id", "-o", "desc" }, desc, 4);
    }

    @Test
    public void testSortByToken()
    {
        sortByTokenInternal(new String[]{ "status", "-s", "token" }, asc);
        sortByTokenInternal(new String[]{ "status", "-s", "token", "-o", "asc" }, asc);
        sortByTokenInternal(new String[]{ "status", "-s", "token", "-o", "desc" }, desc);
    }

    @Test
    public void testSortByRack()
    {
        compareByRacksInternal(new String[]{ "status", "-s", "rack" }, asc, this::tokenComparator);
        compareByRacksInternal(new String[]{ "status", "-s", "rack", "-o", "asc" }, asc, this::tokenComparator);
        compareByRacksInternal(new String[]{ "status", "-s", "rack", "-o", "desc" }, desc, this::tokenComparator);
    }

    private void tokenComparator(String output, int expectedComparision)
    {
        List<String> tokens = extractColumn(output, 5);
        assertEquals(expectedComparision, Long.compare(Long.parseLong(tokens.get(0)), Long.parseLong(tokens.get(1))));
        assertEquals(expectedComparision, Long.compare(Long.parseLong(tokens.get(2)), Long.parseLong(tokens.get(3))));
    }
}
