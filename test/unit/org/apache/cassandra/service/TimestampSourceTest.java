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

package org.apache.cassandra.service;

import org.junit.Test;

import static org.apache.cassandra.service.TimestampSource.merge;
import static org.junit.Assert.assertEquals;

public class TimestampSourceTest
{
    @Test
    public void testMergeSameValues()
    {
        for (var v : TimestampSource.values())
            assertEquals(v, merge(v, v));
    }


    @Test
    public void testMergeAssociativity()
    {
        for (var left : TimestampSource.values())
        {
            for (var right : TimestampSource.values())
            {
                assertEquals(merge(left, right), merge(right, left));
            }
        }
    }

    @Test
    public void testMergeIdentityWithUnknown()
    {
        // unknown should act as identity element
        assertEquals(TimestampSource.server, merge(TimestampSource.server, TimestampSource.unknown));
        assertEquals(TimestampSource.using, merge(TimestampSource.using, TimestampSource.unknown));
        assertEquals(TimestampSource.mixed, merge(TimestampSource.mixed, TimestampSource.unknown));
    }

    @Test
    public void testMergeAbsorbingElementWithMixed()
    {
        for (var v : TimestampSource.values())
            assertEquals(TimestampSource.mixed, merge(TimestampSource.mixed, v));
        for (var v : TimestampSource.values())
            assertEquals(TimestampSource.mixed, merge(v, TimestampSource.mixed));
    }

    @Test
    public void testMergeServerWithOthers()
    {
        assertEquals(TimestampSource.server, merge(TimestampSource.server, TimestampSource.unknown));
        assertEquals(TimestampSource.mixed, merge(TimestampSource.server, TimestampSource.using));
        assertEquals(TimestampSource.mixed, merge(TimestampSource.server, TimestampSource.mixed));
    }

    @Test
    public void testMergeUsingWithOthers()
    {
        assertEquals(TimestampSource.using, merge(TimestampSource.using, TimestampSource.unknown));
        assertEquals(TimestampSource.mixed, merge(TimestampSource.using, TimestampSource.server));
        assertEquals(TimestampSource.mixed, merge(TimestampSource.using, TimestampSource.mixed));
    }
}