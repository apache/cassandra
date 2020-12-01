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

package org.apache.cassandra.repair.asymmetric;

import java.util.Set;

import com.google.common.collect.Sets;
import org.junit.Test;

import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.apache.cassandra.repair.asymmetric.ReduceHelperTest.range;

public class RangeDenormalizerTest
{
    @Test
    public void testDenormalize()
    {
        // test when the new incoming range is fully contained within an existing incoming range
        StreamFromOptions dummy = new StreamFromOptions(null, range(0, 100));
        RangeMap<StreamFromOptions> incoming = new RangeMap<>();
        incoming.put(range(0, 100), dummy);
        Set<Range<Token>> newInput = RangeDenormalizer.denormalize(range(30, 40), incoming);
        assertEquals(3, incoming.size());
        assertTrue(incoming.containsKey(range(0, 30)));
        assertTrue(incoming.containsKey(range(30, 40)));
        assertTrue(incoming.containsKey(range(40, 100)));
        assertEquals(1, newInput.size());
        assertTrue(newInput.contains(range(30, 40)));
    }

    @Test
    public void testDenormalize2()
    {
        // test when the new incoming range fully contains an existing incoming range
        StreamFromOptions dummy = new StreamFromOptions(null, range(40, 50));
        RangeMap<StreamFromOptions> incoming = new RangeMap<>();
        incoming.put(range(40, 50), dummy);
        Set<Range<Token>> newInput = RangeDenormalizer.denormalize(range(0, 100), incoming);
        assertEquals(1, incoming.size());
        assertTrue(incoming.containsKey(range(40, 50)));
        assertEquals(3, newInput.size());
        assertTrue(newInput.contains(range(0, 40)));
        assertTrue(newInput.contains(range(40, 50)));
        assertTrue(newInput.contains(range(50, 100)));
    }

    @Test
    public void testDenormalize3()
    {
        // test when there are multiple existing incoming ranges and the new incoming overlaps some and contains some
        StreamFromOptions dummy = new StreamFromOptions(null, range(0, 100));
        StreamFromOptions dummy2 = new StreamFromOptions(null, range(200, 300));
        StreamFromOptions dummy3 = new StreamFromOptions(null, range(500, 600));
        RangeMap<StreamFromOptions> incoming = new RangeMap<>();
        incoming.put(range(0, 100), dummy);
        incoming.put(range(200, 300), dummy2);
        incoming.put(range(500, 600), dummy3);
        Set<Range<Token>> expectedNewInput = Sets.newHashSet(range(50, 100), range(100, 200), range(200, 300), range(300, 350));
        Set<Range<Token>> expectedIncomingKeys = Sets.newHashSet(range(0, 50), range(50, 100), range(200, 300), range(500, 600));
        Set<Range<Token>> newInput = RangeDenormalizer.denormalize(range(50, 350), incoming);
        assertEquals(expectedNewInput, newInput);
        assertEquals(expectedIncomingKeys, incoming.keySet());
    }
}
