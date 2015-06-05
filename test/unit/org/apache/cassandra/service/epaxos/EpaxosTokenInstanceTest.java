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

package org.apache.cassandra.service.epaxos;

import java.util.Collections;
import java.util.UUID;

import org.junit.Assert;
import org.junit.Test;

import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.service.epaxos.exceptions.InvalidInstanceStateChange;

public class EpaxosTokenInstanceTest extends AbstractEpaxosTest
{

    private static void assertMerge(Range<Token> expected, Range<Token> range1, Range<Token> range2)
    {
        Assert.assertEquals(expected, TokenInstance.mergeRanges(range1, range2));
        Assert.assertEquals(expected, TokenInstance.mergeRanges(range2, range1));
    }

    /**
     * Checks that the range merging method works properly
     */
    @Test
    public void mergeRange()
    {
        Range<Token> range1;
        Range<Token> range2;
        Range<Token> expected;

        // normal
        range1 = range(token(100), token(200));
        range2 = range(token(150), token(250));
        expected = range(token(100), token(250));
        assertMerge(expected, range1, range2);

        // containing
        range1 = range(token(100), token(200));
        range2 = range(token(50), token(250));
        expected = range(token(50), token(250));
        assertMerge(expected, range1, range2);

        // one wrapped
        range1 = range(token(500), token(200));
        range2 = range(token(50), token(250));
        expected = range(token(500), token(250));
        assertMerge(expected, range1, range2);

        // both wrapped
        range1 = range(token(500), token(200));
        range2 = range(token(600), token(300));
        expected = range(token(500), token(300));
        assertMerge(expected, range1, range2);

        // wrapped containing
        range1 = range(token(500), token(200));
        range2 = range(token(600), token(100));
        expected = range(token(500), token(200));
        assertMerge(expected, range1, range2);
    }

    @Test
    public void disagreeingRanges() throws InvalidInstanceStateChange
    {
        TokenInstance instance = new TokenInstance(LOCALHOST, CFID, TOKEN0, range(1, 2), DEFAULT_SCOPE);
        instance.preaccept(Collections.<UUID>emptySet(), Collections.<UUID>emptySet());
        Assert.assertTrue(instance.getLeaderAttrsMatch());

        // matching
        instance.setSplitRange(range(100, 200));
        instance.mergeLocalSplitRange(range(100, 200));
        Assert.assertEquals(range(100, 200), instance.getSplitRange());
        Assert.assertTrue(instance.getLeaderAttrsMatch());

        // intersecting
        instance.setSplitRange(range(100, 200));
        instance.mergeLocalSplitRange(range(150, 250));
        Assert.assertEquals(range(100, 250), instance.getSplitRange());
        Assert.assertFalse(instance.getLeaderAttrsMatch());

        // inclusive 1
        instance.setSplitRange(range(100, 200));
        instance.mergeLocalSplitRange(range(125, 175));
        Assert.assertEquals(range(100, 200), instance.getSplitRange());
        Assert.assertFalse(instance.getLeaderAttrsMatch());

        // inclusive 2
        instance.setSplitRange(range(125, 175));
        instance.mergeLocalSplitRange(range(100, 200));
        Assert.assertEquals(range(100, 200), instance.getSplitRange());
        Assert.assertFalse(instance.getLeaderAttrsMatch());

        // disjoint
        try
        {
            instance.setSplitRange(range(0, 50));
            instance.mergeLocalSplitRange(range(51, 100));
            Assert.fail("Disjoint ranges should not merge");
        }
        catch (AssertionError e)
        {
            // expected
        }

    }
}
