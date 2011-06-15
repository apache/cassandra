/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.cassandra.dht;

import java.util.List;
import static java.util.Arrays.asList;

import org.junit.Test;

import static org.apache.cassandra.Util.range;
import static org.apache.cassandra.Util.bounds;

public class AbstractBoundsTest
{
    private void assertNormalize(List<? extends AbstractBounds> input, List<? extends AbstractBounds> expected)
    {
        List<AbstractBounds> result = AbstractBounds.normalize(input);
        assert result.equals(expected) : "Expecting " + expected + " but got " + result;
    }

    @Test
    public void testNormalizeNoop()
    {
        List<? extends AbstractBounds> l;

        l = asList(range("1", "3"), range("4", "5"));
        assert AbstractBounds.normalize(l).equals(l);

        l = asList(bounds("1", "3"), bounds("4", "5"));
        assertNormalize(l, l);
    }

    @Test
    public void testNormalizeSimpleOverlap()
    {
        List<? extends AbstractBounds> input, expected;

        input = asList(range("1", "4"), range("3", "5"));
        expected = asList(range("1", "5"));
        assertNormalize(input, expected);

        input = asList(range("1", "4"), range("1", "4"));
        expected = asList(range("1", "4"));
        assertNormalize(input, expected);

        input = asList(bounds("1", "4"), bounds("3", "5"));
        expected = asList(bounds("1", "5"));
        assertNormalize(input, expected);

        input = asList(bounds("1", "4"), bounds("1", "4"));
        expected = asList(bounds("1", "4"));
        assertNormalize(input, expected);

        input = asList(bounds("1", "1"), bounds("1", "1"));
        expected = asList(bounds("1", "1"));
        assertNormalize(input, expected);
    }

    @Test
    public void testNormalizeSort()
    {
        List<? extends AbstractBounds> input, expected;

        input = asList(range("4", "5"), range("1", "3"));
        expected = asList(range("1", "3"), range("4", "5"));
        assertNormalize(input, expected);

        input = asList(bounds("4", "5"), bounds("1", "3"));
        expected = asList(bounds("1", "3"), bounds("4", "5"));
        assertNormalize(input, expected);
    }

    @Test
    public void testNormalizeUnwrap()
    {
        List<? extends AbstractBounds> input, expected;

        input = asList(range("9", "2"));
        expected = asList(range("", "2"), range("9", ""));
        assertNormalize(input, expected);

        // Bounds cannot wrap
    }

    @Test
    public void testNormalizeComplex()
    {
        List<? extends AbstractBounds> input, expected;

        input = asList(range("8", "2"), range("7", "9"), range("4", "5"));
        expected = asList(range("", "2"), range("4", "5"), range("7", ""));
        assertNormalize(input, expected);

        input = asList(range("5", "9"), range("2", "5"));
        expected = asList(range("2", "9"));
        assertNormalize(input, expected);

        input = asList(range ("", "1"), range("9", "2"), range("4", "5"), range("", ""));
        expected = asList(range("", ""));
        assertNormalize(input, expected);

        input = asList(range ("", "1"), range("1", "4"), range("4", "5"), range("5", ""));
        expected = asList(range("", ""));
        assertNormalize(input, expected);

        // bounds
        input = asList(bounds("5", "9"), bounds("2", "5"));
        expected = asList(bounds("2", "9"));
        assertNormalize(input, expected);

        input = asList(bounds ("", "1"), bounds("", "9"), bounds("4", "5"), bounds("", ""));
        expected = asList(bounds("", ""));
        assertNormalize(input, expected);

        input = asList(bounds ("", "1"), bounds("1", "4"), bounds("4", "5"), bounds("5", ""));
        expected = asList(bounds("", ""));
        assertNormalize(input, expected);
    }
}
