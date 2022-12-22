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

package org.apache.cassandra.dht;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.dht.RandomPartitioner.BigIntegerToken;
import org.apache.cassandra.index.sai.cql.DataModel;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class RangeIntersectsBoundsTest
{
    @BeforeClass
    public static void setupDD()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    @Test
    public void rangeIntersectsBounds() throws Exception
    {
        Range<Token> all = new Range<>(new BigIntegerToken("0"), new BigIntegerToken("0"));
        Range<Token> some = new Range<>(new BigIntegerToken("4"), new BigIntegerToken("8"));
        Range<Token> someWrapped = new Range<>(some.right, some.left);

        // Coda:
        // l - matches left token of some range
        // r - matches right token of some range
        // b - below left token of some range
        // a - above right token of some range
        // i - inside some range (above left and below right
        Bounds<Token> lr = new Bounds<>(new BigIntegerToken("4"), new BigIntegerToken("8"));
        Bounds<Token> br = new Bounds<>(new BigIntegerToken("3"), new BigIntegerToken("8"));
        Bounds<Token> bi = new Bounds<>(new BigIntegerToken("3"), new BigIntegerToken("7"));
        Bounds<Token> ba = new Bounds<>(new BigIntegerToken("3"), new BigIntegerToken("9"));
        Bounds<Token> la = new Bounds<>(new BigIntegerToken("4"), new BigIntegerToken("9"));
        Bounds<Token> li = new Bounds<>(new BigIntegerToken("4"), new BigIntegerToken("7"));
        Bounds<Token> ii = new Bounds<>(new BigIntegerToken("5"), new BigIntegerToken("7"));
        Bounds<Token> ir = new Bounds<>(new BigIntegerToken("5"), new BigIntegerToken("8"));
        Bounds<Token> bb = new Bounds<>(new BigIntegerToken("2"), new BigIntegerToken("3"));
        Bounds<Token> aa = new Bounds<>(new BigIntegerToken("9"), new BigIntegerToken("10"));
        Bounds<Token> bl = new Bounds<>(new BigIntegerToken("3"), new BigIntegerToken("4"));
        Bounds<Token> ra = new Bounds<>(new BigIntegerToken("8"), new BigIntegerToken("9"));

        assertTrue(all.intersects(lr));
        assertTrue(all.intersects(br));
        assertTrue(all.intersects(bi));
        assertTrue(all.intersects(ba));
        assertTrue(all.intersects(la));
        assertTrue(all.intersects(li));
        assertTrue(all.intersects(ii));
        assertTrue(all.intersects(ir));
        assertTrue(all.intersects(bb));
        assertTrue(all.intersects(aa));
        assertTrue(all.intersects(bl));
        assertTrue(all.intersects(ra));

        assertTrue(some.intersects(lr));
        assertTrue(some.intersects(br));
        assertTrue(some.intersects(bi));
        assertTrue(some.intersects(ba));
        assertTrue(some.intersects(la));
        assertTrue(some.intersects(li));
        assertTrue(some.intersects(ii));
        assertTrue(some.intersects(ir));
        assertFalse(some.intersects(bb));
        assertFalse(some.intersects(aa));
        assertFalse(some.intersects(bl));
        assertTrue(some.intersects(ra));

        assertTrue(someWrapped.intersects(lr));
        assertTrue(someWrapped.intersects(br));
        assertTrue(someWrapped.intersects(bi));
        assertTrue(someWrapped.intersects(ba));
        assertTrue(someWrapped.intersects(la));
        assertTrue(someWrapped.intersects(li));
        assertFalse(someWrapped.intersects(ii));
        assertFalse(someWrapped.intersects(ir));
        assertTrue(someWrapped.intersects(bb));
        assertTrue(someWrapped.intersects(aa));
        assertTrue(someWrapped.intersects(bl));
        assertTrue(someWrapped.intersects(ra));
    }

    @Test
    public void rangeIntersectsExcludingBounds() throws Exception
    {
        Range<Token> all = new Range<>(new BigIntegerToken("0"), new BigIntegerToken("0"));
        Range<Token> some = new Range<>(new BigIntegerToken("4"), new BigIntegerToken("8"));
        Range<Token> someWrapped = new Range<>(some.right, some.left);

        // Coda:
        // l - matches left token of some range
        // r - matches right token of some range
        // b - below left token of some range
        // a - above right token of some range
        // i - inside some range (above left and below right
        ExcludingBounds<Token> lr = new ExcludingBounds<>(new BigIntegerToken("4"), new BigIntegerToken("8"));
        ExcludingBounds<Token> br = new ExcludingBounds<>(new BigIntegerToken("3"), new BigIntegerToken("8"));
        ExcludingBounds<Token> bi = new ExcludingBounds<>(new BigIntegerToken("3"), new BigIntegerToken("7"));
        ExcludingBounds<Token> ba = new ExcludingBounds<>(new BigIntegerToken("3"), new BigIntegerToken("9"));
        ExcludingBounds<Token> la = new ExcludingBounds<>(new BigIntegerToken("4"), new BigIntegerToken("9"));
        ExcludingBounds<Token> li = new ExcludingBounds<>(new BigIntegerToken("4"), new BigIntegerToken("7"));
        ExcludingBounds<Token> ii = new ExcludingBounds<>(new BigIntegerToken("5"), new BigIntegerToken("7"));
        ExcludingBounds<Token> ir = new ExcludingBounds<>(new BigIntegerToken("5"), new BigIntegerToken("8"));
        ExcludingBounds<Token> bb = new ExcludingBounds<>(new BigIntegerToken("2"), new BigIntegerToken("3"));
        ExcludingBounds<Token> aa = new ExcludingBounds<>(new BigIntegerToken("9"), new BigIntegerToken("10"));
        ExcludingBounds<Token> bl = new ExcludingBounds<>(new BigIntegerToken("3"), new BigIntegerToken("4"));
        ExcludingBounds<Token> ra = new ExcludingBounds<>(new BigIntegerToken("8"), new BigIntegerToken("9"));

        assertTrue(all.intersects(lr));
        assertTrue(all.intersects(br));
        assertTrue(all.intersects(bi));
        assertTrue(all.intersects(ba));
        assertTrue(all.intersects(la));
        assertTrue(all.intersects(li));
        assertTrue(all.intersects(ii));
        assertTrue(all.intersects(ir));
        assertTrue(all.intersects(bb));
        assertTrue(all.intersects(aa));
        assertTrue(all.intersects(bl));
        assertTrue(all.intersects(ra));

        assertTrue(some.intersects(lr));
        assertTrue(some.intersects(br));
        assertTrue(some.intersects(bi));
        assertTrue(some.intersects(ba));
        assertTrue(some.intersects(la));
        assertTrue(some.intersects(li));
        assertTrue(some.intersects(ii));
        assertTrue(some.intersects(ir));
        assertFalse(some.intersects(bb));
        assertFalse(some.intersects(aa));
        assertFalse(some.intersects(bl));
        assertFalse(some.intersects(ra));

        assertTrue(someWrapped.intersects(lr));
        assertTrue(someWrapped.intersects(br));
        assertTrue(someWrapped.intersects(bi));
        assertTrue(someWrapped.intersects(ba));
        assertTrue(someWrapped.intersects(la));
        assertTrue(someWrapped.intersects(li));
        assertFalse(someWrapped.intersects(ii));
        assertFalse(someWrapped.intersects(ir));
        assertTrue(someWrapped.intersects(bb));
        assertTrue(someWrapped.intersects(aa));
        assertFalse(someWrapped.intersects(bl));
        assertFalse(someWrapped.intersects(ra));

        Range<Token> range = new Range<>(Murmur3Partitioner.MINIMUM, new Murmur3Partitioner.LongToken(-1));
        ExcludingBounds<Token> bounds = new ExcludingBounds<>(new Murmur3Partitioner.LongToken(-3248873570005575792L), Murmur3Partitioner.MINIMUM);

        assertTrue(range.intersects(bounds));

        range = new Range<>(new Murmur3Partitioner.LongToken(-1), Murmur3Partitioner.MINIMUM);

        assertTrue(range.intersects(bounds));

    }

    @Test
    public void rangeIntersectsIncludingExcludingBounds()
    {
        Range<Token> all = new Range<>(new BigIntegerToken("0"), new BigIntegerToken("0"));
        Range<Token> some = new Range<>(new BigIntegerToken("4"), new BigIntegerToken("8"));
        Range<Token> someWrapped = new Range<>(some.right, some.left);

        // Coda:
        // l - matches left token of some range
        // r - matches right token of some range
        // b - below left token of some range
        // a - above right token of some range
        // i - inside some range (above left and below right
        IncludingExcludingBounds<Token> lr = new IncludingExcludingBounds<>(new BigIntegerToken("4"), new BigIntegerToken("8"));
        IncludingExcludingBounds<Token> br = new IncludingExcludingBounds<>(new BigIntegerToken("3"), new BigIntegerToken("8"));
        IncludingExcludingBounds<Token> bi = new IncludingExcludingBounds<>(new BigIntegerToken("3"), new BigIntegerToken("7"));
        IncludingExcludingBounds<Token> ba = new IncludingExcludingBounds<>(new BigIntegerToken("3"), new BigIntegerToken("9"));
        IncludingExcludingBounds<Token> la = new IncludingExcludingBounds<>(new BigIntegerToken("4"), new BigIntegerToken("9"));
        IncludingExcludingBounds<Token> li = new IncludingExcludingBounds<>(new BigIntegerToken("4"), new BigIntegerToken("7"));
        IncludingExcludingBounds<Token> ii = new IncludingExcludingBounds<>(new BigIntegerToken("5"), new BigIntegerToken("7"));
        IncludingExcludingBounds<Token> ir = new IncludingExcludingBounds<>(new BigIntegerToken("5"), new BigIntegerToken("8"));
        IncludingExcludingBounds<Token> bb = new IncludingExcludingBounds<>(new BigIntegerToken("2"), new BigIntegerToken("3"));
        IncludingExcludingBounds<Token> aa = new IncludingExcludingBounds<>(new BigIntegerToken("9"), new BigIntegerToken("10"));
        IncludingExcludingBounds<Token> bl = new IncludingExcludingBounds<>(new BigIntegerToken("3"), new BigIntegerToken("4"));
        IncludingExcludingBounds<Token> ra = new IncludingExcludingBounds<>(new BigIntegerToken("8"), new BigIntegerToken("9"));

        assertTrue(all.intersects(lr));
        assertTrue(all.intersects(br));
        assertTrue(all.intersects(bi));
        assertTrue(all.intersects(ba));
        assertTrue(all.intersects(la));
        assertTrue(all.intersects(li));
        assertTrue(all.intersects(ii));
        assertTrue(all.intersects(ir));
        assertTrue(all.intersects(bb));
        assertTrue(all.intersects(aa));
        assertTrue(all.intersects(bl));
        assertTrue(all.intersects(ra));

        assertTrue(some.intersects(lr));
        assertTrue(some.intersects(br));
        assertTrue(some.intersects(bi));
        assertTrue(some.intersects(ba));
        assertTrue(some.intersects(la));
        assertTrue(some.intersects(li));
        assertTrue(some.intersects(ii));
        assertTrue(some.intersects(ir));
        assertFalse(some.intersects(bb));
        assertFalse(some.intersects(aa));
        assertFalse(some.intersects(bl));
        assertTrue(some.intersects(ra));

        assertTrue(someWrapped.intersects(lr));
        assertTrue(someWrapped.intersects(br));
        assertTrue(someWrapped.intersects(bi));
        assertTrue(someWrapped.intersects(ba));
        assertTrue(someWrapped.intersects(la));
        assertTrue(someWrapped.intersects(li));
        assertFalse(someWrapped.intersects(ii));
        assertFalse(someWrapped.intersects(ir));
        assertTrue(someWrapped.intersects(bb));
        assertTrue(someWrapped.intersects(aa));
        assertFalse(someWrapped.intersects(bl));
        assertTrue(someWrapped.intersects(ra));
    }

    /**
     * Test that we handle partial bounds of the type x > n or x >= n which specifically have
     * their right value as minimum.
     */
    @Test
    public void rangeIntersectsPartialBounds()
    {
        Range<Token> range = new Range<>(Murmur3Partitioner.MINIMUM, new Murmur3Partitioner.LongToken(-1L));

        Bounds<Token> boundsMatch = new Bounds<>(new Murmur3Partitioner.LongToken(-2L), Murmur3Partitioner.MINIMUM);
        Bounds<Token> boundsNoMatch = new Bounds<>(new Murmur3Partitioner.LongToken(0L), Murmur3Partitioner.MINIMUM);

        assertTrue(range.intersects(boundsMatch));
        assertFalse(range.intersects(boundsNoMatch));

        ExcludingBounds<Token> excBoundsMatch = new ExcludingBounds<>(new Murmur3Partitioner.LongToken(-2L), Murmur3Partitioner.MINIMUM);
        ExcludingBounds<Token> excBoundsNoMatch = new ExcludingBounds<>(new Murmur3Partitioner.LongToken(-1L), Murmur3Partitioner.MINIMUM);

        assertTrue(range.intersects(excBoundsMatch));
        assertFalse(range.intersects(excBoundsNoMatch));

        IncludingExcludingBounds<Token> incExcBoundsMatch = new IncludingExcludingBounds<>(new Murmur3Partitioner.LongToken(-2L), Murmur3Partitioner.MINIMUM);
        IncludingExcludingBounds<Token> incExcBoundsNoMatch = new IncludingExcludingBounds<>(new Murmur3Partitioner.LongToken(0L), Murmur3Partitioner.MINIMUM);

        assertTrue(range.intersects(incExcBoundsMatch));
        assertFalse(range.intersects(incExcBoundsNoMatch));
    }
}
