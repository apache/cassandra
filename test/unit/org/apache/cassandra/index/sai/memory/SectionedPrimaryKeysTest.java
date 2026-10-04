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
package org.apache.cassandra.index.sai.memory;

import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.index.sai.SAITester;
import org.apache.cassandra.index.sai.utils.PrimaryKey;
import org.apache.cassandra.index.sai.utils.PrimaryKeys;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

/**
 * Unit tests for {@link SectionedPrimaryKeys}, the {@link TrieMemoryIndex} node payload used when
 * the SAI prefix feature is enabled. Verifies that the exact and prefix sections are populated and
 * read independently and that the lazily-allocated prefix section reports the shared empty sentinel
 * until it is first written.
 */
public class SectionedPrimaryKeysTest
{
    private PrimaryKey.Factory keyFactory;

    @Before
    public void setup()
    {
        keyFactory = new PrimaryKey.Factory(Murmur3Partitioner.instance, SAITester.EMPTY_COMPARATOR);
    }

    private PrimaryKey key(long token)
    {
        return keyFactory.create(new Murmur3Partitioner.LongToken(token));
    }

    @Test
    public void emptyPayloadReportsEmptyAndSharesEmptySentinel()
    {
        SectionedPrimaryKeys sectioned = new SectionedPrimaryKeys();

        assertTrue(sectioned.isEmpty());
        assertNotNull(sectioned.exact());
        assertTrue(sectioned.exact().isEmpty());
        // prefix is lazily allocated: before any addPrefix it must return the shared EMPTY sentinel.
        assertSame(PrimaryKeys.EMPTY, sectioned.prefix());
        assertTrue(sectioned.prefix().isEmpty());
    }

    @Test
    public void addExactPopulatesOnlyExactSection()
    {
        SectionedPrimaryKeys sectioned = new SectionedPrimaryKeys();

        sectioned.addExact(key(1));
        sectioned.addExact(key(2));

        assertFalse(sectioned.isEmpty());
        assertEquals(2, sectioned.exact().size());
        // The prefix section must remain untouched (still the EMPTY sentinel).
        assertSame(PrimaryKeys.EMPTY, sectioned.prefix());
        assertTrue(sectioned.prefix().isEmpty());
    }

    @Test
    public void addPrefixPopulatesOnlyPrefixSection()
    {
        SectionedPrimaryKeys sectioned = new SectionedPrimaryKeys();

        sectioned.addPrefix(key(1));
        sectioned.addPrefix(key(2));
        sectioned.addPrefix(key(3));

        assertFalse(sectioned.isEmpty());
        assertTrue(sectioned.exact().isEmpty());
        assertEquals(3, sectioned.prefix().size());
    }

    @Test
    public void exactAndPrefixSectionsAreIndependent()
    {
        SectionedPrimaryKeys sectioned = new SectionedPrimaryKeys();

        sectioned.addExact(key(10));
        sectioned.addPrefix(key(20));
        sectioned.addPrefix(key(30));

        assertEquals(1, sectioned.exact().size());
        assertEquals(2, sectioned.prefix().size());
        assertFalse(sectioned.isEmpty());
    }

    @Test
    public void unsharedHeapSizeGrowsWhenPrefixSectionIsAllocated()
    {
        SectionedPrimaryKeys sectioned = new SectionedPrimaryKeys();

        // Only the exact set is allocated up front; the prefix set is lazy (null), so adding exact
        // keys does not allocate a new set and the unshared overhead stays at the single-set size.
        long beforePrefix = sectioned.unsharedHeapSize();
        sectioned.addExact(key(1));
        assertEquals("adding an exact key must not allocate the lazy prefix set",
                     beforePrefix, sectioned.unsharedHeapSize());

        // The first addPrefix lazily allocates the second PrimaryKeys, increasing the unshared overhead.
        sectioned.addPrefix(key(2));
        assertTrue("allocating the prefix section should increase the unshared heap size",
                   sectioned.unsharedHeapSize() > beforePrefix);
    }
}
