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

package org.apache.cassandra.db.compaction;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.junit.Test;

import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.utils.Pair;
import org.mockito.Mockito;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class CompactionAggregateTest
{
    @Test
    public void testContainsSameInstance()
    {
        // Create a CompactionAggregate
        SSTableReader sstableReader = mock(SSTableReader.class);
        CompactionAggregate agg = CompactionAggregate.createForTombstones(sstableReader);
        // Non-existnig compaction
        Pair<Boolean, CompactionPick> res = agg.containsSameInstance(mock(CompactionPick.class));
        
        assertFalse(res.left);
        assertNull(res.right);
        
        // Existing compaction and same instance
        CompactionPick existing = agg.getSelected();
        res = agg.containsSameInstance(existing);
        
        assertTrue(res.left); // same instance
        assertNotNull(res.right);
        assertEquals(existing, res.right);
        assertSame(existing, res.right);

        // Existing compaction but different instance
        CompactionPick otherInstance = existing.withParent(existing.parent());
        res = agg.containsSameInstance(otherInstance);
        
        assertFalse(res.left); // different instance
        assertNotNull(res.right);
        assertEquals(otherInstance, res.right);
        assertNotSame(otherInstance, res.right);
    }

    @Test
    public void testWithReplacedCompaction()
    {
        // Create a CompactionAggregate with two compactions
        CompactionPick anotherCompaction = Mockito.mock(CompactionPick.class);
        when(anotherCompaction.sstables()).thenReturn(ImmutableSet.of());
        SSTableReader sstableReader = mock(SSTableReader.class);
        CompactionAggregate agg = CompactionAggregate.createForTombstones(sstableReader)
                                                         .withAdditionalCompactions(ImmutableList.of(anotherCompaction));

        // Setup existing and replacement CompactionPick
        CompactionPick existing = agg.getSelected();
        CompactionPick replacement = existing.withParent(existing.parent() + 1);

        // Initial conditions
        assertEquals(2, agg.compactions.size());
        assertFalse(agg.compactions.contains(replacement));

        // No existing CompactionPick to replace - replacement is added
        CompactionAggregate res = agg.withReplacedCompaction(replacement, null);
        
        assertEquals(3, res.compactions.size());
        assertTrue(res.compactions.contains(replacement));

        // Existing CompactionPick is replaced
        res = agg.withReplacedCompaction(replacement, existing);
        
        assertEquals(2, res.compactions.size());
        assertFalse(res.compactions.contains(existing));
        assertTrue(res.compactions.contains(replacement));
    }
}
