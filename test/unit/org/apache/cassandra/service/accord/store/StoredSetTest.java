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

package org.apache.cassandra.service.accord.store;

import java.util.Collections;
import java.util.HashSet;
import java.util.NavigableSet;
import java.util.Set;
import java.util.TreeSet;

import org.junit.Assert;
import org.junit.Test;

import org.apache.cassandra.service.accord.AccordState;

import static org.apache.cassandra.service.accord.store.StoredValueTest.assertISE;

public class StoredSetTest
{
    private static NavigableSet<Integer> getAdditions(StoredSet.Navigable<Integer> set)
    {
        NavigableSet<Integer> result = new TreeSet<>();
        set.forEachAddition(result::add);
        return result;
    }

    private static Set<Integer> getDeletions(StoredSet.Navigable<Integer> set)
    {
        Set<Integer> result = new HashSet<>();
        set.forEachDeletion(result::add);
        return result;
    }

    @Test
    public void loadMap()
    {
        NavigableSet<Integer> expected = new TreeSet<>();
        expected.add(1);
        expected.add(5);

        StoredSet.Navigable<Integer> map = new StoredSet.Navigable<>(AccordState.ReadWrite.FULL);

        // no values loaded, getView should fail
        assertISE(map::getView);

        map.load(new TreeSet<>(expected));
        Assert.assertEquals(expected, map.getView());
        Assert.assertFalse(map.hasModifications());
        Assert.assertFalse(map.hasAdditions());
        Assert.assertFalse(map.hasDeletions());

        // check additions
        NavigableSet<Integer> expectedAdditions = new TreeSet<>();
        expectedAdditions.add(3);
        expected.add(3);
        map.blindAdd(3);
        Assert.assertEquals(expected, map.getView());
        Assert.assertTrue(map.hasModifications());
        Assert.assertTrue(map.hasAdditions());
        Assert.assertFalse(map.hasDeletions());

        Assert.assertEquals(expectedAdditions, getAdditions(map));

        // check deletions
        Set<Integer> expectedDeletions = new HashSet<>();
        expectedDeletions.add(5);
        expectedDeletions.add(6);
        map.blindRemove(5);
        map.blindRemove(6);
        expected.remove(5);
        Assert.assertTrue(map.hasModifications());
        Assert.assertTrue(map.hasAdditions());
        Assert.assertTrue(map.hasDeletions());

        Assert.assertEquals(expectedDeletions, getDeletions(map));

        map.clearModifiedFlag();
        Assert.assertFalse(map.hasAdditions());
        Assert.assertFalse(map.hasDeletions());
        Assert.assertTrue(getAdditions(map).isEmpty());
        Assert.assertTrue(getDeletions(map).isEmpty());

        map.unload();
        assertISE(map::getView);
        Assert.assertFalse(map.hasModifications());
        Assert.assertFalse(map.hasAdditions());
        Assert.assertFalse(map.hasDeletions());
    }

    @Test
    public void unloadedAddsAndRemoves()
    {
        StoredSet.Navigable<Integer> map = new StoredSet.Navigable<>(AccordState.ReadWrite.FULL);
        assertISE(map::getView);

        // check additions
        NavigableSet<Integer> expectedAdditions = new TreeSet<>();
        expectedAdditions.add(3);
        map.blindAdd(3);
        Assert.assertTrue(map.hasModifications());
        Assert.assertTrue(map.hasAdditions());
        Assert.assertFalse(map.hasDeletions());

        Assert.assertEquals(expectedAdditions, getAdditions(map));

        // check deletions
        Set<Integer> expectedDeletions = new HashSet<>();
        expectedDeletions.add(5);
        expectedDeletions.add(6);
        map.blindRemove(5);
        map.blindRemove(6);
        Assert.assertTrue(map.hasModifications());
        Assert.assertTrue(map.hasAdditions());
        Assert.assertTrue(map.hasDeletions());

        Assert.assertEquals(expectedDeletions, getDeletions(map));

        // still shouldn't be able to read a complete map
        assertISE(map::getView);
    }

    // deleting a key should remove it from additions
    @Test
    public void additionDeletionCanceling()
    {
        NavigableSet<Integer> expectedData = new TreeSet<>();
        NavigableSet<Integer> expectedAdditions = new TreeSet<>();
        Set<Integer> expectedDeletions = new HashSet<>();

        StoredSet.Navigable<Integer> map = new StoredSet.Navigable<>(AccordState.ReadWrite.FULL);
        map.load(new TreeSet<>());
        Assert.assertEquals(expectedData, map.getView());

        // add
        map.blindAdd(1);
        map.blindAdd(3);

        expectedData.add(1);
        expectedData.add(3);
        expectedAdditions.add(1);
        expectedAdditions.add(3);

        Assert.assertEquals(expectedData, map.getView());
        Assert.assertEquals(expectedAdditions, getAdditions(map));
        Assert.assertEquals(expectedDeletions, getDeletions(map));

        // remove
        map.blindRemove(3);
        expectedData.remove(3);
        expectedAdditions.remove(3);
        expectedDeletions.add(3);
        Assert.assertEquals(expectedData, map.getView());
        Assert.assertEquals(expectedAdditions, getAdditions(map));
        Assert.assertEquals(expectedDeletions, getDeletions(map));


    }

    @Test
    public void clearMap()
    {
        NavigableSet<Integer> expectedData = new TreeSet<>();
        NavigableSet<Integer> expectedAdditions = new TreeSet<>();

        expectedData.add(1);
        StoredSet.Navigable<Integer> map = new StoredSet.Navigable<>(AccordState.ReadWrite.FULL);
        map.load(new TreeSet<>(expectedData));
        Assert.assertEquals(expectedData, map.getView());

        map.clear();
        expectedData.clear();
        Assert.assertEquals(expectedData, map.getView());

        map.blindAdd(3);
        map.blindAdd(5);
        map.blindRemove(3);

        // since this will be written with a range tombstone, deletes shouldn't be tracked
        expectedData.add(5);
        expectedAdditions.add(5);

        Assert.assertEquals(expectedData, map.getView());
        Assert.assertEquals(expectedAdditions, getAdditions(map));
        Assert.assertEquals(Collections.emptySet(), getDeletions(map));
    }
}
