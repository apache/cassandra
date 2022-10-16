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
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;

import org.junit.Assert;
import org.junit.Test;

import org.apache.cassandra.service.accord.AccordState;

import static org.apache.cassandra.service.accord.store.StoredValueTest.assertISE;

public class StoredMapTest
{

    private static NavigableMap<Integer, Integer> getAdditions(StoredNavigableMap<Integer, Integer> map)
    {
        NavigableMap<Integer, Integer> result = new TreeMap<>();
        map.forEachAddition(result::put);
        return result;
    }

    private static Set<Integer> getDeletions(StoredNavigableMap<Integer, Integer> map)
    {
        Set<Integer> result = new HashSet<>();
        map.forEachDeletion(result::add);
        return result;
    }

    @Test
    public void loadMap()
    {
        NavigableMap<Integer, Integer> expectedData = new TreeMap<>();
        expectedData.put(1, 2);
        expectedData.put(5, 6);

        StoredNavigableMap<Integer, Integer> map = new StoredNavigableMap<>(AccordState.ReadWrite.FULL);

        // no values loaded, getView should fail
        assertISE(map::getView);

        map.load(new TreeMap<>(expectedData));
        Assert.assertEquals(expectedData, map.getView());
        Assert.assertFalse(map.hasModifications());
        Assert.assertFalse(map.hasAdditions());
        Assert.assertFalse(map.hasDeletions());

        // check additions
        NavigableMap<Integer, Integer> expectedAdditions = new TreeMap<>();
        expectedAdditions.put(3, 4);
        expectedData.put(3, 4);
        map.blindPut(3, 4);
        Assert.assertEquals(expectedData, map.getView());
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
        expectedData.remove(5);
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
        StoredNavigableMap<Integer, Integer> map = new StoredNavigableMap<>(AccordState.ReadWrite.FULL);
        assertISE(map::getView);

        // check additions
        NavigableMap<Integer, Integer> expectedAdditions = new TreeMap<>();
        expectedAdditions.put(3, 4);
        map.blindPut(3, 4);
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
        NavigableMap<Integer, Integer> expectedData = new TreeMap<>();
        NavigableMap<Integer, Integer> expectedAdditions = new TreeMap<>();
        Set<Integer> expectedDeletions = new HashSet<>();

        StoredNavigableMap<Integer, Integer> map = new StoredNavigableMap<>(AccordState.ReadWrite.FULL);
        map.load(new TreeMap<>());
        Assert.assertEquals(expectedData, map.getView());

        // add
        map.blindPut(1, 2);
        map.blindPut(3, 4);

        expectedData.put(1, 2);
        expectedData.put(3, 4);
        expectedAdditions.put(1, 2);
        expectedAdditions.put(3, 4);

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
        NavigableMap<Integer, Integer> expectedData = new TreeMap<>();
        NavigableMap<Integer, Integer> expectedAdditions = new TreeMap<>();

        expectedData.put(1, 2);
        StoredNavigableMap<Integer, Integer> map = new StoredNavigableMap<>(AccordState.ReadWrite.FULL);
        map.load(new TreeMap<>(expectedData));
        Assert.assertEquals(expectedData, map.getView());

        map.clear();
        expectedData.clear();
        Assert.assertEquals(expectedData, map.getView());

        map.blindPut(3, 4);
        map.blindPut(5, 6);
        map.blindRemove(3);

        // since this will be written with a range tombstone, deletes shouldn't be tracked
        expectedData.put(5, 6);
        expectedAdditions.put(5, 6);

        Assert.assertEquals(expectedData, map.getView());
        Assert.assertEquals(expectedAdditions, getAdditions(map));
        Assert.assertEquals(Collections.emptySet(), getDeletions(map));
    }
}
