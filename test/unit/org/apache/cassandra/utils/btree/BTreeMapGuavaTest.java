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

package org.apache.cassandra.utils.btree;

import java.util.AbstractMap;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.testing.*;
import com.google.common.collect.testing.features.CollectionSize;
import com.google.common.collect.testing.testers.MapEntrySetTester;

import junit.framework.Test; // checkstyle: permit this import
import junit.framework.TestSuite; // checkstyle: permit this import

import static com.google.common.collect.testing.Helpers.getMethod;

public class BTreeMapGuavaTest
{
    public static Test suite()
    {
        return new BTreeMapGuavaTest().allTests();
    }

    public Test allTests()
    {
        TestSuite suite = new TestSuite("btree map tests");
        // todo; add a mutable wrapper and run the full map tests
        // todo; add a navigable wrapper and run the full navigable map tests
        suite.addTest(testsForUnmodifiableMap());
        return suite;
    }

    public Test testsForUnmodifiableMap() {
        return MapTestSuiteBuilder.using(
                                  new TestStringMapGenerator() {
                                      @Override
                                      protected Map<String, String> create(Map.Entry<String, String>[] entries)
                                      {
                                          return populate(entries);
                                      }
                                  })
                                  .named("unmodifiableMap/BTreeMap")
                                  .withFeatures(
                                  CollectionSize.ANY)
                                  .suppressing(Arrays.asList(getMethod(MapEntrySetTester.class, "testContainsEntryWithIncomparableValue")))
                                  .createTestSuite();
    }

    private static Map<String, String> populate(Map.Entry<String, String>[] entries)
    {
        BTreeMap<String, String> map = BTreeMap.empty();
        for (Map.Entry<String, String> entry : entries)
            map = map.withForce(entry.getKey(), entry.getValue());
        return new BTreeMapWrapper<>(map);
    }

    // BTreeSet toString / toArray / hashCode are non-standard, wrap them with the set from BTreeSetGuavaTest;
    private static class BTreeMapWrapper<K extends Comparable<K>, V> extends AbstractMap<K, V>
    {
        private final BTreeMap<K, V> map;

        public BTreeMapWrapper(BTreeMap<K, V> map)
        {
            this.map = map;
        }

        @Override
        public Set<Entry<K, V>> entrySet()
        {
            return new BTreeSetGuavaTest.BTreeSetWithClassicToArray<>((BTreeSet<Map.Entry<K, V>>)map.entrySet());
        }

        @Override
        public Set<K> keySet()
        {
            return new BTreeSetGuavaTest.BTreeSetWithClassicToArray<>((BTreeSet<K>)map.keySet());
        }

        public V get(K key)
        {
            return map.get(key);
        }

        public Collection<V> values()
        {
            return map.values();
        }

        public boolean containsValue(Object value)
        {
            return map.containsValue(value);
        }

        public boolean containsKey(Object key)
        {
            return map.containsKey(key);
        }

        public boolean isEmpty()
        {
            return map.isEmpty();
        }

        public int size()
        {
            return map.size();
        }
    }
}
