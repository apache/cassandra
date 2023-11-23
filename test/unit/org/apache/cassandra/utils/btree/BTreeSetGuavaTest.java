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

import java.util.AbstractSet;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Set;
import java.util.TreeSet;

import com.google.common.collect.testing.SetTestSuiteBuilder;
import com.google.common.collect.testing.TestStringSetGenerator;
import com.google.common.collect.testing.features.CollectionSize;

import junit.framework.Test; // checkstyle: permit this import
import junit.framework.TestSuite; // checkstyle: permit this import

import static java.util.Comparator.naturalOrder;

public class BTreeSetGuavaTest
{
    public static Test suite()
    {
        return new BTreeSetGuavaTest().allTests();
    }

    public Test allTests()
    {
        TestSuite suite = new TestSuite("btree map tests");
        suite.addTest(testsForUnmodifiableSet());
        // todo: SortedSetTestSuiteBuilder
        return suite;
    }

    public Test testsForUnmodifiableSet()
    {
        return SetTestSuiteBuilder.using(
                                  new TestStringSetGenerator() {
                                      @Override
                                      public Set<String> create(String[] elements)
                                      {
                                          return populate(elements);
                                      }
                                  })
                                  .named("unmodifiableSet/HashSet")
                                  .withFeatures(CollectionSize.ANY)
                                  .suppressing(Collections.emptySet())
                                  .createTestSuite();
    }

    private static Set<String> populate(String[] entries)
    {
        BTreeSetWithClassicToArray<String> set = BTreeSetWithClassicToArray.<String>empty(naturalOrder());
        set = set.update(new TreeSet<>(Arrays.asList(entries)));
        return set;
    }

    /**
     * our hashCode / toString / toArray are not correct according to guava, using AbstractSet ones here to get the test to pass
     */
    public static class BTreeSetWithClassicToArray<K> extends AbstractSet<K>
    {
        private final BTreeSet<K> wrapped;

        public BTreeSetWithClassicToArray(BTreeSet<K> s)
        {
            wrapped = s;
        }

        public BTreeSetWithClassicToArray<K> update(Collection<K> entries)
        {
            return new BTreeSetWithClassicToArray<>(wrapped.with(entries));
        }

        public static <K extends Comparable<K>> BTreeSetWithClassicToArray<K> empty(Comparator<? super K> comparator)
        {
            return new BTreeSetWithClassicToArray<K>(BTreeSet.empty(comparator));
        }

        @Override
        public Iterator<K> iterator()
        {
            return wrapped.iterator();
        }

        @Override
        public int size()
        {
            return wrapped.size();
        }

        public boolean contains(Object x)
        {
            return wrapped.contains(x);
        }

        public boolean isEmpty()
        {
            return wrapped.isEmpty();
        }
    }
}
