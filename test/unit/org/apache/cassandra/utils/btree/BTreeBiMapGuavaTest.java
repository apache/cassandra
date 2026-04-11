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

import java.util.Arrays;
import java.util.Map;

import com.google.common.collect.BiMap;
import com.google.common.collect.testing.features.CollectionSize;
import com.google.common.collect.testing.features.MapFeature;
import com.google.common.collect.testing.google.BiMapInverseTester;
import com.google.common.collect.testing.google.BiMapTestSuiteBuilder;
import com.google.common.collect.testing.google.TestBiMapGenerator;
import com.google.common.collect.testing.google.TestStringBiMapGenerator;
import com.google.common.collect.testing.testers.CollectionContainsAllTester;
import com.google.common.collect.testing.testers.CollectionContainsTester;
import com.google.common.collect.testing.testers.CollectionCreationTester;
import com.google.common.collect.testing.testers.CollectionToArrayTester;
import com.google.common.collect.testing.testers.MapCreationTester;
import com.google.common.collect.testing.testers.MapEntrySetTester;
import com.google.common.collect.testing.testers.SetHashCodeTester;

import junit.framework.Test; // checkstyle: permit this import
import junit.framework.TestSuite; // checkstyle: permit this import

import static com.google.common.collect.testing.Helpers.getMethod;

public class BTreeBiMapGuavaTest
{
    public static Test suite() {
        TestSuite suite = new TestSuite();

        suite.addTest(BiMapTestSuiteBuilder.using(generator)
                             .named("ImmutableBiMap")
                             .withFeatures(CollectionSize.ANY, MapFeature.RESTRICTS_KEYS, MapFeature.REJECTS_DUPLICATES_AT_CREATION)
                             .suppressing(Arrays.asList(getMethod(CollectionToArrayTester.class, "testToArray_oversizedArray"),
                                                        getMethod(BiMapInverseTester.class, "testInverseSame"),
                                                        getMethod(MapEntrySetTester.class, "testContainsEntryWithIncomparableValue"),
                                                        getMethod(MapCreationTester.class, "testCreateWithNullValueUnsupported"),
                                                        getMethod(MapCreationTester.class, "testCreateWithNullKeyUnsupported"),
                                                        getMethod(CollectionContainsAllTester.class, "testContainsAll_nullAllowed"),
                                                        getMethod(CollectionCreationTester.class, "testCreateWithNull_unsupported"),
                                                        getMethod(SetHashCodeTester.class, "testHashCode"),
                                                        getMethod(CollectionContainsTester.class, "testContains_nullNotContainedButQueriesSupported")))
                             .createTestSuite());
        return suite;
    }

    static TestBiMapGenerator<String, String> generator = new TestStringBiMapGenerator()
    {
        @Override
        protected BiMap<String, String> create(Map.Entry<String, String>[] entries)
        {
            BTreeBiMap<String, String> bimap = BTreeBiMap.empty();
            if (entries == null)
                return bimap;
            for (Map.Entry<String, String> entry : entries)
                bimap = bimap.with(entry.getKey(), entry.getValue());
            return bimap;
        }
    };
}
