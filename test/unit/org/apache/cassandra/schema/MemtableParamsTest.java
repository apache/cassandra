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

package org.apache.cassandra.schema;

import java.util.Map;

import com.google.common.collect.ImmutableMap;
import org.junit.Assert;
import org.junit.Test;

import org.apache.cassandra.db.memtable.SkipListMemtableFactory;
import org.apache.cassandra.exceptions.ConfigurationException;

import static org.junit.Assert.assertEquals;

public class MemtableParamsTest
{
    static final Map<String, String> DEFAULT = SkipListMemtableFactory.CONFIGURATION;

    @Test
    public void testDefault()
    {
        Map<String, Map<String, String>> map = MemtableParams.expandDefinitions(ImmutableMap.of());
        assertEquals(ImmutableMap.of("default", DEFAULT), map);
    }

    @Test
    public void testDefaultRemapped()
    {
        Map<String, Map<String, String>> map = MemtableParams.expandDefinitions
        (
            ImmutableMap.of("remap", ImmutableMap.of("extends", "default"))
        );
        assertEquals(ImmutableMap.of("default", DEFAULT,
                                     "remap", DEFAULT),
                     map);
    }

    @Test
    public void testOne()
    {
        final ImmutableMap<String, String> one = ImmutableMap.of("class", "SkipList");
        Map<String, Map<String, String>> map = MemtableParams.expandDefinitions(ImmutableMap.of("one", one));
        assertEquals(ImmutableMap.of("default", DEFAULT,
                                     "one", one),
                     map);
    }

    @Test
    public void testExtends()
    {
        final ImmutableMap<String, String> one = ImmutableMap.of("class", "SkipList");
        Map<String, Map<String, String>> map = MemtableParams.expandDefinitions(ImmutableMap.of("one", one,
                                                                                                "two", ImmutableMap.of("extends", "one",
                                                                                                                       "extra", "value")));
        assertEquals(ImmutableMap.of("default", DEFAULT,
                                     "one", one,
                                     "two", ImmutableMap.of("class", "SkipList",
                                                            "extra", "value")),
                     map);
    }

    @Test
    public void testExtendsReplace()
    {
        final ImmutableMap<String, String> one = ImmutableMap.of("class", "SkipList",
                                                                 "extra", "valueOne");
        Map<String, Map<String, String>> map = MemtableParams.expandDefinitions(ImmutableMap.of("one", one,
                                                                                                "two", ImmutableMap.of("extends", "one",
                                                                                                                       "extra", "value")));
        assertEquals(ImmutableMap.of("default", DEFAULT,
                                     "one", one,
                                     "two", ImmutableMap.of("class", "SkipList",
                                                            "extra", "value")),
                     map);
    }

    @Test
    public void testDoubleExtends()
    {
        final ImmutableMap<String, String> one = ImmutableMap.of("class", "SkipList");
        Map<String, Map<String, String>> map = MemtableParams.expandDefinitions(ImmutableMap.of("one", one,
                                                                                                "two", ImmutableMap.of("extends", "one",
                                                                                                                       "param", "valueTwo",
                                                                                                                       "extra", "value"),
                                                                                                "three", ImmutableMap.of("extends", "two",
                                                                                                                         "param", "valueThree",
                                                                                                                         "extraThree", "three")));
        assertEquals(ImmutableMap.of("default", DEFAULT,
                                     "one", one,
                                     "two", ImmutableMap.of("class", "SkipList",
                                                            "param", "valueTwo",
                                                            "extra", "value"),
                                     "three", ImmutableMap.of("class", "SkipList",
                                                              "param", "valueThree",
                                                              "extra", "value",
                                                              "extraThree", "three")),
                     map);
    }

    @Test
    public void testInvalidExtends()
    {
        final ImmutableMap<String, String> one = ImmutableMap.of("class", "SkipList");
        try
        {
            Map<String, Map<String, String>> map = MemtableParams.expandDefinitions(ImmutableMap.of("two", ImmutableMap.of("extends", "one",
                                                                                                                           "extra", "value"),
                                                                                                    "one", one));
            Assert.fail("Expected exception.");
        }
        catch (ConfigurationException e)
        {
            // expected
        }
    }

    @Test
    public void testInvalidSelfExtends()
    {
        try
        {
            Map<String, Map<String, String>> map = MemtableParams.expandDefinitions(ImmutableMap.of("one", ImmutableMap.of("extends", "one",
                                                                                                                           "extra", "value")));
            Assert.fail("Expected exception.");
        }
        catch (ConfigurationException e)
        {
            // expected
        }
    }

    @Test
    public void testReplaceDefault()
    {
        final ImmutableMap<String, String> one = ImmutableMap.of("class", "SkipList",
                                                                 "extra", "valueOne");
        Map<String, Map<String, String>> map = MemtableParams.expandDefinitions(ImmutableMap.of("default", one));
        assertEquals(ImmutableMap.of("default", one), map);
    }

    @Test
    public void testDefaultExtends()
    {
        final ImmutableMap<String, String> one = ImmutableMap.of("class", "SkipList",
                                                                 "extra", "valueOne");
        Map<String, Map<String, String>> map = MemtableParams.expandDefinitions(ImmutableMap.of("one", one,
                                                                                                "default", ImmutableMap.of("extends", "one")));
        assertEquals(ImmutableMap.of("one", one,
                                     "default", one),
                     map);
    }
    // Note: The factories constructed from these parameters are tested in the CreateTest and AlterTest.
}
