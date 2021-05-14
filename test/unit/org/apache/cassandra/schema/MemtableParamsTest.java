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

import org.apache.cassandra.config.InheritingClass;
import org.apache.cassandra.config.ParameterizedClass;
import org.apache.cassandra.db.memtable.SkipListMemtableFactory;
import org.apache.cassandra.exceptions.ConfigurationException;

import static org.junit.Assert.assertEquals;

public class MemtableParamsTest
{
    static final ParameterizedClass DEFAULT = SkipListMemtableFactory.CONFIGURATION;

    @Test
    public void testDefault()
    {
        Map<String, ParameterizedClass> map = MemtableParams.expandDefinitions(ImmutableMap.of());
        assertEquals(ImmutableMap.of("default", DEFAULT), map);
    }

    @Test
    public void testDefaultRemapped()
    {
        Map<String, ParameterizedClass> map = MemtableParams.expandDefinitions
        (
            ImmutableMap.of("remap", new InheritingClass("default", null, null))
        );
        assertEquals(ImmutableMap.of("default", DEFAULT,
                                     "remap", DEFAULT),
                     map);
    }

    @Test
    public void testOne()
    {
        final InheritingClass one = new InheritingClass(null, "SkipList", null);
        Map<String, ParameterizedClass> map = MemtableParams.expandDefinitions(ImmutableMap.of("one", one));
        assertEquals(ImmutableMap.of("default", DEFAULT,
                                     "one", one),
                     map);
    }

    @Test
    public void testExtends()
    {
        final InheritingClass one = new InheritingClass(null, "SkipList", null);
        Map<String, ParameterizedClass> map = MemtableParams.expandDefinitions
        (
            ImmutableMap.of("one", one,
                            "two", new InheritingClass("one",
                                                       null,
                                                       ImmutableMap.of("extra", "value")))
        );

        assertEquals(ImmutableMap.of("default", DEFAULT,
                                     "one", one,
                                     "two", new ParameterizedClass("SkipList",
                                                                   ImmutableMap.of("extra", "value"))),
                     map);
    }

    @Test
    public void testExtendsReplace()
    {
        final InheritingClass one = new InheritingClass(null,
                                                        "SkipList",
                                                        ImmutableMap.of("extra", "valueOne"));
        Map<String, ParameterizedClass> map = MemtableParams.expandDefinitions
        (
            ImmutableMap.of("one", one,
                            "two", new InheritingClass("one",
                                                       null,
                                                       ImmutableMap.of("extra", "value")))
        );
        assertEquals(ImmutableMap.of("default", DEFAULT,
                                     "one", one,
                                     "two", new ParameterizedClass("SkipList",
                                                                   ImmutableMap.of("extra", "value"))),
                     map);
    }

    @Test
    public void testDoubleExtends()
    {
        final InheritingClass one = new InheritingClass(null, "SkipList", null);
        Map<String, ParameterizedClass> map = MemtableParams.expandDefinitions
        (
            ImmutableMap.of("one", one,
                            "two", new InheritingClass("one",
                                                       null,
                                                       ImmutableMap.of("param", "valueTwo",
                                                                       "extra", "value")),
                            "three", new InheritingClass("two",
                                                         "OtherClass",
                                                         ImmutableMap.of("param", "valueThree",
                                                                         "extraThree", "three")))
        );
        assertEquals(ImmutableMap.of("default", DEFAULT,
                                     "one", one,
                                     "two", new ParameterizedClass("SkipList",
                                                                   ImmutableMap.of("param", "valueTwo",
                                                                                   "extra", "value")),
                                     "three", new ParameterizedClass("OtherClass",
                                                                     ImmutableMap.of("param", "valueThree",
                                                                                     "extra", "value",
                                                                                     "extraThree", "three"))),
                     map);
    }

    @Test
    public void testInvalidExtends()
    {
        final InheritingClass one = new InheritingClass(null, "SkipList", null);
        try
        {
            Map<String, ParameterizedClass> map = MemtableParams.expandDefinitions
            (
                ImmutableMap.of("two", new InheritingClass("one",
                                                           null,
                                                           ImmutableMap.of("extra", "value")),
                                "one", one)
            );
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
            Map<String, ParameterizedClass> map = MemtableParams.expandDefinitions
            (
                ImmutableMap.of("one", new InheritingClass("one",
                                                           null,
                                                           ImmutableMap.of("extra", "value")))
            );
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
        final InheritingClass one = new InheritingClass(null,
                                                        "SkipList",
                                                        ImmutableMap.of("extra", "valueOne"));
        Map<String, ParameterizedClass> map = MemtableParams.expandDefinitions(ImmutableMap.of("default", one));
        assertEquals(ImmutableMap.of("default", one), map);
    }

    @Test
    public void testDefaultExtends()
    {
        final InheritingClass one = new InheritingClass(null,
                                                        "SkipList",
                                                        ImmutableMap.of("extra", "valueOne"));
        Map<String, ParameterizedClass> map = MemtableParams.expandDefinitions
        (
            ImmutableMap.of("one", one,
                            "default", new InheritingClass("one", null, ImmutableMap.of()))
        );
        assertEquals(ImmutableMap.of("one", one,
                                     "default", one),
                     map);
    }
    // Note: The factories constructed from these parameters are tested in the CreateTest and AlterTest.
}
