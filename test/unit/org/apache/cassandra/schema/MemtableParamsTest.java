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

import java.util.LinkedHashMap;
import java.util.Map;

import accord.utils.Gen;
import com.google.common.collect.ImmutableMap;
import org.apache.cassandra.config.Config;
import org.apache.cassandra.utils.ConfigGenBuilder;

import org.junit.Test;

import org.apache.cassandra.config.InheritingClass;
import org.apache.cassandra.config.ParameterizedClass;
import org.apache.cassandra.db.memtable.SkipListMemtableFactory;
import org.apache.cassandra.exceptions.ConfigurationException;

import static accord.utils.Property.qt;
import static org.apache.cassandra.config.YamlConfigurationLoader.fromMap;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

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
    public void testOrderingDoesNotMatter()
    {
        // linked hash map preserves insertion order
        Map<String, InheritingClass> config = new LinkedHashMap<>();

        config.put("default", new InheritingClass("trie", null, null));
        config.put("abc", new InheritingClass(null, "Abc", ImmutableMap.of("c", "d")));
        config.put("skiplist", new InheritingClass("skiplistOnSteroids", "SkipListMemtable", ImmutableMap.of("e", "f")));
        config.put("skiplistOnSteroids", new InheritingClass("abc", null, ImmutableMap.of("a", "b")));
        config.put("trie", new InheritingClass(null, "TrieMemtable", null));

        Map<String, ParameterizedClass> map = MemtableParams.expandDefinitions(config);
        assertEquals("TrieMemtable", map.get("default").class_name);

        // this inherits abc which has c as config parameter
        assertTrue(map.get("skiplistOnSteroids").parameters.containsKey("a"));
        assertTrue(map.get("skiplistOnSteroids").parameters.containsKey("c"));
        assertEquals(map.get("skiplistOnSteroids").class_name, config.get("abc").class_name);

        // this inherits from skiplistOnSteroids which so params are carried over as well
        assertTrue(map.get("skiplist").parameters.containsKey("a"));
        assertTrue(map.get("skiplist").parameters.containsKey("c"));
        assertTrue(map.get("skiplist").parameters.containsKey("e"));
        assertEquals(map.get("skiplist").class_name, "SkipListMemtable");

        Map<String, InheritingClass> config2 = new LinkedHashMap<>();
        config2.put("skiplist", new InheritingClass(null, "SkipListMemtable", null));
        config2.put("trie", new InheritingClass(null, "TrieMemtable", null));
        config2.put("default", new InheritingClass("trie", null, null));

        Map<String, ParameterizedClass> map2 = MemtableParams.expandDefinitions(config2);
        assertEquals("TrieMemtable", map2.get("default").class_name);
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
            fail("Expected exception.");
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

    @Test
    public void testExpandDefinitions()
    {
        Gen<Map<String, InheritingClass>> gen = new ConfigGenBuilder()
                                                .buildMemtable()
                                                .map(m -> fromMap(m, Config.class))
                                                .map(c -> c.memtable.configurations);
        qt().forAll(gen).check(configs -> {
            Map<String, ParameterizedClass> result = MemtableParams.expandDefinitions(configs);
            assertThat(result.keySet()).isEqualTo(configs.keySet());
            for (Map.Entry<String, InheritingClass> e : configs.entrySet()) {
                if (e.getValue().inherits == null) continue;
                ParameterizedClass parent = result.get(e.getValue().inherits);
                ParameterizedClass src = result.get(e.getKey());
                assertThat(src.class_name).isEqualTo(parent.class_name);
                assertThat(src.parameters).isEqualTo(parent.parameters);
            }
        });
    }

    @Test
    public void testInheritsNonExistent()
    {
        try
        {
            Map<String, ParameterizedClass> map = MemtableParams.expandDefinitions
            (
            ImmutableMap.of("one", new InheritingClass("two",
                                                       null,
                                                       ImmutableMap.of("extra", "value")),
                            "two", new InheritingClass("three",
                                                       null,
                                                       ImmutableMap.of("extra2", "value2")))
            );
            fail("Expected exception.");
        }
        catch (ConfigurationException e)
        {
            // expected
        }
    }

    @Test
    public void testInvalidLoops()
    {
        try
        {
            Map<String, ParameterizedClass> map = MemtableParams.expandDefinitions
            (
            ImmutableMap.of("one", new InheritingClass("two",
                                                       null,
                                                       ImmutableMap.of("extra", "value")),
                            "two", new InheritingClass("one",
                                                       null,
                                                       ImmutableMap.of("extra2", "value2")))
            );
            fail("Expected exception.");
        }
        catch (ConfigurationException e)
        {
            // expected
        }

        try
        {
            Map<String, ParameterizedClass> map = MemtableParams.expandDefinitions
            (
            ImmutableMap.of("one", new InheritingClass("two",
                                                       null,
                                                       ImmutableMap.of("extra", "value")),
                            "two", new InheritingClass("three",
                                                       null,
                                                       ImmutableMap.of("extra2", "value2")),
                            "three", new InheritingClass("one",
                                                         null,
                                                         ImmutableMap.of("extra3", "value3")))
            );
            fail("Expected exception.");
        }
        catch (ConfigurationException e)
        {
            // expected
        }
    }
}
