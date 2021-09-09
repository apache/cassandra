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

package org.apache.cassandra.tools.nodetool.stats;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.util.Arrays;
import java.util.Map;
import java.util.TreeMap;
import java.util.regex.Pattern;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Tests for {@link StatsPrinter}.
 */
public class StatsPrinterTest
{
    @Test
    public void testPrintEmpty() throws Throwable
    {
        TestCase.create()
                .validateJson("{}")
                .validateYaml("{}");
    }

    @Test
    public void testPrintSimpleTypes() throws Throwable
    {
        TestCase.create("null", null)
                .validateJson("{\"null\":null}")
                .validateYaml("'null': null");
        TestCase.create("string", "")
                .validateJson("{\"string\":\"\"}")
                .validateYaml("string: ''");
        TestCase.create("string", "test")
                .validateJson("{\"string\":\"test\"}")
                .validateYaml("string: test");
        TestCase.create("bool", true)
                .validateJson("{\"bool\":true}")
                .validateYaml("bool: true");
        TestCase.create("bool", false)
                .validateJson("{\"bool\":false}")
                .validateYaml("bool: false");
        TestCase.create("int", 1)
                .validateJson("{\"int\":1}")
                .validateYaml("int: 1");
        TestCase.create("long", 1L)
                .validateJson("{\"long\":1}")
                .validateYaml("long: 1");
        TestCase.create("float", 1.1f)
                .validateJson("{\"float\":1.1}")
                .validateYaml("float: 1.1");
        TestCase.create("double", 1.1d)
                .validateJson("{\"double\":1.1}")
                .validateYaml("double: 1.1");
    }

    @Test
    public void testPrintArrays() throws Throwable
    {
        TestCase.create("ints", new int[]{ -1, 0, 1 })
                .validateJson("{\"ints\":[-1, 0, 1]}")
                .validateYaml("ints:\n- -1\n- 0\n- 1");
        TestCase.create("longs", new long[]{ -1L, 0L, 1L })
                .validateJson("{\"longs\":[-1, 0, 1]}")
                .validateYaml("longs:\n- -1\n- 0\n- 1");
        TestCase.create("floats", new float[]{ -1.1f, 0.1f, 1.1f })
                .validateJson("{\"floats\":[-1.1, 0.1, 1.1]}")
                .validateYaml("floats:\n- -1.1\n- 0.1\n- 1.1");
        TestCase.create("doubles", new double[]{ -1.1d, 0.1d, 1.1d })
                .validateJson("{\"doubles\":[-1.1, 0.1, 1.1]}")
                .validateYaml("doubles:\n- -1.1\n- 0.1\n- 1.1");
    }

    @Test
    public void testPrintLists() throws Throwable
    {
        TestCase.create("ints", Arrays.asList(-1, 0, 1))
                .validateJson("{\"ints\":[-1,0,1]}")
                .validateYaml("ints:\n- -1\n- 0\n- 1");
        TestCase.create("longs", Arrays.asList(-1L, 0L, 1L))
                .validateJson("{\"longs\":[-1,0,1]}")
                .validateYaml("longs:\n- -1\n- 0\n- 1");
        TestCase.create("floats", Arrays.asList(-1.1f, 0.1f, 1.1f))
                .validateJson("{\"floats\":[-1.1,0.1,1.1]}")
                .validateYaml("floats:\n- -1.1\n- 0.1\n- 1.1");
        TestCase.create("doubles", Arrays.asList(-1.1d, 0.1d, 1.1d))
                .validateJson("{\"doubles\":[-1.1,0.1,1.1]}")
                .validateYaml("doubles:\n- -1.1\n- 0.1\n- 1.1");
    }

    @Test
    public void testPrintMultiple() throws Throwable
    {
        TestCase.create()
                .put("string", "test")
                .put("array", new int[]{ -1, 0, 1 })
                .put("list", Arrays.asList(-1, 0, 1))
                .validateJson("{\"array\":[-1,0,1],\"list\":[-1,0,1],\"string\":\"test\"}")
                .validateYaml("array:\n- -1\n- 0\n- 1\nlist:\n- -1\n- 0\n- 1\nstring: test");
    }

    private static class TestCase implements StatsHolder
    {
        private static final StatsPrinter<TestCase> jsonPrinter = new StatsPrinter.JsonPrinter<>();
        private static final StatsPrinter<TestCase> yamlPrinter = new StatsPrinter.YamlPrinter<>();

        private final Map<String, Object> map;

        private TestCase()
        {
            this.map = new TreeMap<>();
        }

        public static TestCase create()
        {
            return new TestCase();
        }

        public static TestCase create(String key, Object value)
        {
            return create().put(key, value);
        }

        public TestCase put(String key, Object value)
        {
            map.put(key, value);
            return this;
        }

        @Override
        public Map<String, Object> convert2Map()
        {
            return map;
        }

        private String print(StatsPrinter<TestCase> printer) throws IOException
        {
            try (ByteArrayOutputStream stream = new ByteArrayOutputStream())
            {
                printer.print(this, new PrintStream(stream));
                return stream.toString();
            }
        }

        private static String cleanJson(String json)
        {
            return Pattern.compile("\n|\\s*").matcher(json).replaceAll("");
        }

        TestCase validateJson(String expectedJson) throws IOException
        {
            String expected = cleanJson(expectedJson);
            String actual = cleanJson(print(jsonPrinter));
            assertEquals(expected, actual);
            return this;
        }

        @SuppressWarnings("UnusedReturnValue")
        TestCase validateYaml(String expectedYaml) throws IOException
        {
            assertEquals(expectedYaml + "\n\n", print(yamlPrinter));
            return this;
        }
    }
}
