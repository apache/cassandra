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

package org.apache.cassandra.tools.nodetool;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.tools.ToolRunner;

/**
 * Verifies backward compatibility of the {@code -pp/--print-port} option placement.
 * For each command in {@code PRINT_PORT_COMMANDS}, both syntaxes must succeed:
 * <ul>
 *   <li>{@code nodetool <command> --print-port} (natural picocli parsing via {@link PrintPortMixin})</li>
 *   <li>{@code nodetool --print-port <command>} (backward-compatible relocation)</li>
 * </ul>
 * Commands that do not accept {@code --print-port} must reject it.
 */
@RunWith(Parameterized.class)
public class NodetoolPrintPortBackwardCompatibilityTest extends CQLTester
{
    private static final String[] DEFAULT_STRING_ARRAY = new String[0];

    @Parameterized.Parameter
    public String command;

    @Parameterized.Parameter(1)
    public String[] extraArgs;

    @Parameterized.Parameters(name = "{0}")
    public static Collection<Object[]> data()
    {
        return List.of(
            new Object[]{ "status", DEFAULT_STRING_ARRAY },
            new Object[]{ "ring", DEFAULT_STRING_ARRAY },
            new Object[]{ "netstats", DEFAULT_STRING_ARRAY },
            new Object[]{ "gossipinfo", DEFAULT_STRING_ARRAY },
            new Object[]{ "failuredetector", DEFAULT_STRING_ARRAY },
            new Object[]{ "describecluster", DEFAULT_STRING_ARRAY },
            new Object[]{ "describering", new String[]{ KEYSPACE } },
            new Object[]{ "getendpoints", new String[]{ KEYSPACE, "pp_compat_tbl", "key1" } }
        );
    }

    @BeforeClass
    public static void setup() throws Exception
    {
        requireNetwork();
        startJMXServer();
    }

    @Before
    public void createTestData()
    {
        schemaChange("CREATE TABLE IF NOT EXISTS " + KEYSPACE + ".pp_compat_tbl (k text PRIMARY KEY)");
    }

    @Test
    public void testPrintPortAfterSubcommand()
    {
        assertCleanExit(args(command, extraArgs));
    }

    @Test
    public void testPrintPortBeforeSubcommand()
    {
        assertCleanExit(args("--print-port", command, extraArgs));
    }

    @Test
    public void testShortOptionBeforeSubcommand()
    {
        assertCleanExit(args("-pp", command, extraArgs));
    }

    private static void assertCleanExit(String[] args)
    {
        ToolRunner.ToolResult result = ToolRunner.invokeNodetoolInJvm(args);
        result.assertOnCleanExit();
    }

    private static String[] args(String first, String[] middle)
    {
        List<String> list = new ArrayList<>();
        list.add(first);
        list.addAll(List.of(middle));
        list.add("--print-port");
        return list.toArray(DEFAULT_STRING_ARRAY);
    }

    private static String[] args(String first, String second, String[] rest)
    {
        List<String> list = new ArrayList<>();
        list.add(first);
        list.add(second);
        list.addAll(List.of(rest));
        return list.toArray(DEFAULT_STRING_ARRAY);
    }
}