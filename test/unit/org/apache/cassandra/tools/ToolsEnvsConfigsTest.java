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

package org.apache.cassandra.tools;

import java.util.Collections;

import com.google.common.collect.ImmutableMap;

import org.junit.Test;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.tools.ToolRunner.ToolResult;

import static org.junit.Assert.assertTrue;

public class ToolsEnvsConfigsTest
{
    //Some JDK can output env info on stdout/err. Check we can clean them
    @Test
    public void testJDKEnvInfoDefaultCleaners()
    {
        ToolResult tool = ToolRunner.invoke(ImmutableMap.of("_JAVA_OPTIONS", "-Djava.net.preferIPv4Stack=true"),
                                            null,
                                            CQLTester.buildNodetoolArgs(Collections.emptyList()));
        assertTrue("Cleaned Stderr was not empty: " + tool.getCleanedStderr(), tool.getCleanedStderr().isEmpty());
    }
}
