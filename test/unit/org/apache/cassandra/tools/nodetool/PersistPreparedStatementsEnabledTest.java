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

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.tools.ToolRunner;

import static org.assertj.core.api.Assertions.assertThat;

public class PersistPreparedStatementsEnabledTest extends CQLTester
{
    @BeforeClass
    public static void setup() throws Exception
    {
        CQLTester.setUpClass();
        startJMXServer();
    }

    @Test
    public void testGetter()
    {
        // by default, we enable persisting prepared statements
        assertThat(DatabaseDescriptor.getPersistPreparedStatementsEnabled()).isTrue();
        ToolRunner.ToolResult tool = ToolRunner.invokeNodetool("getpersistpreparedstatementsenabled");
        tool.assertOnCleanExit();
        assertThat(tool.getStdout()).contains("true");
    }

    @Test
    public void testSetter()
    {
        assertThat(DatabaseDescriptor.getPersistPreparedStatementsEnabled()).isTrue();
        ToolRunner.ToolResult tool = ToolRunner.invokeNodetool("setpersistpreparedstatementsenabled", "false");
        tool.assertOnCleanExit();
        assertThat(DatabaseDescriptor.getPersistPreparedStatementsEnabled()).isFalse();
    }
}
