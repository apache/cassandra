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

package org.apache.cassandra.tools.cqlsh;

import java.io.IOException;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.tools.ToolRunner;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class CqlshTest extends CQLTester
{
    private ToolRunner.Runners runner = new ToolRunner.Runners();
    
    @BeforeClass
    public static void setUp()
    {
        requireNetwork();
    }

    @Test
    public void testKeyspaceRequired() throws IOException
    {
        ToolRunner tool = runner.invokeCqlsh("SELECT * FROM test");
        assertTrue("Tool stderr: " +  tool.getStderr(), tool.getStderr().contains("No keyspace has been specified"));
        assertEquals(2, tool.getExitCode());
    }
    
}
