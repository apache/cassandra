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

import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.guardrails.Guardrails;
import org.apache.cassandra.tools.ToolRunner;

import static org.assertj.core.api.Assertions.assertThat;

public class GetGuardrailsConfigTest extends CQLTester
{
    @BeforeClass
    public static void setup() throws Exception
    {
        CQLTester.setUpClass();
        CQLTester.requireAuthentication();
        startJMXServer();
    }

    @Test
    public void testDefaultConfig()
    {
        // by default, none of the guardrail is enabled
        // guardrails by default is not enabled on superusers
        ToolRunner.ToolResult tool = ToolRunner.invokeNodetool("getguardrailsconfig");
        tool.assertOnCleanExit();
        assertThat(tool.getStdout()).isEqualTo("Guardrails Configuration:\n" +
                                               "guardrails applied on supuerusers\n" +
                                               "\tenabled: false\n" +
                                               '\n');
    }

    @Test
    public void testDefaultConfigFullConfig()
    {
        // by default, none of the guardrail is enabled
        // guardrails by default is not enabled on superusers
        ToolRunner.ToolResult tool = ToolRunner.invokeNodetool("getguardrailsconfig", "--full");
        tool.assertOnCleanExit();
        assertThat(tool.getStdout()).isEqualTo("Guardrails Configuration:\n" +
                                               "guardrails applied on supuerusers\n" +
                                               "\tenabled: false\n" +
                                               "total number of user keyspaces\n" +
                                               "\twarning threshold(maximum): -1\n" +
                                               "\tfailing threashold(maximum): -1\n" +
                                               "total number of tables on user keyspaces\n" +
                                               "\twarning threshold(maximum): -1\n" +
                                               "\tfailing threashold(maximum): -1\n" +
                                               "number of columns per table\n" +
                                               "\twarning threshold(maximum): -1\n" +
                                               "\tfailing threashold(maximum): -1\n" +
                                               "number of secondary indexes per table\n" +
                                               "\twarning threshold(maximum): -1\n" +
                                               "\tfailing threashold(maximum): -1\n" +
                                               "ability to create secondary indexes\n" +
                                               "\tenabled: true\n" +
                                               "number of materialized views per table\n" +
                                               "\twarning threshold(maximum): -1\n" +
                                               "\tfailing threashold(maximum): -1\n" +
                                               "usage of certain table properties\n" +
                                               "\twarning values: null\n" +
                                               "\tignored values: null\n" +
                                               "\tdisallowed values: null\n" +
                                               "ability to use user-provided timestamps\n" +
                                               "\tenabled: true\n" +
                                               "ability to use GROUP BY\n" +
                                               "\tenabled: true\n" +
                                               "ability to use DROP and TRUNCATE TABLE\n" +
                                               "\tenabled: true\n" +
                                               "ability to do bulk load\n" +
                                               "\tenabled: true\n" +
                                               "ability to execute DDL statements\n" +
                                               "\tenabled: true\n" +
                                               "ability to execute DCL statements\n" +
                                               "\tenabled: true\n" +
                                               "ability to turn off compression\n" +
                                               "\tenabled: true\n" +
                                               "ability to create new COMPACT STORAGE tables\n" +
                                               "\tenabled: true\n" +
                                               "number of elements returned within page\n" +
                                               "\twarning threshold(maximum): -1\n" +
                                               "\tfailing threashold(maximum): -1\n" +
                                               "number of partition keys in the IN clause\n" +
                                               "\twarning threshold(maximum): -1\n" +
                                               "\tfailing threashold(maximum): -1\n" +
                                               "ability on operate lists that require read before write\n" +
                                               "\tenabled: true\n" +
                                               "ability to execute statement with ALLOW FILTERING\n" +
                                               "\tenabled: true\n" +
                                               "number of restrictions created by a cartesian product of a CQL's IN query\n" +
                                               "\twarning threshold(maximum): -1\n" +
                                               "\tfailing threashold(maximum): -1\n" +
                                               "usage on read consistency levels\n" +
                                               "\twarning values: null\n" +
                                               "\tignored values: null\n" +
                                               "\tdisallowed values: null\n" +
                                               "usage on write consistency levels\n" +
                                               "\twarning values: null\n" +
                                               "\tignored values: null\n" +
                                               "\tdisallowed values: null\n" +
                                               "size of a collection\n" +
                                               "\twarning threshold(maximum): null\n" +
                                               "\tfailing threashold(maximum): null\n" +
                                               "number of items of a collection\n" +
                                               "\twarning threshold(maximum): -1\n" +
                                               "\tfailing threashold(maximum): -1\n" +
                                               "number of fields on each UDT\n" +
                                               "\twarning threshold(maximum): -1\n" +
                                               "\tfailing threashold(maximum): -1\n" +
                                               "data disk usage percentage on the local node, used by a periodic task to calculate and propagate that status\n" +
                                               "\twarning threshold(max percentage): -1%\n" +
                                               "\tfailing threashold(max percentage): -1%\n" +
                                               "number of minimum replication factor\n" +
                                               "\twarning threshold(minimum): -1\n" +
                                               "\tfailing threashold(minimum): -1\n" +
                                               '\n');
    }

    @Test
    public void testSetColumnsPerTableThresholds()
    {
        // set to some new MaxThreshold and the change should be reflected in the nodetool return
        Guardrails.instance.setColumnsPerTableThreshold(3, 3);

        ToolRunner.ToolResult tool = ToolRunner.invokeNodetool("getguardrailsconfig");
        tool.assertOnCleanExit();
        assertThat(tool.getStdout()).isEqualTo("Guardrails Configuration:\n" +
                                               "guardrails applied on supuerusers\n" +
                                               "\tenabled: false\n" +
                                               "number of columns per table\n" +
                                               "\twarning threshold(maximum): 3\n" +
                                               "\tfailing threashold(maximum): 3\n" +
                                               '\n');
        // reset
        Guardrails.instance.setColumnsPerTableThreshold(-1, -1);
    }

    @Test
    public void testDisableDDL()
    {
        // set to some new EnabledFlag and the change should be reflected in the nodetool return
        Guardrails.instance.setDDLEnabled(false);

        ToolRunner.ToolResult tool = ToolRunner.invokeNodetool("getguardrailsconfig");
        tool.assertOnCleanExit();
        assertThat(tool.getStdout()).isEqualTo("Guardrails Configuration:\n" +
                                               "guardrails applied on supuerusers\n" +
                                               "\tenabled: false\n" +
                                               "ability to execute DDL statements\n"+
                                               "\tenabled: false\n" +
                                               '\n');
        // reset
        Guardrails.instance.setDDLEnabled(true);
    }

    @Test
    public void testEnableGuardrailsOnSuperuser()
    {
        // Guardrails applied on superusers should always stay in the return
        Guardrails.instance.setGuardrailsOnSuperuserEnabled(true);

        ToolRunner.ToolResult tool = ToolRunner.invokeNodetool("getguardrailsconfig");
        tool.assertOnCleanExit();
        assertThat(tool.getStdout()).isEqualTo("Guardrails Configuration:\n" +
                                               "guardrails applied on supuerusers\n" +
                                               "\tenabled: true\n" +
                                               '\n');
        // reset
        Guardrails.instance.setGuardrailsOnSuperuserEnabled(false);
    }
}
