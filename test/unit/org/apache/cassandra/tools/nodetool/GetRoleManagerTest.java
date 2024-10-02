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

import com.google.common.collect.Iterables;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.auth.AuthKeyspace;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.tools.ToolRunner;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertNotNull;

public class GetRoleManagerTest extends CQLTester
{
    @BeforeClass
    public static void setup() throws Exception
    {
        startJMXServer();
        SchemaLoader.createKeyspace(SchemaConstants.AUTH_KEYSPACE_NAME,
                                    KeyspaceParams.simple(1),
                                    Iterables.toArray(AuthKeyspace.metadata().tables, TableMetadata.class));
    }

    @Test
    public void testGetRoleManager()
    {
        assertNotNull(getRoleManager());
    }

    @Test
    public void testGetUpdatedRoleManager()
    {
        String roleManager = "org.apache.cassandra.auth.CassandraRoleManager";
        setRoleManager(roleManager);
        assertThat(getRoleManager()).isEqualTo(roleManager);
    }

    private String getRoleManager()
    {
        ToolRunner.ToolResult tool = ToolRunner.invokeNodetool("getrolemanager");
        tool.assertOnCleanExit();
        return tool.getStdout().trim();
    }

    private void setRoleManager(String roleManager)
    {
        ToolRunner.invokeNodetool("setrolemanager",
                                  roleManager).assertOnCleanExit();
    }
}
