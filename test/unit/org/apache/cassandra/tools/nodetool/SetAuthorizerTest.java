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
import org.apache.cassandra.auth.AuthCacheService;
import org.apache.cassandra.auth.AuthKeyspace;
import org.apache.cassandra.auth.AuthSchemaChangeListener;
import org.apache.cassandra.auth.AuthTestUtils;
import org.apache.cassandra.auth.IRoleManager;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.tools.ToolRunner;

import static org.assertj.core.api.Assertions.assertThat;

public class SetAuthorizerTest extends CQLTester
{
    private static final String user = "user";
    private static final String pass = "12345";
    private String keyspace = "test";
    private String table = "perf";

    @BeforeClass
    public static void setup() throws Exception
    {
        startJMXServer();
        SchemaLoader.createKeyspace(SchemaConstants.AUTH_KEYSPACE_NAME,
                                    KeyspaceParams.simple(1),
                                    Iterables.toArray(AuthKeyspace.metadata().tables, TableMetadata.class));
        requireNetwork();
        CQLTester.setUpClass();
    }

    @Test
    public void testSetAuthorizerNull()
    {
        ToolRunner.ToolResult tool = ToolRunner.invokeNodetool("setauthorizer", "");
        assertThat(tool.getExitCode()).isEqualTo(1);
        assertThat(tool.getStdout().contains("Required parameters are missing"));
    }

    @Test
    public void testSetWrongAuthorizer()
    {
        ToolRunner.ToolResult tool = ToolRunner.invokeNodetool("setauthorizer", "xyz");
        assertThat(tool.getExitCode()).isEqualTo(2);
        assertThat(tool.getCleanedStderr()).isNotEmpty();
    }

    @Test
    public void testSetAuthorizer() throws Throwable
    {
        requiredSetup();

        useUser(user, pass);
        assertRowsNet(sessionNet().execute(String.format("SELECT * FROM %s.%s", keyspace, table)), row(1, 100));

        String authorizer = "org.apache.cassandra.auth.CassandraAuthorizer";
        ToolRunner.ToolResult tool = ToolRunner.invokeNodetool("setauthorizer", authorizer);
        assertThat(tool.getExitCode()).isEqualTo(0);
        assertThat(tool.getCleanedStderr()).isEmpty();
        assertThat(getAuthorizer()).isEqualTo(authorizer);
        assertUnauthorizedQuery(String.format("User user has no SELECT permission on <table %s.%s> or any of its parents", keyspace, table),
                                String.format("SELECT * FROM %s.%s", keyspace, table));
    }

    private void requiredSetup()
    {
        setupAuth();
        createRole();
        createSchema();
    }

    private void setupAuth(){
        DatabaseDescriptor.setAuthenticator(new AuthTestUtils.LocalPasswordAuthenticator());
        IRoleManager roleManager =  new AuthTestUtils.LocalCassandraRoleManager()
        {
            public void setup()
            {
                loadRoleStatement();
                QueryProcessor.executeInternal(createDefaultRoleQuery());
            }
        };
        DatabaseDescriptor.setRoleManager(roleManager);
        DatabaseDescriptor.getRoleManager().setup();
        DatabaseDescriptor.getAuthenticator().setup();
        Schema.instance.registerListener(new AuthSchemaChangeListener());
        AuthCacheService.initializeAndRegisterCaches();
    }

    private void createRole() {
        useSuperUser();
        sessionNet().execute(String.format("CREATE ROLE %s WITH LOGIN = TRUE AND password='%s'", user, pass));
    }

    private void createSchema() {
        sessionNet().execute(String.format("CREATE KEYSPACE %s WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}", keyspace));
        sessionNet().execute(String.format("CREATE TABLE %s.%s (pk int PRIMARY KEY, val int)", keyspace, table));
        sessionNet().execute(String.format("INSERT INTO %s.%s (pk, val) VALUES(%d, %d)", keyspace, table, 1, 100));
    }

    private String getAuthorizer()
    {
        ToolRunner.ToolResult tool = ToolRunner.invokeNodetool("getauthorizer");
        tool.assertOnCleanExit();
        return tool.getStdout().trim();
    }
}
