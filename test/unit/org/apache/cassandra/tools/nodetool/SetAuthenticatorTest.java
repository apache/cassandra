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
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.assertj.core.api.Assertions.assertThatNoException;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.exceptions.AuthenticationException;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.auth.AuthKeyspace;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.tools.ToolRunner;

public class SetAuthenticatorTest extends CQLTester
{

    @BeforeClass
    public static void setup() throws Throwable
    {
        startJMXServer();
        SchemaLoader.createKeyspace(SchemaConstants.AUTH_KEYSPACE_NAME,
                                    KeyspaceParams.simple(1),
                                    Iterables.toArray(AuthKeyspace.metadata().tables, TableMetadata.class));
        requireNetwork();
    }

    @Test
    public void testSetAuthenticatorNull()
    {
        ToolRunner.ToolResult tool = ToolRunner.invokeNodetool("setauthenticator", "");
        assertThat(tool.getExitCode()).isEqualTo(1);
        assertThat(tool.getStdout().contains("Required parameters are missing"));
    }

    @Test
    public void testSetWrongAuthenticator()
    {
        ToolRunner.ToolResult tool = ToolRunner.invokeNodetool("setauthenticator", "xyz");
        assertThat(tool.getExitCode()).isEqualTo(2);
        assertThat(tool.getCleanedStderr()).isNotEmpty();
    }

    @Test
    public void testSetAuthenticator()
    {
        Cluster cluster = Cluster.builder()
                                 .addContactPoints(nativeAddr)
                                 .withPort(nativePort)
                                 .build();
        assertThatNoException().isThrownBy(cluster::connect);

        String authenticator = "org.apache.cassandra.auth.PasswordAuthenticator";
        ToolRunner.ToolResult tool = ToolRunner.invokeNodetool("setauthenticator", authenticator);
        assertThat(tool.getExitCode()).isEqualTo(0);
        assertThat(tool.getCleanedStderr()).isEmpty();
        assertThat(getAuthenticator()).isEqualTo(authenticator);

        cluster = Cluster.builder()
                         .addContactPoints(nativeAddr)
                         .withPort(nativePort)
                         .build();;
        assertThatExceptionOfType(AuthenticationException.class).isThrownBy(cluster::connect);
    }

    private String getAuthenticator()
    {
        ToolRunner.ToolResult tool = ToolRunner.invokeNodetool("getauthenticator");
        tool.assertOnCleanExit();
        return tool.getStdout().trim();
    }
}
