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

import org.apache.cassandra.auth.AuthCache;
import org.apache.cassandra.auth.AuthenticatedUser;
import org.apache.cassandra.auth.NetworkPermissionsCacheMBean;
import org.apache.cassandra.auth.PasswordAuthenticator;
import org.apache.cassandra.auth.PermissionsCacheMBean;
import org.apache.cassandra.auth.Roles;
import org.apache.cassandra.auth.RolesCacheMBean;
import org.apache.cassandra.auth.jmx.AuthorizationProxy;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.tools.ToolRunner;

import static org.assertj.core.api.Assertions.assertThat;

public class GetAuthCacheConfigTest extends CQLTester
{
    @BeforeClass
    public static void setup() throws Exception
    {
        CQLTester.requireAuthentication();
        requireNetwork();
        startJMXServer();
    }

    @Test
    public void testInvalidCacheName()
    {
        ToolRunner.ToolResult tool = ToolRunner.invokeNodetool("getauthcacheconfig");
        assertThat(tool.getExitCode()).isEqualTo(1);
        assertThat(tool.getStdout()).isEqualTo(wrapByDefaultNodetoolMessage("Missing required option: '--cache-name=cache-name'"));
        assertThat(tool.getCleanedStderr()).isEmpty();

        tool = ToolRunner.invokeNodetool("getauthcacheconfig", "--cache-name");
        assertThat(tool.getExitCode()).isEqualTo(1);
        assertThat(tool.getStdout()).isEqualTo(wrapByDefaultNodetoolMessage("Missing required parameter for option '--cache-name' (cache-name)"));
        assertThat(tool.getCleanedStderr()).isEmpty();

        tool = ToolRunner.invokeNodetool("getauthcacheconfig", "--cache-name", "wrong");
        assertThat(tool.getExitCode()).isEqualTo(1);
        assertThat(tool.getStdout()).isEqualTo(wrapByDefaultNodetoolMessage("Unknown cache name: wrong"));
        assertThat(tool.getCleanedStderr()).isEmpty();
    }

    @Test
    public void testGetConfig()
    {
        assertGetConfig(AuthenticatedUser.permissionsCache, PermissionsCacheMBean.CACHE_NAME);

        PasswordAuthenticator passwordAuthenticator = (PasswordAuthenticator) DatabaseDescriptor.getAuthenticator();
        assertGetConfig(passwordAuthenticator.getCredentialsCache(), PasswordAuthenticator.CredentialsCacheMBean.CACHE_NAME);

        assertGetConfig(AuthorizationProxy.jmxPermissionsCache, AuthorizationProxy.JmxPermissionsCacheMBean.CACHE_NAME);

        assertGetConfig(AuthenticatedUser.networkPermissionsCache, NetworkPermissionsCacheMBean.CACHE_NAME);

        assertGetConfig(Roles.cache, RolesCacheMBean.CACHE_NAME);
    }

    @SuppressWarnings("SingleCharacterStringConcatenation")
    private void assertGetConfig(AuthCache<?, ?> authCache, String cacheName)
    {
        ToolRunner.ToolResult tool = ToolRunner.invokeNodetool("getauthcacheconfig", "--cache-name", cacheName);
        tool.assertOnCleanExit();
        assertThat(tool.getStdout()).isEqualTo("Validity Period: " + authCache.getValidity() + "\n" +
                                               "Update Interval: " + authCache.getUpdateInterval() + "\n" +
                                               "Max Entries: " + authCache.getMaxEntries() + "\n" +
                                               "Active Update: " + authCache.getActiveUpdate() + "\n");
    }

    private String wrapByDefaultNodetoolMessage(String s)
    {
        return "nodetool: " + s + "\nSee 'nodetool help' or 'nodetool help <command>'.\n";
    }
}
