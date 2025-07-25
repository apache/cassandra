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

public class SetAuthCacheConfigTest extends CQLTester
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
        ToolRunner.ToolResult tool = ToolRunner.invokeNodetool("setauthcacheconfig");
        assertThat(tool.getExitCode()).isEqualTo(1);
        assertThat(tool.getStdout()).isEqualTo(wrapByDefaultNodetoolMessage("Missing required option: '--cache-name=cache-name'"));
        assertThat(tool.getCleanedStderr()).isEmpty();

        tool = ToolRunner.invokeNodetool("setauthcacheconfig", "--cache-name");
        assertThat(tool.getExitCode()).isEqualTo(1);
        assertThat(tool.getStdout()).isEqualTo(wrapByDefaultNodetoolMessage("Missing required parameter for option '--cache-name' (cache-name)"));
        assertThat(tool.getCleanedStderr()).isEmpty();

        tool = ToolRunner.invokeNodetool("setauthcacheconfig", "--cache-name", "wrong", "--validity-period", "1");
        assertThat(tool.getExitCode()).isEqualTo(1);
        assertThat(tool.getStdout()).isEqualTo(wrapByDefaultNodetoolMessage("Unknown cache name: wrong"));
        assertThat(tool.getCleanedStderr()).isEmpty();
    }

    @Test
    public void testNoOptionalParameters()
    {
        ToolRunner.ToolResult tool = ToolRunner.invokeNodetool("setauthcacheconfig", "--cache-name", "PermissionCache");
        assertThat(tool.getExitCode()).isEqualTo(1);
        assertThat(tool.getStdout()).isEqualTo(wrapByDefaultNodetoolMessage("At least one optional parameter need to be passed"));
        assertThat(tool.getCleanedStderr()).isEmpty();
    }

    @Test
    public void testBothEnableAndDisableActiveUpdate()
    {
        ToolRunner.ToolResult tool = ToolRunner.invokeNodetool("setauthcacheconfig",
                                                               "--cache-name", "PermissionCache",
                                                               "--enable-active-update",
                                                               "--disable-active-update");
        assertThat(tool.getExitCode()).isEqualTo(1);
        assertThat(tool.getStdout()).isEqualTo(wrapByDefaultNodetoolMessage("enable-active-update and disable-active-update cannot be used together"));
        assertThat(tool.getCleanedStderr()).isEmpty();
    }

    @Test
    public void testSetConfig()
    {
        assertSetConfig(AuthenticatedUser.permissionsCache, PermissionsCacheMBean.CACHE_NAME);

        PasswordAuthenticator passwordAuthenticator = (PasswordAuthenticator) DatabaseDescriptor.getAuthenticator();
        assertSetConfig(passwordAuthenticator.getCredentialsCache(), PasswordAuthenticator.CredentialsCacheMBean.CACHE_NAME);

        assertSetConfig(AuthorizationProxy.jmxPermissionsCache, AuthorizationProxy.JmxPermissionsCacheMBean.CACHE_NAME);

        assertSetConfig(AuthenticatedUser.networkPermissionsCache, NetworkPermissionsCacheMBean.CACHE_NAME);

        assertSetConfig(Roles.cache, RolesCacheMBean.CACHE_NAME);
    }

    @Test
    public void testSetConfigDisabled()
    {
        assertSetConfigDisabled(AuthenticatedUser.permissionsCache, PermissionsCacheMBean.CACHE_NAME);

        PasswordAuthenticator passwordAuthenticator = (PasswordAuthenticator) DatabaseDescriptor.getAuthenticator();
        assertSetConfigDisabled(passwordAuthenticator.getCredentialsCache(), PasswordAuthenticator.CredentialsCacheMBean.CACHE_NAME);

        assertSetConfigDisabled(AuthorizationProxy.jmxPermissionsCache, AuthorizationProxy.JmxPermissionsCacheMBean.CACHE_NAME);

        assertSetConfigDisabled(AuthenticatedUser.networkPermissionsCache, NetworkPermissionsCacheMBean.CACHE_NAME);

        assertSetConfigDisabled(Roles.cache, RolesCacheMBean.CACHE_NAME);
    }

    private void assertSetConfig(AuthCache<?, ?> authCache, String cacheName)
    {
        ToolRunner.ToolResult tool = ToolRunner.invokeNodetool("setauthcacheconfig",
                                                               "--cache-name", cacheName,
                                                               "--validity-period", "1",
                                                               "--update-interval", "2",
                                                               "--max-entries", "3",
                                                               "--disable-active-update");
        tool.assertOnCleanExit();
        assertThat(tool.getStdout()).isEqualTo("Changed Validity Period to 1\n" +
                                               "Changed Update Interval to 2\n" +
                                               "Changed Max Entries to 3\n" +
                                               "Changed Active Update to false\n");

        assertThat(authCache.getValidity()).isEqualTo(1);
        assertThat(authCache.getUpdateInterval()).isEqualTo(2);
        assertThat(authCache.getMaxEntries()).isEqualTo(3);
        assertThat(authCache.getActiveUpdate()).isFalse();
    }

    private void assertSetConfigDisabled(AuthCache<?, ?> authCache, String cacheName)
    {
        ToolRunner.ToolResult tool = ToolRunner.invokeNodetool("setauthcacheconfig",
                                                               "--cache-name", cacheName,
                                                               "--validity-period", "1",
                                                               "--update-interval", "-1",
                                                               "--max-entries", "3",
                                                               "--disable-active-update");
        tool.assertOnCleanExit();
        assertThat(tool.getStdout()).isEqualTo("Changed Validity Period to 1\n" +
                                               "Changed Update Interval to -1\n" +
                                               "Changed Max Entries to 3\n" +
                                               "Changed Active Update to false\n");
        // -1 means disabled and means update_interval will be assigned the value of validity_period
        assertThat(authCache.getValidity()).isEqualTo(1);
        assertThat(authCache.getUpdateInterval()).isEqualTo(1);
        assertThat(authCache.getMaxEntries()).isEqualTo(3);
        assertThat(authCache.getActiveUpdate()).isFalse();
    }

    private String wrapByDefaultNodetoolMessage(String s)
    {
        return "nodetool: " + s + "\nSee 'nodetool help' or 'nodetool help <command>'.\n";
    }
}
