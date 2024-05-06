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

import java.util.HashMap;
import java.util.Map;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.auth.IAuthenticator;
import org.apache.cassandra.auth.PasswordAuthenticator;
import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.exceptions.AuthenticationException;
import org.apache.cassandra.tools.ToolRunner;
import org.apache.cassandra.utils.FBUtilities;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class SetAuthEnforcementFlagTest extends CQLTester
{
    @BeforeClass
    public static void setup() throws Exception
    {
        CQLTester.setUpClass();
        startJMXServer();
        requireNetwork();
    }

    @Test
    public void testSetAuthEnforcementFlag()
    {
        assertEquals(DatabaseDescriptor.getAuthEnforcementFlag(), Config.AuthEnforcementFlag.none);
        // set to PasswordAuthenticator
        IAuthenticator authenticator = FBUtilities.newAuthenticator(PasswordAuthenticator.class.getSimpleName());
        authenticator.setup();
        assertTrue(authenticator.requireAuthentication());

        DatabaseDescriptor.setAuthenticator(authenticator);
        Map<String, String> credentials = new HashMap<>();
        credentials.put(PasswordAuthenticator.USERNAME_KEY, "t");
        credentials.put(PasswordAuthenticator.PASSWORD_KEY, "t");
        // should receive AuthenticationException
        try
        {
            authenticator.legacyAuthenticate(credentials);
            throw new RuntimeException("Expecting AuthenticationException");
        }
        catch (AuthenticationException e)
        {
            // expected
        }

        ToolRunner.ToolResult tool = ToolRunner.invokeNodetool("setauthenforcementflag", "soft");
        tool.assertOnCleanExit();
        assertEquals(DatabaseDescriptor.getAuthEnforcementFlag(), Config.AuthEnforcementFlag.soft);
        // should now be able to log in with any user/pw
        authenticator.legacyAuthenticate(credentials);
    }
}
