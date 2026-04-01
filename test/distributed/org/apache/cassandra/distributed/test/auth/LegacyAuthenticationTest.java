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

package org.apache.cassandra.distributed.test.auth;

import java.io.IOException;

import com.datastax.driver.core.PlainTextAuthProvider;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.exceptions.AuthenticationException;

import org.junit.Test;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.test.TestBaseImpl;

import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NATIVE_PROTOCOL;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;
import static org.apache.cassandra.distributed.util.Auth.waitForExistingRoles;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * Tests for legacy (pre-negotiation) authentication behavior.
 * These tests verify authentication works correctly when:
 * 1. No authenticator_negotiation config is present (legacy single authenticator)
 * 2. authenticator_negotiation.enabled is explicitly set to false
 */
public class LegacyAuthenticationTest extends TestBaseImpl
{
    /**
     * Tests legacy PasswordAuthenticator configuration (no negotiation config).
     * Verifies that authentication is required and works correctly.
     */
    @Test
    public void testLegacyPasswordAuthenticator() throws IOException
    {
        try (Cluster cluster = builder().withNodes(1)
                                        .withConfig(config -> config.with(NETWORK, GOSSIP, NATIVE_PROTOCOL)
                                                                    .set("authenticator", "PasswordAuthenticator"))
                                        .start())
        {
            waitForExistingRoles(cluster.get(1));
            
            // Should be able to connect with valid credentials
            com.datastax.driver.core.Cluster.Builder authBuilder = 
                com.datastax.driver.core.Cluster.builder()
                    .addContactPoint("127.0.0.1")
                    .withAuthProvider(new PlainTextAuthProvider("cassandra", "cassandra"));

            try (com.datastax.driver.core.Cluster c = authBuilder.build(); 
                 Session session = c.connect())
            {
                assertNotNull("Should connect with valid credentials", session);
                assertCurrentRole(session, "cassandra");
                session.execute("SELECT * FROM system.local");
            }
            
            // Should NOT be able to connect without credentials
            com.datastax.driver.core.Cluster.Builder noAuthBuilder = 
                com.datastax.driver.core.Cluster.builder()
                    .addContactPoint("127.0.0.1");

            try (com.datastax.driver.core.Cluster c = noAuthBuilder.build())
            {
                c.connect();
                org.junit.Assert.fail("Should not be able to connect without credentials");
            }
            catch (AuthenticationException e)
            {
                // Expected - authentication required
            }
        }
    }

    /**
     * Tests legacy AllowAllAuthenticator configuration (no negotiation config).
     * Verifies that no authentication is required.
     */
    @Test
    public void testLegacyAllowAllAuthenticator() throws IOException
    {
        try (Cluster cluster = builder().withNodes(1)
                                        .withConfig(config -> config.with(NETWORK, GOSSIP, NATIVE_PROTOCOL)
                                                                    .set("authenticator", "AllowAllAuthenticator"))
                                        .start())
        {
            // Should be able to connect without credentials
            com.datastax.driver.core.Cluster.Builder builder = 
                com.datastax.driver.core.Cluster.builder()
                    .addContactPoint("127.0.0.1");

            try (com.datastax.driver.core.Cluster c = builder.build(); 
                 Session session = c.connect())
            {
                assertNotNull("Should connect without credentials", session);
                assertCurrentRole(session, "anonymous");
                session.execute("SELECT * FROM system.local");
            }
        }
    }

    /**
     * Tests that when authenticator_negotiation.enabled is explicitly set to false,
     * the system behaves like legacy mode (uses the single authenticator config).
     */
    @Test
    public void testNegotiationExplicitlyDisabled() throws IOException
    {
        try (Cluster cluster = builder().withNodes(1)
                                        .withConfig(config -> {
                                            config.with(NETWORK, GOSSIP, NATIVE_PROTOCOL)
                                                  .set("authenticator", "PasswordAuthenticator");
                                            
                                            // Explicitly disable negotiation
                                            config.set("authenticator_negotiation", 
                                                      new java.util.HashMap<String, Object>() {{
                                                          put("enabled", false);
                                                      }});
                                        })
                                        .start())
        {
            waitForExistingRoles(cluster.get(1));
            
            // Should behave exactly like legacy PasswordAuthenticator
            com.datastax.driver.core.Cluster.Builder authBuilder = 
                com.datastax.driver.core.Cluster.builder()
                    .addContactPoint("127.0.0.1")
                    .withAuthProvider(new PlainTextAuthProvider("cassandra", "cassandra"));

            try (com.datastax.driver.core.Cluster c = authBuilder.build(); 
                 Session session = c.connect())
            {
                assertNotNull("Should connect with valid credentials", session);
                assertCurrentRole(session, "cassandra");
                session.execute("SELECT * FROM system.local");
            }
        }
    }

    /**
     * Helper method to verify the current authenticated user identity.
     */
    private void assertCurrentRole(Session session, String expectedRole)
    {
        String actualRole;
        try
        {
            com.datastax.driver.core.ResultSet rs = session.execute("LIST ROLES");
            actualRole = rs.one().getString("role");
        }
        catch (com.datastax.driver.core.exceptions.UnauthorizedException e)
        {
            actualRole = e.getMessage().contains("not anonymous") ? "anonymous" : null;
        }
        
        assertEquals("Current authenticated role", expectedRole, actualRole);
    }
}
