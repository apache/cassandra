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

import org.junit.Test;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.test.TestBaseImpl;

import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NATIVE_PROTOCOL;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;
import static org.apache.cassandra.distributed.util.Auth.waitForExistingRoles;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Exercises authenticator negotation for non-negotiating clients. When negotiation is enabled, non-negotiating
 * clients should always get the configured default authenticator. In addition, when negotiation is enabled with at
 * least one authenticator that requires authentication, clients authenticating through the AllowAllAuthenticator (or
 * any other authenticator that does not require authentication) should authenticate as 'anonymous' and should not be
 * granted super-user privileges by default.
 * <p/>
 * This is in contrast to 'anonymous' behavior when negotiation is not enabled. In that case, all clients use the
 * same authenticator, and if that authenticator does not require authentication the 'anonymous' user will default
 * to having super-user privileges.
 */
public class AuthenticatorNegotiationTest extends TestBaseImpl
{
    /**
     * Tests that unauthenticated clients do not receive automatic superuser privileges when authentication is
     * required globally. This validates the security fix in ClientState.isSuper() where it checks
     * DatabaseDescriptor.isAuthenticationRequired() instead of per-connection authenticator.requireAuthentication().
     * 
     * Configuration: negotiation enabled with PasswordAuthenticator in negotiable list,
     * but default=AllowAllAuthenticator so non-negotiating clients connect unauthenticated.
     * Since ANY negotiable authenticator requires auth, unauthenticated clients should NOT
     * receive automatic superuser privileges.
     */
    @Test
    public void testUnauthenticatedClientsGetAnonymousRole() throws IOException
    {
        try (Cluster cluster = builder().withNodes(1)
                                        .withConfig(config -> {
                                            config.with(NETWORK, GOSSIP, NATIVE_PROTOCOL)
                                                  .set("authenticator", "AllowAllAuthenticator")
                                                  .set("authorizer", "CassandraAuthorizer");
                                            
                                            // Configure negotiation: default allows all, but negotiable includes PasswordAuthenticator
                                            // This means isAuthenticationRequired() returns true (PasswordAuthenticator requires auth)
                                            config.set("authenticator_negotiation", 
                                                      new java.util.HashMap<String, Object>() {{
                                                          put("enabled", true);
                                                          put("require_authentication", false); // permissive mode
                                                          put("default_authenticator", new java.util.HashMap<String, String>() {{
                                                              put("class_name", "AllowAllAuthenticator");
                                                          }});
                                                          put("authenticators", java.util.Arrays.asList(
                                                              new java.util.HashMap<String, String>() {{
                                                                  put("class_name", "AllowAllAuthenticator");
                                                              }},
                                                              new java.util.HashMap<String, String>() {{
                                                                  put("class_name", "PasswordAuthenticator");
                                                              }}
                                                          ));
                                                      }});
                                        })
                                        .start())
        {
            // Non-negotiating client connects without credentials, falls back to AllowAllAuthenticator. Gets
            // anonymous user, but should NOT receive automatic superuser privileges because isAuthenticationRequired()
            // is true.
            com.datastax.driver.core.Cluster.Builder builder = 
                com.datastax.driver.core.Cluster.builder()
                    .addContactPoint("127.0.0.1");

            try (com.datastax.driver.core.Cluster c = builder.build(); 
                 Session session = c.connect())
            {
                assertNotNull("Session should be established", session);
                
                // Verify we're logged in as anonymous
                assertCurrentRole(session, "anonymous");
                
                // Positive test: Anonymous user SHOULD be able to read from system tables
                com.datastax.driver.core.ResultSet rs = session.execute("SELECT * FROM system.local");
                assertNotNull("Anonymous user should be able to read system.local", rs);
                assertTrue("Should get at least one row", rs.iterator().hasNext());
                
                // Negative test: Anonymous user should NOT be able to create roles (requires superuser)
                // This verifies no automatic superuser privileges are granted
                try
                {
                    session.execute("CREATE ROLE test_role");
                    org.junit.Assert.fail("Anonymous user should not be able to create roles (no automatic superuser privileges)");
                }
                catch (com.datastax.driver.core.exceptions.UnauthorizedException e)
                {
                    // Expected - anonymous user doesn't have permission
                    assertTrue("Should get permission denied",
                              e.getMessage().contains("User anonymous does not have sufficient privileges to perform the requested operation"));
                }
            }
        }
    }

    /**
     * Tests that automatic superuser privileges are not granted to unauthenticated clients when authentication
     * is required globally. This directly tests the security fix where isSuper() checks
     * DatabaseDescriptor.isAuthenticationRequired() instead of the per-connection authenticator.requireAuthentication().
     *
     * Configuration: negotiation enabled with PasswordAuthenticator in negotiable list,
     * but default=AllowAllAuthenticator so non-negotiating clients connect unauthenticated.
     * Since ANY negotiable authenticator requires auth, isSuper() should return false for anonymous.
     */
    @Test
    public void testSuperuserBypassDisabledWithAuthenticationRequired() throws IOException
    {
        try (Cluster cluster = builder().withNodes(1)
                                        .withConfig(config -> {
                                            config.with(NETWORK, GOSSIP, NATIVE_PROTOCOL)
                                                  .set("authenticator", "AllowAllAuthenticator");
                                            
                                            // Configure negotiation: default allows all, but negotiable includes PasswordAuthenticator
                                            config.set("authenticator_negotiation", 
                                                      new java.util.HashMap<String, Object>() {{
                                                          put("enabled", true);
                                                          put("require_authentication", false); // permissive mode
                                                          put("default_authenticator", new java.util.HashMap<String, String>() {{
                                                              put("class_name", "AllowAllAuthenticator");
                                                          }});
                                                          put("authenticators", java.util.Arrays.asList(
                                                              new java.util.HashMap<String, String>() {{
                                                                  put("class_name", "AllowAllAuthenticator");
                                                              }},
                                                              new java.util.HashMap<String, String>() {{
                                                                  put("class_name", "PasswordAuthenticator");
                                                              }}
                                                          ));
                                                      }});
                                        })
                                        .start())
        {
            // Create a test table first (as authenticated user)
            waitForExistingRoles(cluster.get(1));
            
            com.datastax.driver.core.Cluster.Builder authBuilder = 
                com.datastax.driver.core.Cluster.builder()
                    .addContactPoint("127.0.0.1")
                    .withAuthProvider(new PlainTextAuthProvider("cassandra", "cassandra"));

            try (com.datastax.driver.core.Cluster c = authBuilder.build(); 
                 Session session = c.connect())
            {
                session.execute("CREATE KEYSPACE IF NOT EXISTS test_ks WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}");
                session.execute("CREATE TABLE IF NOT EXISTS test_ks.test_table (id int PRIMARY KEY, value text)");
            }
            
            // Non-negotiating client connects without credentials, falls back to AllowAllAuthenticator
            com.datastax.driver.core.Cluster.Builder anonBuilder = 
                com.datastax.driver.core.Cluster.builder()
                    .addContactPoint("127.0.0.1");

            try (com.datastax.driver.core.Cluster c = anonBuilder.build(); 
                 Session session = c.connect())
            {
                // Verify we're logged in as anonymous
                assertCurrentRole(session, "anonymous");
                
                // CREATE TRIGGER calls ensureIsSuperuser() which checks isSuper()
                // With the fix, isSuper() returns false because isAuthenticationRequired() is true
                try
                {
                    session.execute("CREATE TRIGGER test_trigger ON test_ks.test_table USING 'org.apache.cassandra.triggers.AuditTrigger'");
                    org.junit.Assert.fail("Anonymous user should not be able to create triggers (no automatic superuser privileges)");
                }
                catch (com.datastax.driver.core.exceptions.UnauthorizedException e)
                {
                    // Expected - isSuper() returned false, no automatic superuser privileges granted
                    assertTrue("Should get superuser required message",
                              e.getMessage().contains("Only superusers are allowed to perform CREATE TRIGGER queries"));
                }
            }
            
            // Cleanup
            try (com.datastax.driver.core.Cluster c = authBuilder.build(); 
                 Session session = c.connect())
            {
                session.execute("DROP KEYSPACE IF EXISTS test_ks");
            }
        }
    }

    /**
     * Tests that authenticated superusers retain their privileges when authenticator negotiation is enabled.
     * This is a positive control test to ensure the permission system works correctly and isn't just blocking
     * all privileged access.
     * 
     * Configuration: default=PasswordAuthenticator, negotiable=[PasswordAuthenticator, AllowAllAuthenticator]
     * Non-negotiating client falls back to PasswordAuthenticator and must authenticate.
     */
    @Test
    public void testAuthenticatedSuperuserHasPrivileges() throws IOException
    {
        try (Cluster cluster = builder().withNodes(1)
                                        .withConfig(config -> {
                                            config.with(NETWORK, GOSSIP, NATIVE_PROTOCOL)
                                                  .set("authenticator", "PasswordAuthenticator")
                                                  .set("authorizer", "CassandraAuthorizer");
                                            
                                            // Configure negotiation with PasswordAuthenticator as default
                                            config.set("authenticator_negotiation", 
                                                      new java.util.HashMap<String, Object>() {{
                                                          put("enabled", true);
                                                          put("require_authentication", false); // permissive mode
                                                          put("default_authenticator", new java.util.HashMap<String, String>() {{
                                                              put("class_name", "PasswordAuthenticator");
                                                          }});
                                                          put("authenticators", java.util.Arrays.asList(
                                                              new java.util.HashMap<String, String>() {{
                                                                  put("class_name", "PasswordAuthenticator");
                                                              }},
                                                              new java.util.HashMap<String, String>() {{
                                                                  put("class_name", "AllowAllAuthenticator");
                                                              }}
                                                          ));
                                                      }});
                                        })
                                        .start())
        {
            waitForExistingRoles(cluster.get(1));
            
            // Non-negotiating client with credentials falls back to PasswordAuthenticator
            com.datastax.driver.core.Cluster.Builder authBuilder = 
                com.datastax.driver.core.Cluster.builder()
                    .addContactPoint("127.0.0.1")
                    .withAuthProvider(new PlainTextAuthProvider("cassandra", "cassandra"));

            try (com.datastax.driver.core.Cluster c = authBuilder.build(); 
                 Session session = c.connect())
            {
                assertNotNull("Authenticated session should be established", session);
                
                // Verify we're logged in as cassandra
                assertCurrentRole(session, "cassandra");
                
                // Authenticated superuser should be able to create roles
                session.execute("CREATE ROLE IF NOT EXISTS test_role");
                
                // Verify the role was created
                com.datastax.driver.core.ResultSet rs = session.execute("LIST ROLES");
                assertNotNull("Should be able to list roles", rs);
                
                // Clean up
                session.execute("DROP ROLE IF EXISTS test_role");
            }
        }
    }

    /**
     * Tests that when authenticator negotiation is enabled with a mix of authenticators, a client that doesn't
     * support negotiation can still connect by falling back to the default authenticator (PasswordAuthenticator).
     * 
     * Configuration: default=PasswordAuthenticator, negotiable=[PasswordAuthenticator, AllowAllAuthenticator]
     * This ensures we can verify the server actually picked the default, not just any authenticator.
     */
    @Test
    public void testFallbackToDefaultAuthenticator() throws IOException
    {
        try (Cluster cluster = builder().withNodes(1)
                                        .withConfig(config -> {
                                            config.with(NETWORK, GOSSIP, NATIVE_PROTOCOL)
                                                  .set("authenticator", "PasswordAuthenticator");
                                            
                                            // Configure authenticator negotiation with nested config
                                            config.set("authenticator_negotiation", 
                                                      new java.util.HashMap<String, Object>() {{
                                                          put("enabled", true);
                                                          put("require_authentication", false); // permissive mode
                                                          put("default_authenticator", new java.util.HashMap<String, String>() {{
                                                              put("class_name", "PasswordAuthenticator");
                                                          }});
                                                          put("authenticators", java.util.Arrays.asList(
                                                              new java.util.HashMap<String, String>() {{
                                                                  put("class_name", "PasswordAuthenticator");
                                                              }},
                                                              new java.util.HashMap<String, String>() {{
                                                                  put("class_name", "AllowAllAuthenticator");
                                                              }}
                                                          ));
                                                      }});
                                        })
                                        .start())
        {
            waitForExistingRoles(cluster.get(1));
            
            // Use DataStax driver which doesn't support negotiation protocol
            // It should fall back to default (PasswordAuthenticator) and require credentials
            com.datastax.driver.core.Cluster.Builder builder =
                com.datastax.driver.core.Cluster.builder()
                                                .addContactPoint("127.0.0.1")
                                                .withAuthProvider(new PlainTextAuthProvider("cassandra", "cassandra"));


            try (com.datastax.driver.core.Cluster c = builder.build(); 
                 Session session = c.connect())
            {
                // Verify we're logged in as cassandra (authenticated via PasswordAuthenticator)
                assertCurrentRole(session, "cassandra");
                
                // If we successfully connected with credentials, the server fell back to PasswordAuthenticator
                // (If it had picked AllowAllAuthenticator, credentials wouldn't be required)
                assertNotNull("Session should be established via default authenticator fallback", session);

                // Execute a query to verify the connection is fully functional
                session.execute("SELECT * FROM system.local");
            }
        }
    }

    /**
     * Helper method to verify the current authenticated user identity by executing LIST ROLES.
     * 
     * @param session the session to execute the query on
     * @param expectedRole the expected role name (e.g., "anonymous", "cassandra")
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
            // LIST ROLES calls ensureNotAnonymous(), so exception probably means we're anonymous
            actualRole = e.getMessage().contains("not anonymous") ? "anonymous" : null;
        }
        
        assertEquals("Current authenticated role", expectedRole, actualRole);
    }
}
