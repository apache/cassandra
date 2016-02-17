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
package org.apache.cassandra.auth;

import java.net.InetAddress;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.UncheckedExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.cql3.statements.SelectStatement;
import org.apache.cassandra.exceptions.AuthenticationException;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.exceptions.RequestExecutionException;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.transport.messages.ResultMessage;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.mindrot.jbcrypt.BCrypt;

import static org.apache.cassandra.auth.CassandraRoleManager.consistencyForRole;

/**
 * PasswordAuthenticator is an IAuthenticator implementation
 * that keeps credentials (rolenames and bcrypt-hashed passwords)
 * internally in C* - in system_auth.roles CQL3 table.
 * Since 2.2, the management of roles (creation, modification,
 * querying etc is the responsibility of IRoleManager. Use of
 * PasswordAuthenticator requires the use of CassandraRoleManager
 * for storage and retrieval of encrypted passwords.
 */
public class PasswordAuthenticator implements IAuthenticator
{
    private static final Logger logger = LoggerFactory.getLogger(PasswordAuthenticator.class);

    // name of the hash column.
    private static final String SALTED_HASH = "salted_hash";

    // really this is a rolename now, but as it only matters for Thrift, we leave it for backwards compatibility
    public static final String USERNAME_KEY = "username";
    public static final String PASSWORD_KEY = "password";

    private static final byte NUL = 0;
    private SelectStatement authenticateStatement;

    public static final String LEGACY_CREDENTIALS_TABLE = "credentials";
    private SelectStatement legacyAuthenticateStatement;

    private CredentialsCache cache;

    // No anonymous access.
    public boolean requireAuthentication()
    {
        return true;
    }

    private AuthenticatedUser authenticate(String username, String password) throws AuthenticationException
    {
        try
        {
            String hash = cache.get(username);
            if (!BCrypt.checkpw(password, hash))
                throw new AuthenticationException("Username and/or password are incorrect");

            return new AuthenticatedUser(username);
        }
        catch (ExecutionException | UncheckedExecutionException e)
        {
            // the credentials were somehow invalid - either a non-existent role, or one without a defined password
            if (e.getCause() instanceof NoSuchCredentialsException)
                throw new AuthenticationException("Username and/or password are incorrect");

            // an unanticipated exception occured whilst querying the credentials table
            if (e.getCause() instanceof RequestExecutionException)
            {
                logger.trace("Error performing internal authentication", e);
                throw new AuthenticationException(e.getMessage());
            }

            throw new RuntimeException(e);
        }
    }

    private String queryHashedPassword(String username) throws NoSuchCredentialsException
    {
        try
        {
            // If the legacy users table exists try to verify credentials there. This is to handle the case
            // where the cluster is being upgraded and so is running with mixed versions of the authn tables
            SelectStatement authenticationStatement = Schema.instance.getCFMetaData(AuthKeyspace.NAME, LEGACY_CREDENTIALS_TABLE) == null
                                                    ? authenticateStatement
                                                    : legacyAuthenticateStatement;

            ResultMessage.Rows rows =
                authenticationStatement.execute(QueryState.forInternalCalls(),
                                                QueryOptions.forInternalCalls(consistencyForRole(username),
                                                                              Lists.newArrayList(ByteBufferUtil.bytes(username))));

            // If either a non-existent role name was supplied, or no credentials
            // were found for that role we don't want to cache the result so we throw
            // a specific, but unchecked, exception to keep LoadingCache happy.
            if (rows.result.isEmpty())
                throw new NoSuchCredentialsException();

            UntypedResultSet result = UntypedResultSet.create(rows.result);
            if (!result.one().has(SALTED_HASH))
                throw new NoSuchCredentialsException();

            return result.one().getString(SALTED_HASH);
        }
        catch (RequestExecutionException e)
        {
            logger.trace("Error performing internal authentication", e);
            throw e;
        }
    }

    public Set<DataResource> protectedResources()
    {
        // Also protected by CassandraRoleManager, but the duplication doesn't hurt and is more explicit
        return ImmutableSet.of(DataResource.table(AuthKeyspace.NAME, AuthKeyspace.ROLES));
    }

    public void validateConfiguration() throws ConfigurationException
    {
    }

    public void setup()
    {
        String query = String.format("SELECT %s FROM %s.%s WHERE role = ?",
                                     SALTED_HASH,
                                     AuthKeyspace.NAME,
                                     AuthKeyspace.ROLES);
        authenticateStatement = prepare(query);

        if (Schema.instance.getCFMetaData(AuthKeyspace.NAME, LEGACY_CREDENTIALS_TABLE) != null)
        {
            query = String.format("SELECT %s from %s.%s WHERE username = ?",
                                  SALTED_HASH,
                                  AuthKeyspace.NAME,
                                  LEGACY_CREDENTIALS_TABLE);
            legacyAuthenticateStatement = prepare(query);
        }

        cache = new CredentialsCache(this);
    }

    public AuthenticatedUser legacyAuthenticate(Map<String, String> credentials) throws AuthenticationException
    {
        String username = credentials.get(USERNAME_KEY);
        if (username == null)
            throw new AuthenticationException(String.format("Required key '%s' is missing", USERNAME_KEY));

        String password = credentials.get(PASSWORD_KEY);
        if (password == null)
            throw new AuthenticationException(String.format("Required key '%s' is missing", PASSWORD_KEY));

        return authenticate(username, password);
    }

    public SaslNegotiator newSaslNegotiator(InetAddress clientAddress)
    {
        return new PlainTextSaslAuthenticator();
    }

    private static SelectStatement prepare(String query)
    {
        return (SelectStatement) QueryProcessor.getStatement(query, ClientState.forInternalCalls()).statement;
    }

    private class PlainTextSaslAuthenticator implements SaslNegotiator
    {
        private boolean complete = false;
        private String username;
        private String password;

        public byte[] evaluateResponse(byte[] clientResponse) throws AuthenticationException
        {
            decodeCredentials(clientResponse);
            complete = true;
            return null;
        }

        public boolean isComplete()
        {
            return complete;
        }

        public AuthenticatedUser getAuthenticatedUser() throws AuthenticationException
        {
            if (!complete)
                throw new AuthenticationException("SASL negotiation not complete");
            return authenticate(username, password);
        }

        /**
         * SASL PLAIN mechanism specifies that credentials are encoded in a
         * sequence of UTF-8 bytes, delimited by 0 (US-ASCII NUL).
         * The form is : {code}authzId<NUL>authnId<NUL>password<NUL>{code}
         * authzId is optional, and in fact we don't care about it here as we'll
         * set the authzId to match the authnId (that is, there is no concept of
         * a user being authorized to act on behalf of another with this IAuthenticator).
         *
         * @param bytes encoded credentials string sent by the client
         * @throws org.apache.cassandra.exceptions.AuthenticationException if either the
         *         authnId or password is null
         */
        private void decodeCredentials(byte[] bytes) throws AuthenticationException
        {
            logger.trace("Decoding credentials from client token");
            byte[] user = null;
            byte[] pass = null;
            int end = bytes.length;
            for (int i = bytes.length - 1 ; i >= 0; i--)
            {
                if (bytes[i] == NUL)
                {
                    if (pass == null)
                        pass = Arrays.copyOfRange(bytes, i + 1, end);
                    else if (user == null)
                        user = Arrays.copyOfRange(bytes, i + 1, end);
                    end = i;
                }
            }

            if (pass == null)
                throw new AuthenticationException("Password must not be null");
            if (user == null)
                throw new AuthenticationException("Authentication ID must not be null");

            username = new String(user, StandardCharsets.UTF_8);
            password = new String(pass, StandardCharsets.UTF_8);
        }
    }

    private static class CredentialsCache extends AuthCache<String, String> implements CredentialsCacheMBean
    {
        private CredentialsCache(PasswordAuthenticator authenticator)
        {
            super("CredentialsCache",
                  DatabaseDescriptor::setCredentialsValidity,
                  DatabaseDescriptor::getCredentialsValidity,
                  DatabaseDescriptor::setCredentialsUpdateInterval,
                  DatabaseDescriptor::getCredentialsUpdateInterval,
                  DatabaseDescriptor::setCredentialsCacheMaxEntries,
                  DatabaseDescriptor::getCredentialsCacheMaxEntries,
                  authenticator::queryHashedPassword,
                  () -> true);
        }

        public void invalidateCredentials(String roleName)
        {
            invalidate(roleName);
        }
    }

    public static interface CredentialsCacheMBean extends AuthCacheMBean
    {
        public void invalidateCredentials(String roleName);
    }

    // Just a marker so we can identify that invalid credentials were the
    // cause of a loading exception from the cache
    private static final class NoSuchCredentialsException extends RuntimeException
    {
    }
}
