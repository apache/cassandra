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
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.exceptions.RequestExecutionException;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.cql3.statements.SelectStatement;
import org.apache.cassandra.exceptions.AuthenticationException;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.transport.messages.ResultMessage;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.mindrot.jbcrypt.BCrypt;

import static org.apache.cassandra.auth.CassandraRoleManager.consistencyForRoleRead;
import static org.apache.cassandra.utils.Clock.Global.nanoTime;

/**
 * PasswordAuthenticator is an IAuthenticator implementation
 * that keeps credentials (rolenames and bcrypt-hashed passwords)
 * internally in C* - in system_auth.roles CQL3 table.
 * Since 2.2, the management of roles (creation, modification,
 * querying etc is the responsibility of IRoleManager. Use of
 * PasswordAuthenticator requires the use of CassandraRoleManager
 * for storage and retrieval of encrypted passwords.
 */
public class PasswordAuthenticator implements IAuthenticator, AuthCache.BulkLoader<String, String>
{
    private static final Logger logger = LoggerFactory.getLogger(PasswordAuthenticator.class);

    /** We intentionally use an empty string sentinel to allow object equality comparison */
    private static final String NO_SUCH_CREDENTIAL = "";

    // name of the hash column.
    private static final String SALTED_HASH = "salted_hash";

    // really this is a rolename now, but as it only matters for Thrift, we leave it for backwards compatibility
    public static final String USERNAME_KEY = "username";
    public static final String PASSWORD_KEY = "password";

    static final byte NUL = 0;
    private SelectStatement authenticateStatement;

    private final CredentialsCache cache;

    public PasswordAuthenticator()
    {
        cache = new CredentialsCache(this);
        AuthCacheService.instance.register(cache);
    }

    // No anonymous access.
    public boolean requireAuthentication()
    {
        return true;
    }

    @Override
    public Supplier<Map<String, String>> bulkLoader()
    {
        return () ->
        {
            Map<String, String> entries = new HashMap<>();

            logger.info("Pre-warming credentials cache from roles table");
            UntypedResultSet results = process("SELECT role, salted_hash FROM system_auth.roles", CassandraAuthorizer.authReadConsistencyLevel());
            results.forEach(row -> {
                if (row.has("salted_hash"))
                {
                    entries.put(row.getString("role"), row.getString("salted_hash"));
                }
            });
            return entries;
        };
    }

    public CredentialsCache getCredentialsCache()
    {
        return cache;
    }

    protected static boolean checkpw(String password, String hash)
    {
        try
        {
            return BCrypt.checkpw(password, hash);
        }
        catch (Exception e)
        {
            // Improperly formatted hashes may cause BCrypt.checkpw to throw, so trap any other exception as a failure
            logger.warn("Error: invalid password hash encountered, rejecting user", e);
            return false;
        }
    }

    /**
     * This is exposed so we can override the consistency level for tests that are single node
     */
    @VisibleForTesting
    UntypedResultSet process(String query, ConsistencyLevel cl)
    {
        return QueryProcessor.process(query, cl);
    }

    private AuthenticatedUser authenticate(String username, String password) throws AuthenticationException
    {
        String hash = cache.get(username);

        // intentional use of object equality
        if (hash == NO_SUCH_CREDENTIAL)
        {
            // The cache was unable to load credentials via queryHashedPassword, probably because the supplied
            // rolename doesn't exist. If caching is enabled we will have now cached the sentinel value for that key
            // so we should invalidate it otherwise the cache will continue to serve that until it expires which
            // will be a problem if the role is added in the meantime.
            //
            // We can't just throw the AuthenticationException directly from queryHashedPassword for a similar reason:
            // if an existing role is dropped and active updates are enabled for the cache, the refresh in
            // CacheRefresher::run will log and swallow the exception and keep serving the stale credentials until they
            // eventually expire.
            //
            // So whenever we encounter the sentinal value, here and also in CacheRefresher (if active updates are
            // enabled), we manually expunge the key from the cache. If caching is not enabled, AuthCache::invalidate
            // is a safe no-op.
            cache.invalidateCredentials(username);
            throw new AuthenticationException(String.format("Provided username %s and/or password are incorrect", username));
        }

        if (!checkpw(password, hash))
            throw new AuthenticationException(String.format("Provided username %s and/or password are incorrect", username));

        return new AuthenticatedUser(username);
    }

    private String queryHashedPassword(String username)
    {
        try
        {
            QueryOptions options = QueryOptions.forInternalCalls(consistencyForRoleRead(username),
                    Lists.newArrayList(ByteBufferUtil.bytes(username)));

            ResultMessage.Rows rows = select(authenticateStatement, options);

            // If either a non-existent role name was supplied, or no credentials
            // were found for that role, we don't want to cache the result so we
            // return a sentinel value. On receiving the sentinel, the caller can
            // invalidate the cache and throw an appropriate exception.
            if (rows.result.isEmpty())
                return NO_SUCH_CREDENTIAL;

            UntypedResultSet result = UntypedResultSet.create(rows.result);
            if (!result.one().has(SALTED_HASH))
                return NO_SUCH_CREDENTIAL;

            return result.one().getString(SALTED_HASH);
        }
        catch (RequestExecutionException e)
        {
            throw new AuthenticationException("Unable to perform authentication: " + e.getMessage(), e);
        }
    }

    @VisibleForTesting
    ResultMessage.Rows select(SelectStatement statement, QueryOptions options)
    {
        return statement.execute(QueryState.forInternalCalls(), options, nanoTime());
    }

    public Set<DataResource> protectedResources()
    {
        // Also protected by CassandraRoleManager, but the duplication doesn't hurt and is more explicit
        return ImmutableSet.of(DataResource.table(SchemaConstants.AUTH_KEYSPACE_NAME, AuthKeyspace.ROLES));
    }

    public void validateConfiguration() throws ConfigurationException
    {
    }

    public void setup()
    {
        String query = String.format("SELECT %s FROM %s.%s WHERE role = ?",
                                     SALTED_HASH,
                                     SchemaConstants.AUTH_KEYSPACE_NAME,
                                     AuthKeyspace.ROLES);
        authenticateStatement = prepare(query);
    }

    public AuthenticatedUser legacyAuthenticate(Map<String, String> credentials) throws AuthenticationException
    {
        String username = credentials.get(USERNAME_KEY);
        if (username == null)
            throw new AuthenticationException(String.format("Required key '%s' is missing", USERNAME_KEY));

        String password = credentials.get(PASSWORD_KEY);
        if (password == null)
            throw new AuthenticationException(String.format("Required key '%s' is missing for provided username %s", PASSWORD_KEY, username));

        return authenticate(username, password);
    }

    public SaslNegotiator newSaslNegotiator(InetAddress clientAddress)
    {
        return new PlainTextSaslAuthenticator();
    }

    private static SelectStatement prepare(String query)
    {
        return (SelectStatement) QueryProcessor.getStatement(query, ClientState.forInternalCalls());
    }

    @VisibleForTesting
    class PlainTextSaslAuthenticator implements SaslNegotiator
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
         * The form is : {@code authzId<NUL>authnId<NUL>password<NUL>}
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
            for (int i = bytes.length - 1; i >= 0; i--)
            {
                if (bytes[i] == NUL)
                {
                    if (pass == null)
                        pass = Arrays.copyOfRange(bytes, i + 1, end);
                    else if (user == null)
                        user = Arrays.copyOfRange(bytes, i + 1, end);
                    else
                        throw new AuthenticationException("Credential format error: username or password is empty or contains NUL(\\0) character");

                    end = i;
                }
            }

            if (pass == null || pass.length == 0)
                throw new AuthenticationException("Password must not be null");
            if (user == null || user.length == 0)
                throw new AuthenticationException("Authentication ID must not be null");

            username = new String(user, StandardCharsets.UTF_8);
            password = new String(pass, StandardCharsets.UTF_8);
        }
    }

    public static class CredentialsCache extends AuthCache<String, String> implements CredentialsCacheMBean
    {
        private CredentialsCache(PasswordAuthenticator authenticator)
        {
            super(CACHE_NAME,
                  DatabaseDescriptor::setCredentialsValidity,
                  DatabaseDescriptor::getCredentialsValidity,
                  DatabaseDescriptor::setCredentialsUpdateInterval,
                  DatabaseDescriptor::getCredentialsUpdateInterval,
                  DatabaseDescriptor::setCredentialsCacheMaxEntries,
                  DatabaseDescriptor::getCredentialsCacheMaxEntries,
                  DatabaseDescriptor::setCredentialsCacheActiveUpdate,
                  DatabaseDescriptor::getCredentialsCacheActiveUpdate,
                  authenticator::queryHashedPassword,
                  authenticator.bulkLoader(),
                  () -> true,
                  (k,v) -> NO_SUCH_CREDENTIAL == v); // use a known object as a sentinel value. CacheRefresher will
                                                     // invalidate the key if the sentinel is loaded during a refresh
        }

        public void invalidateCredentials(String roleName)
        {
            invalidate(roleName);
        }
    }

    public static interface CredentialsCacheMBean extends AuthCacheMBean
    {
        public static final String CACHE_NAME = "CredentialsCache";

        public void invalidateCredentials(String roleName);
    }
}
