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

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.concurrent.ScheduledExecutors;
import org.apache.cassandra.config.Config;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.statements.SelectStatement;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.exceptions.*;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.transport.messages.ResultMessage;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.mindrot.jbcrypt.BCrypt;

/**
 * PasswordAuthenticator is an IAuthenticator implementation
 * that keeps credentials (usernames and bcrypt-hashed passwords)
 * internally in C* - in system_auth.credentials CQL3 table.
 */
public class PasswordAuthenticator implements ISaslAwareAuthenticator
{
    private static final Logger logger = LoggerFactory.getLogger(PasswordAuthenticator.class);

    // 2 ** GENSALT_LOG2_ROUNDS rounds of hashing will be performed.
    private static final String GENSALT_LOG2_ROUNDS_PROPERTY = Config.PROPERTY_PREFIX + "auth_bcrypt_gensalt_log2_rounds";
    private static final int GENSALT_LOG2_ROUNDS = getGensaltLogRounds();

    static int getGensaltLogRounds()
    {
        int rounds = Integer.getInteger(GENSALT_LOG2_ROUNDS_PROPERTY, 10);
        if (rounds < 4 || rounds > 31)
            throw new RuntimeException(new ConfigurationException(String.format("Bad value for system property -D%s. " +
                                                                                "Please use a value 4 and 31",
                                                                                GENSALT_LOG2_ROUNDS_PROPERTY)));
        return rounds;
    }

    // name of the hash column.
    private static final String SALTED_HASH = "salted_hash";

    private static final String DEFAULT_USER_NAME = Auth.DEFAULT_SUPERUSER_NAME;
    private static final String DEFAULT_USER_PASSWORD = Auth.DEFAULT_SUPERUSER_NAME;

    private static final String CREDENTIALS_CF = "credentials";
    private static final String CREDENTIALS_CF_SCHEMA = String.format("CREATE TABLE %s.%s ("
                                                                      + "username text,"
                                                                      + "salted_hash text," // salt + hash + number of rounds
                                                                      + "options map<text,text>," // for future extensions
                                                                      + "PRIMARY KEY(username)"
                                                                      + ") WITH gc_grace_seconds=%d",
                                                                      Auth.AUTH_KS,
                                                                      CREDENTIALS_CF,
                                                                      90 * 24 * 60 * 60); // 3 months.

    private SelectStatement authenticateStatement;

    // No anonymous access.
    public boolean requireAuthentication()
    {
        return true;
    }

    public Set<Option> supportedOptions()
    {
        return ImmutableSet.of(Option.PASSWORD);
    }

    // Let users alter their own password.
    public Set<Option> alterableOptions()
    {
        return ImmutableSet.of(Option.PASSWORD);
    }

    public AuthenticatedUser authenticate(Map<String, String> credentials) throws AuthenticationException
    {
        String username = credentials.get(USERNAME_KEY);
        if (username == null)
            throw new AuthenticationException(String.format("Required key '%s' is missing", USERNAME_KEY));

        String password = credentials.get(PASSWORD_KEY);
        if (password == null)
            throw new AuthenticationException(String.format("Required key '%s' is missing", PASSWORD_KEY));

        UntypedResultSet result;
        try
        {
            ResultMessage.Rows rows = authenticateStatement.execute(QueryState.forInternalCalls(),
                                                                    QueryOptions.forInternalCalls(consistencyForUser(username),
                                                                                                  Lists.newArrayList(ByteBufferUtil.bytes(username))));
            result = UntypedResultSet.create(rows.result);
        }
        catch (RequestValidationException e)
        {
            throw new AssertionError(e); // not supposed to happen
        }
        catch (RequestExecutionException e)
        {
            throw new AuthenticationException(e.toString());
        }

        if (result.isEmpty() || !BCrypt.checkpw(password, result.one().getString(SALTED_HASH)))
            throw new AuthenticationException("Username and/or password are incorrect");

        return new AuthenticatedUser(username);
    }

    public void create(String username, Map<Option, Object> options) throws InvalidRequestException, RequestExecutionException
    {
        String password = (String) options.get(Option.PASSWORD);
        if (password == null)
            throw new InvalidRequestException("PasswordAuthenticator requires PASSWORD option");

        process(String.format("INSERT INTO %s.%s (username, salted_hash) VALUES ('%s', '%s')",
                              Auth.AUTH_KS,
                              CREDENTIALS_CF,
                              escape(username),
                              escape(hashpw(password))),
                consistencyForUser(username));
    }

    public void alter(String username, Map<Option, Object> options) throws RequestExecutionException
    {
        process(String.format("UPDATE %s.%s SET salted_hash = '%s' WHERE username = '%s'",
                              Auth.AUTH_KS,
                              CREDENTIALS_CF,
                              escape(hashpw((String) options.get(Option.PASSWORD))),
                              escape(username)),
                consistencyForUser(username));
    }

    public void drop(String username) throws RequestExecutionException
    {
        process(String.format("DELETE FROM %s.%s WHERE username = '%s'", Auth.AUTH_KS, CREDENTIALS_CF, escape(username)),
                consistencyForUser(username));
    }

    public Set<DataResource> protectedResources()
    {
        return ImmutableSet.of(DataResource.columnFamily(Auth.AUTH_KS, CREDENTIALS_CF));
    }

    public void validateConfiguration() throws ConfigurationException
    {
    }

    public void setup()
    {
        Auth.setupTable(CREDENTIALS_CF, CREDENTIALS_CF_SCHEMA);

        // the delay is here to give the node some time to see its peers - to reduce
        // "skipped default user setup: some nodes are were not ready" log spam.
        // It's the only reason for the delay.
        ScheduledExecutors.nonPeriodicTasks.schedule(new Runnable()
        {
            public void run()
            {
              setupDefaultUser();
            }
        }, Auth.SUPERUSER_SETUP_DELAY, TimeUnit.MILLISECONDS);

        try
        {
            String query = String.format("SELECT %s FROM %s.%s WHERE username = ?",
                                         SALTED_HASH,
                                         Auth.AUTH_KS,
                                         CREDENTIALS_CF);
            authenticateStatement = (SelectStatement) QueryProcessor.parseStatement(query).prepare().statement;
        }
        catch (RequestValidationException e)
        {
            throw new AssertionError(e); // not supposed to happen
        }
    }

    public SaslAuthenticator newAuthenticator()
    {
        return new PlainTextSaslAuthenticator();
    }

    // if there are no users yet - add default superuser.
    private void setupDefaultUser()
    {
        try
        {
            // insert the default superuser if AUTH_KS.CREDENTIALS_CF is empty.
            if (!hasExistingUsers())
            {
                process(String.format("INSERT INTO %s.%s (username, salted_hash) VALUES ('%s', '%s') USING TIMESTAMP 0",
                                      Auth.AUTH_KS,
                                      CREDENTIALS_CF,
                                      DEFAULT_USER_NAME,
                                      escape(hashpw(DEFAULT_USER_PASSWORD))),
                        ConsistencyLevel.ONE);
                logger.info("PasswordAuthenticator created default user '{}'", DEFAULT_USER_NAME);
            }
        }
        catch (RequestExecutionException e)
        {
            logger.warn("PasswordAuthenticator skipped default user setup: some nodes were not ready");
        }
    }

    private static boolean hasExistingUsers() throws RequestExecutionException
    {
        // Try looking up the 'cassandra' default user first, to avoid the range query if possible.
        String defaultSUQuery = String.format("SELECT * FROM %s.%s WHERE username = '%s'", Auth.AUTH_KS, CREDENTIALS_CF, DEFAULT_USER_NAME);
        String allUsersQuery = String.format("SELECT * FROM %s.%s LIMIT 1", Auth.AUTH_KS, CREDENTIALS_CF);
        return !process(defaultSUQuery, ConsistencyLevel.ONE).isEmpty()
            || !process(defaultSUQuery, ConsistencyLevel.QUORUM).isEmpty()
            || !process(allUsersQuery, ConsistencyLevel.QUORUM).isEmpty();
    }

    private static String hashpw(String password)
    {
        return BCrypt.hashpw(password, BCrypt.gensalt(GENSALT_LOG2_ROUNDS));
    }

    private static String escape(String name)
    {
        return StringUtils.replace(name, "'", "''");
    }

    private static UntypedResultSet process(String query, ConsistencyLevel cl) throws RequestExecutionException
    {
        return QueryProcessor.process(query, cl);
    }

    private static ConsistencyLevel consistencyForUser(String username)
    {
        if (username.equals(DEFAULT_USER_NAME))
            return ConsistencyLevel.QUORUM;
        else
            return ConsistencyLevel.LOCAL_ONE;
    }

    private class PlainTextSaslAuthenticator implements ISaslAwareAuthenticator.SaslAuthenticator
    {
        private static final byte NUL = 0;

        private boolean complete = false;
        private Map<String, String> credentials;

        @Override
        public byte[] evaluateResponse(byte[] clientResponse) throws AuthenticationException
        {
            credentials = decodeCredentials(clientResponse);
            complete = true;
            return null;
        }

        @Override
        public boolean isComplete()
        {
            return complete;
        }

        @Override
        public AuthenticatedUser getAuthenticatedUser() throws AuthenticationException
        {
            return authenticate(credentials);
        }

        /**
         * SASL PLAIN mechanism specifies that credentials are encoded in a
         * sequence of UTF-8 bytes, delimited by 0 (US-ASCII NUL).
         * The form is : {code}authzId<NUL>authnId<NUL>password<NUL>{code}
         * authzId is optional, and in fact we don't care about it here as we'll
         * set the authzId to match the authnId (that is, there is no concept of
         * a user being authorized to act on behalf of another).
         *
         * @param bytes encoded credentials string sent by the client
         * @return map containing the username/password pairs in the form an IAuthenticator
         * would expect
         * @throws javax.security.sasl.SaslException
         */
        private Map<String, String> decodeCredentials(byte[] bytes) throws AuthenticationException
        {
            logger.debug("Decoding credentials from client token");
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

            if (user == null)
                throw new AuthenticationException("Authentication ID must not be null");
            if (pass == null)
                throw new AuthenticationException("Password must not be null");

            Map<String, String> credentials = new HashMap<String, String>();
            credentials.put(IAuthenticator.USERNAME_KEY, new String(user, StandardCharsets.UTF_8));
            credentials.put(IAuthenticator.PASSWORD_KEY, new String(pass, StandardCharsets.UTF_8));
            return credentials;
        }
    }
}
