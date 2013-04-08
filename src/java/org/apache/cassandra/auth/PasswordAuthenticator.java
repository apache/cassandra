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

import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.ImmutableSet;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.exceptions.AuthenticationException;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.exceptions.RequestExecutionException;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.FBUtilities;
import org.mindrot.jbcrypt.BCrypt;

/**
 * PasswordAuthenticator is an IAuthenticator implementation
 * that keeps credentials (usernames and bcrypt-hashed passwords)
 * internally in C* - in system_auth.credentials CQL3 table.
 */
public class PasswordAuthenticator implements IAuthenticator
{
    private static final Logger logger = LoggerFactory.getLogger(PasswordAuthenticator.class);

    // 2 ** GENSALT_LOG2_ROUNS rounds of hashing will be performed.
    private static final int GENSALT_LOG2_ROUNDS = 10;

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
            result = process(String.format("SELECT %s FROM %s.%s WHERE username = '%s'",
                                           SALTED_HASH,
                                           Auth.AUTH_KS,
                                           CREDENTIALS_CF,
                                           escape(username)),
                             consistencyForUser(username));
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
        setupCredentialsTable();

        // the delay is here to give the node some time to see its peers - to reduce
        // "skipped default user setup: some nodes are were not ready" log spam.
        // It's the only reason for the delay.
        if (DatabaseDescriptor.getSeeds().contains(FBUtilities.getBroadcastAddress()) || !DatabaseDescriptor.isAutoBootstrap())
        {
            StorageService.tasks.schedule(new Runnable()
                                          {
                                              public void run()
                                              {
                                                  setupDefaultUser();
                                              }
                                          },
                                          Auth.SUPERUSER_SETUP_DELAY,
                                          TimeUnit.MILLISECONDS);
        }
    }

    private void setupCredentialsTable()
    {
        if (Schema.instance.getCFMetaData(Auth.AUTH_KS, CREDENTIALS_CF) == null)
        {
            try
            {
                process(CREDENTIALS_CF_SCHEMA, ConsistencyLevel.ANY);
            }
            catch (RequestExecutionException e)
            {
                throw new AssertionError(e);
            }
        }
    }

    // if there are no users yet - add default superuser.
    private void setupDefaultUser()
    {
        try
        {
            // insert a default superuser if AUTH_KS.CREDENTIALS_CF is empty.
            if (process(String.format("SELECT * FROM %s.%s", Auth.AUTH_KS, CREDENTIALS_CF), ConsistencyLevel.QUORUM).isEmpty())
            {
                process(String.format("INSERT INTO %s.%s (username, salted_hash) VALUES ('%s', '%s') USING TIMESTAMP 0",
                                      Auth.AUTH_KS,
                                      CREDENTIALS_CF,
                                      DEFAULT_USER_NAME,
                                      escape(hashpw(DEFAULT_USER_PASSWORD))),
                        ConsistencyLevel.QUORUM);
                logger.info("PasswordAuthenticator created default user '{}'", DEFAULT_USER_NAME);
            }
        }
        catch (RequestExecutionException e)
        {
            logger.warn("PasswordAuthenticator skipped default user setup: some nodes were not ready");
        }
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
            return ConsistencyLevel.ONE;
    }
}
