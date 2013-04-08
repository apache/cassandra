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

import java.util.concurrent.TimeUnit;

import com.google.common.collect.ImmutableMap;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.KSMetaData;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.exceptions.RequestExecutionException;
import org.apache.cassandra.locator.SimpleStrategy;
import org.apache.cassandra.service.IMigrationListener;
import org.apache.cassandra.service.MigrationManager;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.FBUtilities;

public class Auth
{
    private static final Logger logger = LoggerFactory.getLogger(Auth.class);

    public static final String DEFAULT_SUPERUSER_NAME = "cassandra";

    public static final long SUPERUSER_SETUP_DELAY = Long.getLong("cassandra.superuser_setup_delay_ms", 10000);

    public static final String AUTH_KS = "system_auth";
    public static final String USERS_CF = "users";

    private static final String USERS_CF_SCHEMA = String.format("CREATE TABLE %s.%s ("
                                                                + "name text,"
                                                                + "super boolean,"
                                                                + "PRIMARY KEY(name)"
                                                                + ") WITH gc_grace_seconds=%d",
                                                                AUTH_KS,
                                                                USERS_CF,
                                                                90 * 24 * 60 * 60); // 3 months.

    /**
     * Checks if the username is stored in AUTH_KS.USERS_CF.
     *
     * @param username Username to query.
     * @return whether or not Cassandra knows about the user.
     */
    public static boolean isExistingUser(String username)
    {
        String query = String.format("SELECT * FROM %s.%s WHERE name = '%s'", AUTH_KS, USERS_CF, escape(username));
        try
        {
            return !QueryProcessor.process(query, consistencyForUser(username)).isEmpty();
        }
        catch (RequestExecutionException e)
        {
            throw new RuntimeException(e);
        }
    }

    /**
     * Checks if the user is a known superuser.
     *
     * @param username Username to query.
     * @return true is the user is a superuser, false if they aren't or don't exist at all.
     */
    public static boolean isSuperuser(String username)
    {
        String query = String.format("SELECT super FROM %s.%s WHERE name = '%s'", AUTH_KS, USERS_CF, escape(username));
        try
        {
            UntypedResultSet result = QueryProcessor.process(query, consistencyForUser(username));
            return !result.isEmpty() && result.one().getBoolean("super");
        }
        catch (RequestExecutionException e)
        {
            throw new RuntimeException(e);
        }
    }

    /**
     * Inserts the user into AUTH_KS.USERS_CF (or overwrites their superuser status as a result of an ALTER USER query).
     *
     * @param username Username to insert.
     * @param isSuper User's new status.
     * @throws RequestExecutionException
     */
    public static void insertUser(String username, boolean isSuper) throws RequestExecutionException
    {
        QueryProcessor.process(String.format("INSERT INTO %s.%s (name, super) VALUES ('%s', %s)",
                                             AUTH_KS,
                                             USERS_CF,
                                             escape(username),
                                             isSuper),
                               consistencyForUser(username));
    }

    /**
     * Deletes the user from AUTH_KS.USERS_CF.
     *
     * @param username Username to delete.
     * @throws RequestExecutionException
     */
    public static void deleteUser(String username) throws RequestExecutionException
    {
        QueryProcessor.process(String.format("DELETE FROM %s.%s WHERE name = '%s'",
                                             AUTH_KS,
                                             USERS_CF,
                                             escape(username)),
                               consistencyForUser(username));
    }

    /**
     * Sets up Authenticator and Authorizer.
     */
    public static void setup()
    {
        setupAuthKeyspace();
        setupUsersTable();

        DatabaseDescriptor.getAuthenticator().setup();
        DatabaseDescriptor.getAuthorizer().setup();

        // register a custom MigrationListener for permissions cleanup after dropped keyspaces/cfs.
        MigrationManager.instance.register(new MigrationListener());

        // the delay is here to give the node some time to see its peers - to reduce
        // "Skipped default superuser setup: some nodes were not ready" log spam.
        // It's the only reason for the delay.
        if (DatabaseDescriptor.getSeeds().contains(FBUtilities.getBroadcastAddress()) || !DatabaseDescriptor.isAutoBootstrap())
        {
            StorageService.tasks.schedule(new Runnable()
                                          {
                                              public void run()
                                              {
                                                  setupDefaultSuperuser();
                                              }
                                          },
                                          SUPERUSER_SETUP_DELAY,
                                          TimeUnit.MILLISECONDS);
        }
    }

    // Only use QUORUM cl for the default superuser.
    private static ConsistencyLevel consistencyForUser(String username)
    {
        if (username.equals(DEFAULT_SUPERUSER_NAME))
            return ConsistencyLevel.QUORUM;
        else
            return ConsistencyLevel.ONE;
    }

    private static void setupAuthKeyspace()
    {
        if (Schema.instance.getKSMetaData(AUTH_KS) == null)
        {
            try
            {
                KSMetaData ksm = KSMetaData.newKeyspace(AUTH_KS, SimpleStrategy.class.getName(), ImmutableMap.of("replication_factor", "1"), true);
                MigrationManager.announceNewKeyspace(ksm, 0);
            }
            catch (Exception e)
            {
                throw new AssertionError(e); // shouldn't ever happen.
            }
        }
    }

    private static void setupUsersTable()
    {
        if (Schema.instance.getCFMetaData(AUTH_KS, USERS_CF) == null)
        {
            try
            {
                QueryProcessor.process(USERS_CF_SCHEMA, ConsistencyLevel.ANY);
            }
            catch (RequestExecutionException e)
            {
                throw new AssertionError(e);
            }
        }
    }

    private static void setupDefaultSuperuser()
    {
        try
        {
            // insert a default superuser if AUTH_KS.USERS_CF is empty.
            if (QueryProcessor.process(String.format("SELECT * FROM %s.%s", AUTH_KS, USERS_CF), ConsistencyLevel.QUORUM).isEmpty())
            {
                QueryProcessor.process(String.format("INSERT INTO %s.%s (name, super) VALUES ('%s', %s) USING TIMESTAMP 0",
                                                     AUTH_KS,
                                                     USERS_CF,
                                                     DEFAULT_SUPERUSER_NAME,
                                                     true),
                                       ConsistencyLevel.QUORUM);
                logger.info("Created default superuser '{}'", DEFAULT_SUPERUSER_NAME);
            }
        }
        catch (RequestExecutionException e)
        {
            logger.warn("Skipped default superuser setup: some nodes were not ready");
        }
    }

    // we only worry about one character ('). Make sure it's properly escaped.
    private static String escape(String name)
    {
        return StringUtils.replace(name, "'", "''");
    }

    /**
     * IMigrationListener implementation that cleans up permissions on dropped resources.
     */
    public static class MigrationListener implements IMigrationListener
    {
        public void onDropKeyspace(String ksName)
        {
            DatabaseDescriptor.getAuthorizer().revokeAll(DataResource.keyspace(ksName));
        }

        public void onDropColumnFamily(String ksName, String cfName)
        {
            DatabaseDescriptor.getAuthorizer().revokeAll(DataResource.columnFamily(ksName, cfName));
        }

        public void onCreateKeyspace(String ksName)
        {
        }

        public void onCreateColumnFamily(String ksName, String cfName)
        {
        }

        public void onUpdateKeyspace(String ksName)
        {
        }

        public void onUpdateColumnFamily(String ksName, String cfName)
        {
        }
    }
}
