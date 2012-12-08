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

import org.apache.commons.lang.StringUtils;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.service.MigrationManager;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.WrappedRunnable;

public class Auth
{
    public static final String DEFAULT_SUPERUSER_NAME = "cassandra";

    public static final String AUTH_KS = "system_auth";
    public static final String USERS_CF = "users";

    /**
     * Checks if the username is stored in AUTH_KS.USERS_CF.
     *
     * @param username Username to query.
     * @return whether or not Cassandra knows about the user.
     */
    public static boolean isExistingUser(String username)
    {
        String query = String.format("SELECT * FROM %s.%s WHERE name = '%s'", AUTH_KS, USERS_CF, escape(username));
        return !QueryProcessor.process(query).isEmpty();
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
        UntypedResultSet result = QueryProcessor.process(query);
        return !result.isEmpty() && result.one().getBoolean("super");
    }

    /**
     * Inserts the user into AUTH_KS.USERS_CF (or overwrites their superuser status as a result of an ALTER USER query).
     *
     * @param username Username to insert.
     * @param isSuper User's new status.
     */
    public static void insertUser(String username, boolean isSuper)
    {
        QueryProcessor.process(String.format("INSERT INTO %s.%s (name, super) VALUES ('%s', %s)",
                                             AUTH_KS,
                                             USERS_CF,
                                             escape(username),
                                             isSuper));
    }

    /**
     * Deletes the user from AUTH_KS.USERS_CF.
     *
     * @param username Username to delete.
     */
    public static void deleteUser(String username)
    {
        QueryProcessor.process(String.format("DELETE FROM %s.%s WHERE name = '%s'",
                                             AUTH_KS,
                                             USERS_CF,
                                             escape(username)));
    }

    /**
     * Sets up Authenticator and Authorizer.
     */
    public static void setup()
    {
        authenticator().setup();
        authorizer().setup();

        // register a custom MigrationListener for permissions cleanup after dropped keyspaces/cfs.
        MigrationManager.instance.register(new MigrationListener());

        // schedule seeding a superuser in RING_DELAY milliseconds.
        Runnable runnable = new WrappedRunnable()
        {
            public void runMayThrow() throws InvalidRequestException
            {
                // insert a default superuser if AUTH_KS.USERS_CF is empty.
                if (QueryProcessor.process(String.format("SELECT * FROM %s.%s", AUTH_KS, USERS_CF)).isEmpty())
                    insertUser(DEFAULT_SUPERUSER_NAME, true);
            }
        };
        StorageService.tasks.schedule(runnable, StorageService.RING_DELAY, TimeUnit.MILLISECONDS);
    }

    // we only worry about one character ('). Make sure it's properly escaped.
    private static String escape(String name)
    {
        return StringUtils.replace(name, "'", "''");
    }

    private static IAuthenticator authenticator()
    {
        return DatabaseDescriptor.getAuthenticator();
    }

    private static IAuthorizer authorizer()
    {
        return DatabaseDescriptor.getAuthorizer();
    }
}
