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

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.ServerTestUtils;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLTester;

import static org.mindrot.jbcrypt.BCrypt.gensalt;
import static org.mindrot.jbcrypt.BCrypt.hashpw;

public class CreateAndAlterRoleTest extends CQLTester
{
    @BeforeClass
    public static void setUpClass()
    {
        ServerTestUtils.daemonInitialization();

        DatabaseDescriptor.setPermissionsValidity(0);
        DatabaseDescriptor.setRolesValidity(0);
        DatabaseDescriptor.setCredentialsValidity(0);

        CQLTester.setUpClass();
        requireAuthentication();
        requireNetwork();
    }

    @Test
    public void createAlterRoleWithHashedPassword() throws Throwable
    {
        String user1 = "hashed_pw_role";
        String user2 = "pw_role";
        String plainTextPwd = "super_secret_thing";
        String plainTextPwd2 = "much_safer_password";
        String hashedPassword = hashpw(plainTextPwd, gensalt(4));
        String hashedPassword2 = hashpw(plainTextPwd2, gensalt(4));

        useSuperUser();

        assertInvalidMessage("Invalid hashed password value",
                             String.format("CREATE ROLE %s WITH login=true AND hashed password='%s'",
                                           user1, "this_is_an_invalid_hash"));
        assertInvalidMessage("Options 'password' and 'hashed password' are mutually exclusive",
                             String.format("CREATE ROLE %s WITH login=true AND password='%s' AND hashed password='%s'",
                                           user1, plainTextPwd, hashedPassword));
        executeNet(String.format("CREATE ROLE %s WITH login=true AND hashed password='%s'", user1, hashedPassword));
        executeNet(String.format("CREATE ROLE %s WITH login=true AND password='%s'", user2, plainTextPwd));

        useUser(user1, plainTextPwd);

        executeNet("SELECT key FROM system.local");

        useUser(user2, plainTextPwd);

        executeNet("SELECT key FROM system.local");

        useSuperUser();

        assertInvalidMessage("Options 'password' and 'hashed password' are mutually exclusive",
                             String.format("ALTER ROLE %s WITH password='%s' AND hashed password='%s'",
                                           user1, plainTextPwd2, hashedPassword2));
        executeNet(String.format("ALTER ROLE %s WITH password='%s'", user1, plainTextPwd2));
        executeNet(String.format("ALTER ROLE %s WITH hashed password='%s'", user2, hashedPassword2));

        useUser(user1, plainTextPwd2);

        executeNet("SELECT key FROM system.local");

        useUser(user2, plainTextPwd2);

        executeNet("SELECT key FROM system.local");
    }

    @Test
    public void createAlterUserWithHashedPassword() throws Throwable
    {
        String user1 = "hashed_pw_user";
        String user2 = "pw_user";
        String plainTextPwd = "super_secret_thing";
        String plainTextPwd2 = "much_safer_password";
        String hashedPassword = hashpw(plainTextPwd, gensalt(4));
        String hashedPassword2 = hashpw(plainTextPwd2, gensalt(4));

        useSuperUser();

        assertInvalidMessage("Invalid hashed password value",
                             String.format("CREATE USER %s WITH hashed password '%s'",
                                           user1, "this_is_an_invalid_hash"));
        executeNet(String.format("CREATE USER %s WITH hashed password '%s'", user1, hashedPassword));
        executeNet(String.format("CREATE USER %s WITH password '%s'",  user2, plainTextPwd));

        useUser(user1, plainTextPwd);

        executeNet("SELECT key FROM system.local");

        useUser(user2, plainTextPwd);

        executeNet("SELECT key FROM system.local");

        useSuperUser();

        executeNet(String.format("ALTER USER %s WITH password '%s'", user1, plainTextPwd2));
        executeNet(String.format("ALTER USER %s WITH hashed password '%s'", user2, hashedPassword2));

        useUser(user1, plainTextPwd2);

        executeNet("SELECT key FROM system.local");

        useUser(user2, plainTextPwd2);

        executeNet("SELECT key FROM system.local");
    }
}
