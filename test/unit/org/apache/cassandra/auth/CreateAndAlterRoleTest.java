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

import java.util.HashSet;
import java.util.Set;

import org.junit.BeforeClass;
import org.junit.Test;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.exceptions.AuthenticationException;
import com.datastax.driver.core.exceptions.InvalidQueryException;
import org.apache.cassandra.Util;
import org.apache.cassandra.cql3.CQLTester;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mindrot.jbcrypt.BCrypt.gensalt;
import static org.mindrot.jbcrypt.BCrypt.hashpw;

public class CreateAndAlterRoleTest extends CQLTester
{
    @BeforeClass
    public static void setUpClass()
    {
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

        executeNetWithAuthSpin("SELECT key FROM system.local");

        useUser(user2, plainTextPwd);

        executeNetWithAuthSpin("SELECT key FROM system.local");

        useSuperUser();

        assertInvalidMessage("Options 'password' and 'hashed password' are mutually exclusive",
                             String.format("ALTER ROLE %s WITH password='%s' AND hashed password='%s'",
                                           user1, plainTextPwd2, hashedPassword2));
        executeNet(String.format("ALTER ROLE %s WITH password='%s'", user1, plainTextPwd2));
        executeNet(String.format("ALTER ROLE %s WITH hashed password='%s'", user2, hashedPassword2));

        useUser(user1, plainTextPwd2);

        executeNetWithAuthSpin("SELECT key FROM system.local");

        useUser(user2, plainTextPwd2);

        executeNetWithAuthSpin("SELECT key FROM system.local");
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
        executeNet(String.format("CREATE USER %s WITH password '%s'", user2, plainTextPwd));

        useUser(user1, plainTextPwd);

        executeNetWithAuthSpin("SELECT key FROM system.local");

        useUser(user2, plainTextPwd);

        executeNetWithAuthSpin("SELECT key FROM system.local");

        useSuperUser();

        executeNet(String.format("ALTER USER %s WITH password '%s'", user1, plainTextPwd2));
        executeNet(String.format("ALTER USER %s WITH hashed password '%s'", user2, hashedPassword2));

        useUser(user1, plainTextPwd2);

        executeNetWithAuthSpin("SELECT key FROM system.local");

        useUser(user2, plainTextPwd2);

        executeNetWithAuthSpin("SELECT key FROM system.local");
    }

    @Test
    public void createAlterRoleIfExists() throws Throwable
    {
        useSuperUser();

        executeNet("CREATE ROLE IF NOT EXISTS does_not_exist_yet");
        assertTrue(getAllRoles().contains("does_not_exist_yet"));

        // execute one more time
        executeNet("CREATE ROLE IF NOT EXISTS does_not_exist_yet");

        assertThatThrownBy(() -> executeNet("CREATE ROLE does_not_exist_yet"))
        .isInstanceOf(InvalidQueryException.class)
        .hasMessageContaining("does_not_exist_yet already exists");

        // alter non-existing is no-op when "if exists" is specified
        executeNet("ALTER ROLE IF EXISTS also_does_not_exist_yet WITH LOGIN = true");
        Set<String> roles = getAllRoles();
        assertTrue(roles.contains("does_not_exist_yet"));
        // not created - CASSANDRA-19749
        assertFalse(roles.contains("also_does_not_exist_yet"));
    }

    private Set<String> getAllRoles() throws Throwable
    {
        ResultSet rows = executeNet("SELECT role FROM system_auth.roles");
        Set<String> roles = new HashSet<>();
        rows.forEach(row -> roles.add(row.getString(0)));
        return roles;
    }

    /**
     * Altering or creating auth may take some time to be effective
     *
     * @param query
     */
    void executeNetWithAuthSpin(String query)
    {
        Util.spinAssertEquals(true, () -> {
            try
            {
                executeNet(query);
                return true;
            }
            catch (Throwable e)
            {
                assertTrue("Unexpected exception: " + e, e instanceof AuthenticationException);
                reinitializeNetwork();
                return false;
            }
        }, 10);
    }
}
