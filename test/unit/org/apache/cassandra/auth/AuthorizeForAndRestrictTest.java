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

import java.util.concurrent.Callable;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.Util;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.tools.ToolRunner;

public class AuthorizeForAndRestrictTest extends CQLTester
{
    @BeforeClass
    public static void setup() throws Throwable
    {
        requireAuthentication();
        DatabaseDescriptor.setPermissionsValidity(9999);
        DatabaseDescriptor.setPermissionsUpdateInterval(9999);
        requireNetwork();

        startJMXServer();
    }

    @Test
    public void testAuthorizeFor() throws Throwable
    {
        useSuperUser();

        executeNet("CREATE USER authfor1 WITH PASSWORD 'pass1'");
        executeNet("CREATE USER authfor2 WITH PASSWORD 'pass2'");
        executeNet("CREATE ROLE authfor_role1");
        executeNet("GRANT authfor_role1 TO authfor1");

        executeNet("CREATE KEYSPACE authfor_test WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}");
        executeNet("CREATE TABLE authfor_test.t1 (id int PRIMARY KEY, val text)");
        executeNet("CREATE TABLE authfor_test.t2 (id int PRIMARY KEY, val text)");

        executeNet("GRANT AUTHORIZE FOR SELECT ON KEYSPACE authfor_test TO authfor1");
        executeNet("GRANT MODIFY ON TABLE authfor_test.t1 TO authfor1");
        executeNet("GRANT AUTHORIZE FOR MODIFY ON TABLE authfor_test.t2 TO authfor2");
        executeNet("GRANT MODIFY ON TABLE authfor_test.t2 TO authfor2");


        useUser("authfor1", "pass1");

        // SELECT on table t1 must not work, authfor1 only has the privilege to grant the SELECT permission
        assertUnauthorizedQuery("User authfor1 has no SELECT permission on <table authfor_test.t1> or any of its parents",
                                "SELECT * FROM authfor_test.t1");

        // authfor1 must not be able to grant the SELECT permission to himself
        assertUnauthorizedQuery("User authfor1 has grant privilege for SELECT permission(s) on <table authfor_test.t1> but must not grant/revoke for him/herself",
                                "GRANT SELECT ON TABLE authfor_test.t1 TO authfor1");

        // authfor1 must not be able to grant the SELECT permission to himself - even via a role
        assertUnauthorizedQuery("User authfor1 has grant privilege for SELECT permission(s) on <table authfor_test.t1> but must not grant/revoke for him/herself",
                                "GRANT SELECT ON TABLE authfor_test.t1 TO authfor_role1");

        // authfor1 has MODIFIY permission on t1 but not the privilege to grant the MODIFY permission
        assertUnauthorizedQuery("User authfor1 has no AUTHORIZE permission nor AUTHORIZE FOR MODIFY permission on <table authfor_test.t1> or any of its parents",
                                "GRANT MODIFY ON TABLE authfor_test.t1 to authfor2");

        assertUnauthorizedQuery("User authfor1 must not grant AUTHORIZE FOR AUTHORIZE permission on <keyspace authfor_test>",
                                "GRANT AUTHORIZE FOR SELECT ON KEYSPACE authfor_test TO authfor2");

        // authfor1 can grant the SELECT privilege - all that must work (although the GRANT on the keyspace is technically sufficient)
        executeNet("GRANT SELECT ON KEYSPACE authfor_test TO authfor2");
        executeNet("GRANT SELECT ON TABLE authfor_test.t1 TO authfor2");
        executeNet("GRANT SELECT ON TABLE authfor_test.t2 TO authfor2");

        // authfor1 has no MODIFIY permission on t2
        assertUnauthorizedQuery("User authfor1 has no MODIFY permission on <table authfor_test.t2> or any of its parents",
                                "INSERT INTO authfor_test.t2 (id, val) VALUES (1, 'foo')");

        useUser("authfor2", "pass2");

        // authfor2 has SELECT permission on t1
        executeNet("SELECT * FROM authfor_test.t1");

        // authfor2 has no MODIFY permission on t1
        assertUnauthorizedQuery("User authfor2 has no MODIFY permission on <table authfor_test.t1> or any of its parents",
                                "INSERT INTO authfor_test.t1 (id, val) VALUES (1, 'foo')");

        // authfor2 has the privilege to grant the MODIFY permission
        executeNet("GRANT MODIFY ON TABLE authfor_test.t2 TO authfor1");

        // authfor2 has MODIFY permission on t2
        executeNet("INSERT INTO authfor_test.t2 (id, val) VALUES (1, 'foo')");

        // authfor2 has SElECT permission on t2
        assertRowsNet(executeNet("SELECT id, val FROM authfor_test.t2"),
                      new Object[] { 1, "foo" });

        useUser("authfor1", "pass1");

        // attn: the permission is still cached !

        assertUnauthorizedQuery("User authfor1 has no MODIFY permission on <table authfor_test.t2> or any of its parents",
                                "INSERT INTO authfor_test.t2 (id, val) VALUES (2, 'bar')");

        // need to invalidate the permissions cache
        // now the CQL works
        clearAuthCacheSpin(() -> {
            try
            {
                executeNet("INSERT INTO authfor_test.t2 (id, val) VALUES (2, 'bar')");
                return true;
            }
            catch(Throwable e)
            {
                return false;
            }
        });

        useUser("authfor2", "pass2");

        assertRowsNet(executeNet("SELECT id, val FROM authfor_test.t2"),
                      row(1, "foo"),
                      row(2, "bar"));

        useSuperUser();
        assertRowsNet(executeNet("LIST ALL PERMISSIONS OF authfor1"),
                      row("authfor1", "authfor1", "<keyspace authfor_test>", "SELECT", false, false, true),
                      row("authfor1", "authfor1", "<table authfor_test.t1>", "MODIFY", true, false, false),
                      row("authfor1", "authfor1", "<table authfor_test.t2>", "MODIFY", true, false, false));
        assertRowsNet(executeNet("LIST ALL PERMISSIONS OF authfor2"),
                      row("authfor2", "authfor2", "<keyspace authfor_test>", "SELECT", true, false, false),
                      row("authfor2", "authfor2", "<table authfor_test.t1>", "SELECT", true, false, false),
                      row("authfor2", "authfor2", "<table authfor_test.t2>", "SELECT", true, false, false),
                      row("authfor2", "authfor2", "<table authfor_test.t2>", "MODIFY", true, false, true));

        // all permissions and grant options must have been removed
        executeNet("DROP ROLE authfor1");
        executeNet("CREATE USER authfor1 WITH PASSWORD 'pass1'");

        assertRowsNet(executeNet("LIST ALL PERMISSIONS OF authfor1"));
        assertRowsNet(executeNet("LIST ALL PERMISSIONS OF authfor2"),
                      row("authfor2", "authfor2", "<keyspace authfor_test>", "SELECT", true, false, false),
                      row("authfor2", "authfor2", "<table authfor_test.t1>", "SELECT", true, false, false),
                      row("authfor2", "authfor2", "<table authfor_test.t2>", "SELECT", true, false, false),
                      row("authfor2", "authfor2", "<table authfor_test.t2>", "MODIFY", true, false, true));

        // all permissions and grant options must have been removed
        executeNet("DROP ROLE authfor2");
        executeNet("CREATE USER authfor2 WITH PASSWORD 'pass2'");

        assertRowsNet(executeNet("LIST ALL PERMISSIONS OF authfor1"));
        assertRowsNet(executeNet("LIST ALL PERMISSIONS OF authfor2"));
    }

    @Test
    public void testRestrict() throws Throwable
    {
        // Test story:
        // - one table
        // - one security admin user (restrict1), which must not gain access to a resource (the table)
        // - one role that has legit access to that table
        // - security admin user is granted the role
        // - security admin user must still not be able to access that table
        // - security admin user must not be able to "unrestrict"

        useSuperUser();

        executeNet("CREATE KEYSPACE restrict_test WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}");
        executeNet("CREATE TABLE restrict_test.t1 (id int PRIMARY KEY, val text)");

        executeNet("CREATE ROLE role_restrict");
        executeNet("CREATE USER restrict1 WITH PASSWORD 'restrict1'");

        executeNet("GRANT AUTHORIZE FOR SELECT ON KEYSPACE restrict_test TO restrict1");


        useUser("restrict1", "restrict1");

        assertUnauthorizedQuery("User restrict1 has no SELECT permission on <table restrict_test.t1> or any of its parents",
                                "SELECT * FROM restrict_test.t1");

        executeNet("GRANT SELECT ON KEYSPACE restrict_test TO role_restrict");

        assertUnauthorizedQuery("User restrict1 has no SELECT permission on <table restrict_test.t1> or any of its parents",
                                "SELECT * FROM restrict_test.t1");

        useSuperUser();
        executeNet("GRANT role_restrict TO restrict1");

        useUser("restrict1", "restrict1");

        clearAuthCacheSpin(() -> {
            try
            {
                assertRowsNet(executeNet("SELECT * FROM restrict_test.t1"));
                return true;
            }
            catch(Throwable e)
            {
                return false;
            }
        });

        useSuperUser();
        executeNet("RESTRICT SELECT ON TABLE restrict_test.t1 TO restrict1");

        useUser("restrict1", "restrict1");
        clearAuthCacheSpin(() -> {
            try
            {
                assertUnauthorizedQuery("Access for user restrict1 on <table restrict_test.t1> or any of its parents with SELECT permission is restricted",
                                        "SELECT * FROM restrict_test.t1");
                return true;
            }
            catch(Throwable e)
            {
                return false;
            }
        });

        assertUnauthorizedQuery("Only superusers are allowed to RESTRICT/UNRESTRICT",
                                "UNRESTRICT SELECT ON TABLE restrict_test.t1 FROM restrict1");

        assertUnauthorizedQuery("Access for user restrict1 on <table restrict_test.t1> or any of its parents with SELECT permission is restricted",
                                "SELECT * FROM restrict_test.t1");
    }

    private void clearAuthCacheSpin(Callable<Boolean> task)
    {
        Util.spinAssertEquals(true, () -> {
            try
            {
                ToolRunner.ToolResult tool = ToolRunner.invokeNodetool("invalidatepermissionscache");
                tool.assertOnCleanExit();
                return task.call();
            }
            catch(Throwable e)
            {
                return false;
            }
        }, 10);
    }
}
