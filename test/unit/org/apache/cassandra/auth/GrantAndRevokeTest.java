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

import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;

import com.datastax.driver.core.ResultSet;
import org.apache.cassandra.ServerTestUtils;
import org.apache.cassandra.Util;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLTester;

import static org.junit.Assert.assertTrue;

public class GrantAndRevokeTest extends CQLTester
{
    private static final String user = "user";
    private static final String pass = "12345";

    @BeforeClass
    public static void setUpClass()
    {
        ServerTestUtils.daemonInitialization();
        DatabaseDescriptor.setPermissionsValidity(0);
        CQLTester.setUpClass();
        requireAuthentication();
        requireNetwork();
    }

    @After
    public void tearDown() throws Throwable
    {
        useSuperUser();
        executeNet("DROP ROLE " + user);
    }

    @Test
    public void testGrantedKeyspace() throws Throwable
    {
        useSuperUser();

        executeNet(String.format("CREATE ROLE %s WITH LOGIN = TRUE AND password='%s'", user, pass));
        executeNet("GRANT CREATE ON KEYSPACE " + KEYSPACE_PER_TEST + " TO " + user);
        String table = KEYSPACE_PER_TEST + '.' + createTable(KEYSPACE_PER_TEST, "CREATE TABLE %s (pk int, ck int, val int, val_2 text, PRIMARY KEY (pk, ck))");
        String index = KEYSPACE_PER_TEST + '.' + createIndex(KEYSPACE_PER_TEST, "CREATE INDEX ON %s (val_2)");
        String type = KEYSPACE_PER_TEST + '.' + createType(KEYSPACE_PER_TEST, "CREATE TYPE %s (a int, b text)");
        String mv = KEYSPACE_PER_TEST + ".ks_mv_01";
        executeNet("CREATE MATERIALIZED VIEW " + mv + " AS SELECT * FROM " + table + " WHERE val IS NOT NULL AND pk IS NOT NULL AND ck IS NOT NULL PRIMARY KEY (val, pk, ck)");

        useUser(user, pass);

        // ALTER and DROP tables created by somebody else
        // Spin assert for effective auth changes.
        final String spinAssertTable = table;
        Util.spinAssertEquals(false, () -> {
            try
            {
                assertUnauthorizedQuery("User user has no MODIFY permission on <table " + spinAssertTable + "> or any of its parents",
                                        formatQuery(KEYSPACE_PER_TEST, "INSERT INTO %s (pk, ck, val, val_2) VALUES (1, 1, 1, '1')"));
            }
            catch(Throwable e)
            {
                return true;
            }
            return false;
        }, 10);
        assertUnauthorizedQuery("User user has no MODIFY permission on <table " + table + "> or any of its parents",
                                formatQuery(KEYSPACE_PER_TEST, "UPDATE %s SET val = 1 WHERE pk = 1 AND ck = 1"));
        assertUnauthorizedQuery("User user has no MODIFY permission on <table " + table + "> or any of its parents",
                                formatQuery(KEYSPACE_PER_TEST, "DELETE FROM %s WHERE pk = 1 AND ck = 2"));
        assertUnauthorizedQuery("User user has no SELECT permission on <table " + table + "> or any of its parents",
                                formatQuery(KEYSPACE_PER_TEST, "SELECT * FROM %s WHERE pk = 1 AND ck = 1"));
        assertUnauthorizedQuery("User user has no SELECT permission on <table " + table + "> or any of its parents",
                                "SELECT * FROM " + mv + " WHERE val = 1 AND pk = 1 AND ck = 1");
        assertUnauthorizedQuery("User user has no MODIFY permission on <table " + table + "> or any of its parents",
                                formatQuery(KEYSPACE_PER_TEST, "TRUNCATE TABLE %s"));
        assertUnauthorizedQuery("User user has no ALTER permission on <table " + table + "> or any of its parents",
                                formatQuery(KEYSPACE_PER_TEST, "ALTER TABLE %s ADD val_3 int"));
        assertUnauthorizedQuery("User user has no DROP permission on <table " + table + "> or any of its parents",
                                formatQuery(KEYSPACE_PER_TEST, "DROP TABLE %s"));
        assertUnauthorizedQuery("User user has no ALTER permission on <all tables in " + KEYSPACE_PER_TEST + "> or any of its parents",
                                "ALTER TYPE " + type + " ADD c bigint");
        assertUnauthorizedQuery("User user has no DROP permission on <all tables in " + KEYSPACE_PER_TEST + "> or any of its parents",
                                "DROP TYPE " + type);
        assertUnauthorizedQuery("User user has no ALTER permission on <table " + table + "> or any of its parents",
                                "DROP MATERIALIZED VIEW " + mv);
        assertUnauthorizedQuery("User user has no ALTER permission on <table " + table + "> or any of its parents",
                                "DROP INDEX " + index);


        useSuperUser();

        executeNet("GRANT ALTER, DROP, SELECT, MODIFY ON KEYSPACE " + KEYSPACE_PER_TEST + " TO " + user);

        useUser(user, pass);

        // Spin assert for effective auth changes.
        Util.spinAssertEquals(false, () -> {
            try
            {
                executeNet("ALTER KEYSPACE " + KEYSPACE_PER_TEST + " WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}");
            }
            catch(Throwable e)
            {
                return true;
            }
            return false;
        }, 10);

        executeNet(formatQuery(KEYSPACE_PER_TEST, "INSERT INTO %s (pk, ck, val, val_2) VALUES (1, 1, 1, '1')"));
        executeNet(formatQuery(KEYSPACE_PER_TEST, "UPDATE %s SET val = 1 WHERE pk = 1 AND ck = 1"));
        executeNet(formatQuery(KEYSPACE_PER_TEST, "DELETE FROM %s WHERE pk = 1 AND ck = 2"));
        assertRowsNet(executeNet(formatQuery(KEYSPACE_PER_TEST, "SELECT * FROM %s WHERE pk = 1 AND ck = 1")), row(1, 1, 1, "1"));
        assertRowsNet(executeNet("SELECT * FROM " + mv + " WHERE val = 1 AND pk = 1"), row(1, 1, 1, "1"));
        executeNet(formatQuery(KEYSPACE_PER_TEST, "TRUNCATE TABLE %s"));
        executeNet(formatQuery(KEYSPACE_PER_TEST, "ALTER TABLE %s ADD val_3 int"));
        executeNet("DROP MATERIALIZED VIEW " + mv);
        executeNet("DROP INDEX " + index);
        executeNet(formatQuery(KEYSPACE_PER_TEST, "DROP TABLE %s"));
        executeNet("ALTER TYPE " + type + " ADD c bigint");
        executeNet("DROP TYPE " + type);

        // calling creatTableName to create a new table name that will be used by the formatQuery
        table = createTableName();
        type = KEYSPACE_PER_TEST + "." + createTypeName();
        mv = KEYSPACE_PER_TEST + ".ks_mv_02";
        executeNet("CREATE TYPE " + type + " (a int, b text)");
        executeNet(formatQuery(KEYSPACE_PER_TEST, "CREATE TABLE %s (pk int, ck int, val int, val_2 text, PRIMARY KEY (pk, ck))"));
        executeNet("CREATE MATERIALIZED VIEW " + mv + " AS SELECT * FROM " + table + " WHERE val IS NOT NULL AND pk IS NOT NULL AND ck IS NOT NULL PRIMARY KEY (val, pk, ck)");
        executeNet(formatQuery(KEYSPACE_PER_TEST, "INSERT INTO %s (pk, ck, val, val_2) VALUES (1, 1, 1, '1')"));
        executeNet(formatQuery(KEYSPACE_PER_TEST, "UPDATE %s SET val = 1 WHERE pk = 1 AND ck = 1"));
        executeNet(formatQuery(KEYSPACE_PER_TEST, "DELETE FROM %s WHERE pk = 1 AND ck = 2"));
        assertRowsNet(executeNet(formatQuery(KEYSPACE_PER_TEST, "SELECT * FROM %s WHERE pk = 1 AND ck = 1")), row(1, 1, 1, "1"));
        assertRowsNet(executeNet("SELECT * FROM " + mv + " WHERE val = 1 AND pk = 1"), row(1, 1, 1, "1"));
        executeNet(formatQuery(KEYSPACE_PER_TEST, "TRUNCATE TABLE %s"));
        executeNet(formatQuery(KEYSPACE_PER_TEST, "ALTER TABLE %s ADD val_3 int"));
        executeNet("DROP MATERIALIZED VIEW " + mv);
        executeNet(formatQuery(KEYSPACE_PER_TEST, "DROP TABLE %s"));
        executeNet("ALTER TYPE " + type + " ADD c bigint");
        executeNet("DROP TYPE " + type);

        useSuperUser();

        executeNet("REVOKE ALTER, DROP, MODIFY, SELECT ON KEYSPACE " + KEYSPACE_PER_TEST + " FROM " + user);

        table = KEYSPACE_PER_TEST + "." + createTable(KEYSPACE_PER_TEST, "CREATE TABLE %s (pk int, ck int, val int, val_2 text, PRIMARY KEY (pk, ck))");
        type = KEYSPACE_PER_TEST + "." + createType(KEYSPACE_PER_TEST, "CREATE TYPE %s (a int, b text)");
        index = KEYSPACE_PER_TEST + '.' + createIndex(KEYSPACE_PER_TEST, "CREATE INDEX ON %s (val_2)");
        mv = KEYSPACE_PER_TEST + ".ks_mv_03";
        executeNet("CREATE MATERIALIZED VIEW " + mv + " AS SELECT * FROM " + table + " WHERE val IS NOT NULL AND pk IS NOT NULL AND ck IS NOT NULL PRIMARY KEY (val, pk, ck)");

        useUser(user, pass);

        // Spin assert for effective auth changes.
        final String spinAssertTable2 = table;
        Util.spinAssertEquals(false, () -> {
            try
            {
                assertUnauthorizedQuery("User user has no MODIFY permission on <table " + spinAssertTable2 + "> or any of its parents",
                                        "INSERT INTO " + spinAssertTable2 + " (pk, ck, val, val_2) VALUES (1, 1, 1, '1')");
            }
            catch(Throwable e)
            {
                return true;
            }
            return false;
        }, 10);
        assertUnauthorizedQuery("User user has no MODIFY permission on <table " + table + "> or any of its parents",
                                "UPDATE " + table + " SET val = 1 WHERE pk = 1 AND ck = 1");
        assertUnauthorizedQuery("User user has no MODIFY permission on <table " + table + "> or any of its parents",
                                "DELETE FROM " + table + " WHERE pk = 1 AND ck = 2");
        assertUnauthorizedQuery("User user has no SELECT permission on <table " + table + "> or any of its parents",
                                "SELECT * FROM " + table + " WHERE pk = 1 AND ck = 1");
        assertUnauthorizedQuery("User user has no SELECT permission on <table " + table + "> or any of its parents",
                                "SELECT * FROM " + mv + " WHERE val = 1 AND pk = 1 AND ck = 1");
        assertUnauthorizedQuery("User user has no MODIFY permission on <table " + table + "> or any of its parents",
                                "TRUNCATE TABLE " + table);
        assertUnauthorizedQuery("User user has no ALTER permission on <table " + table + "> or any of its parents",
                                formatQuery(KEYSPACE_PER_TEST, "ALTER TABLE " + table + " ADD val_3 int"));
        assertUnauthorizedQuery("User user has no DROP permission on <table " + table + "> or any of its parents",
                                formatQuery(KEYSPACE_PER_TEST, "DROP TABLE " + table));
        assertUnauthorizedQuery("User user has no ALTER permission on <all tables in " + KEYSPACE_PER_TEST + "> or any of its parents",
                                "ALTER TYPE " + type + " ADD c bigint");
        assertUnauthorizedQuery("User user has no DROP permission on <all tables in " + KEYSPACE_PER_TEST + "> or any of its parents",
                                "DROP TYPE " + type);
        assertUnauthorizedQuery("User user has no ALTER permission on <table " + table + "> or any of its parents",
                                "DROP MATERIALIZED VIEW " + mv);
        assertUnauthorizedQuery("User user has no ALTER permission on <table " + table + "> or any of its parents",
                                "DROP INDEX " + index);

    }

    @Test
    public void testGrantedAllTables() throws Throwable
    {
        useSuperUser();

        executeNet(String.format("CREATE ROLE %s WITH LOGIN = TRUE AND password='%s'", user, pass));
        executeNet("GRANT CREATE ON ALL TABLES IN KEYSPACE " + KEYSPACE_PER_TEST + " TO " + user);
        String table = KEYSPACE_PER_TEST + "." + createTable(KEYSPACE_PER_TEST, "CREATE TABLE %s (pk int, ck int, val int, val_2 text, PRIMARY KEY (pk, ck))");
        String index = KEYSPACE_PER_TEST + '.' + createIndex(KEYSPACE_PER_TEST, "CREATE INDEX ON %s (val_2)");
        String type = KEYSPACE_PER_TEST + "." + createType(KEYSPACE_PER_TEST, "CREATE TYPE %s (a int, b text)");
        String mv = KEYSPACE_PER_TEST + ".alltables_mv_01";
        executeNet("CREATE MATERIALIZED VIEW " + mv + " AS SELECT * FROM " + table + " WHERE val IS NOT NULL AND pk IS NOT NULL AND ck IS NOT NULL PRIMARY KEY (val, pk, ck)");

        useUser(user, pass);

        // ALTER and DROP tables created by somebody else
        // Spin assert for effective auth changes.
        final String spinAssertTable = table;
        Util.spinAssertEquals(false, () -> {
            try
            {
                assertUnauthorizedQuery("User user has no MODIFY permission on <table " + spinAssertTable + "> or any of its parents",
                                        formatQuery(KEYSPACE_PER_TEST, "INSERT INTO %s (pk, ck, val, val_2) VALUES (1, 1, 1, '1')"));
            }
            catch(Throwable e)
            {
                return true;
            }
            return false;
        }, 10);
        assertUnauthorizedQuery("User user has no MODIFY permission on <table " + table + "> or any of its parents",
                                formatQuery(KEYSPACE_PER_TEST, "UPDATE %s SET val = 1 WHERE pk = 1 AND ck = 1"));
        assertUnauthorizedQuery("User user has no MODIFY permission on <table " + table + "> or any of its parents",
                                formatQuery(KEYSPACE_PER_TEST, "DELETE FROM %s WHERE pk = 1 AND ck = 2"));
        assertUnauthorizedQuery("User user has no SELECT permission on <table " + table + "> or any of its parents",
                                formatQuery(KEYSPACE_PER_TEST, "SELECT * FROM %s WHERE pk = 1 AND ck = 1"));
        assertUnauthorizedQuery("User user has no SELECT permission on <table " + table + "> or any of its parents",
                                "SELECT * FROM " + mv + " WHERE val = 1 AND pk = 1 AND ck = 1");
        assertUnauthorizedQuery("User user has no MODIFY permission on <table " + table + "> or any of its parents",
                                formatQuery(KEYSPACE_PER_TEST, "TRUNCATE TABLE %s"));
        assertUnauthorizedQuery("User user has no ALTER permission on <table " + table + "> or any of its parents",
                                formatQuery(KEYSPACE_PER_TEST, "ALTER TABLE %s ADD val_3 int"));
        assertUnauthorizedQuery("User user has no DROP permission on <table " + table + "> or any of its parents",
                                formatQuery(KEYSPACE_PER_TEST, "DROP TABLE %s"));
        assertUnauthorizedQuery("User user has no ALTER permission on <all tables in " + KEYSPACE_PER_TEST + "> or any of its parents",
                                "ALTER TYPE " + type + " ADD c bigint");
        assertUnauthorizedQuery("User user has no DROP permission on <all tables in " + KEYSPACE_PER_TEST + "> or any of its parents",
                                "DROP TYPE " + type);
        assertUnauthorizedQuery("User user has no ALTER permission on <table " + table + "> or any of its parents",
                                "DROP MATERIALIZED VIEW " + mv);
        assertUnauthorizedQuery("User user has no ALTER permission on <table " + table + "> or any of its parents",
                                "DROP INDEX " + index);

        useSuperUser();

        executeNet("GRANT ALTER, DROP, SELECT, MODIFY ON ALL TABLES IN KEYSPACE " + KEYSPACE_PER_TEST + " TO " + user);

        useUser(user, pass);

        // Spin assert for effective auth changes.
        Util.spinAssertEquals(false, () -> {
            try
            {
                assertUnauthorizedQuery("User user has no ALTER permission on <keyspace " + KEYSPACE_PER_TEST + "> or any of its parents",
                                        "ALTER KEYSPACE " + KEYSPACE_PER_TEST + " WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}");
            }
            catch(Throwable e)
            {
                return true;
            }
            return false;
        }, 10);
        executeNet(formatQuery(KEYSPACE_PER_TEST, "INSERT INTO %s (pk, ck, val, val_2) VALUES (1, 1, 1, '1')"));
        executeNet(formatQuery(KEYSPACE_PER_TEST, "UPDATE %s SET val = 1 WHERE pk = 1 AND ck = 1"));
        executeNet(formatQuery(KEYSPACE_PER_TEST, "DELETE FROM %s WHERE pk = 1 AND ck = 2"));
        assertRowsNet(executeNet(formatQuery(KEYSPACE_PER_TEST, "SELECT * FROM %s WHERE pk = 1 AND ck = 1")), row(1, 1, 1, "1"));
        assertRowsNet(executeNet("SELECT * FROM " + mv + " WHERE val = 1 AND pk = 1"), row(1, 1, 1, "1"));
        executeNet(formatQuery(KEYSPACE_PER_TEST, "TRUNCATE TABLE %s"));
        executeNet(formatQuery(KEYSPACE_PER_TEST, "ALTER TABLE %s ADD val_3 int"));
        executeNet("DROP MATERIALIZED VIEW " + mv);
        executeNet("DROP INDEX " + index);
        executeNet(formatQuery(KEYSPACE_PER_TEST, "DROP TABLE %s"));
        executeNet("ALTER TYPE " + type + " ADD c bigint");
        executeNet("DROP TYPE " + type);

        // calling creatTableName to create a new table name that will be used by the formatQuery
        table = createTableName();
        type = KEYSPACE_PER_TEST + "." + createTypeName();
        mv = KEYSPACE_PER_TEST + ".alltables_mv_02";
        executeNet("CREATE TYPE " + type + " (a int, b text)");
        executeNet(formatQuery(KEYSPACE_PER_TEST, "CREATE TABLE %s (pk int, ck int, val int, val_2 text, PRIMARY KEY (pk, ck))"));
        index = KEYSPACE_PER_TEST + '.' + createIndex(KEYSPACE_PER_TEST, "CREATE INDEX ON %s (val_2)");
        executeNet("CREATE MATERIALIZED VIEW " + mv + " AS SELECT * FROM " + table + " WHERE val IS NOT NULL AND pk IS NOT NULL AND ck IS NOT NULL PRIMARY KEY (val, pk, ck)");
        executeNet(formatQuery(KEYSPACE_PER_TEST, "INSERT INTO %s (pk, ck, val, val_2) VALUES (1, 1, 1, '1')"));
        executeNet(formatQuery(KEYSPACE_PER_TEST, "UPDATE %s SET val = 1 WHERE pk = 1 AND ck = 1"));
        executeNet(formatQuery(KEYSPACE_PER_TEST, "DELETE FROM %s WHERE pk = 1 AND ck = 2"));
        assertRowsNet(executeNet(formatQuery(KEYSPACE_PER_TEST, "SELECT * FROM %s WHERE pk = 1 AND ck = 1")), row(1, 1, 1, "1"));
        assertRowsNet(executeNet("SELECT * FROM " + mv + " WHERE val = 1 AND pk = 1"), row(1, 1, 1, "1"));
        executeNet(formatQuery(KEYSPACE_PER_TEST, "TRUNCATE TABLE %s"));
        executeNet(formatQuery(KEYSPACE_PER_TEST, "ALTER TABLE %s ADD val_3 int"));
        executeNet("DROP MATERIALIZED VIEW " + mv);
        executeNet("DROP INDEX " + index);
        executeNet(formatQuery(KEYSPACE_PER_TEST, "DROP TABLE %s"));
        executeNet("ALTER TYPE " + type + " ADD c bigint");
        executeNet("DROP TYPE " + type);

        useSuperUser();

        executeNet("REVOKE ALTER, DROP, SELECT, MODIFY ON ALL TABLES IN KEYSPACE " + KEYSPACE_PER_TEST + " FROM " + user);

        table = KEYSPACE_PER_TEST + "." + createTable(KEYSPACE_PER_TEST, "CREATE TABLE %s (pk int, ck int, val int, val_2 text, PRIMARY KEY (pk, ck))");
        index = KEYSPACE_PER_TEST + '.' + createIndex(KEYSPACE_PER_TEST, "CREATE INDEX ON %s (val_2)");
        type = KEYSPACE_PER_TEST + "." + createType(KEYSPACE_PER_TEST, "CREATE TYPE %s (a int, b text)");
        mv = KEYSPACE_PER_TEST + ".alltables_mv_03";
        executeNet("CREATE MATERIALIZED VIEW " + mv + " AS SELECT * FROM " + table + " WHERE val IS NOT NULL AND pk IS NOT NULL AND ck IS NOT NULL PRIMARY KEY (val, pk, ck)");

        useUser(user, pass);

        // Spin assert for effective auth changes.
        final String spinAssertTable2 = table;
        Util.spinAssertEquals(false, () -> {
            try
            {
                assertUnauthorizedQuery("User user has no MODIFY permission on <table " + spinAssertTable2 + "> or any of its parents",
                                        "INSERT INTO " + spinAssertTable2 + " (pk, ck, val, val_2) VALUES (1, 1, 1, '1')");
            }
            catch(Throwable e)
            {
                return true;
            }
            return false;
        }, 10);
        assertUnauthorizedQuery("User user has no MODIFY permission on <table " + table + "> or any of its parents",
                                "UPDATE " + table + " SET val = 1 WHERE pk = 1 AND ck = 1");
        assertUnauthorizedQuery("User user has no MODIFY permission on <table " + table + "> or any of its parents",
                                "DELETE FROM " + table + " WHERE pk = 1 AND ck = 2");
        assertUnauthorizedQuery("User user has no SELECT permission on <table " + table + "> or any of its parents",
                                "SELECT * FROM " + table + " WHERE pk = 1 AND ck = 1");
        assertUnauthorizedQuery("User user has no SELECT permission on <table " + table + "> or any of its parents",
                                "SELECT * FROM " + mv + " WHERE val = 1 AND pk = 1 AND ck = 1");
        assertUnauthorizedQuery("User user has no MODIFY permission on <table " + table + "> or any of its parents",
                                "TRUNCATE TABLE " + table);
        assertUnauthorizedQuery("User user has no ALTER permission on <table " + table + "> or any of its parents",
                                formatQuery(KEYSPACE_PER_TEST, "ALTER TABLE " + table + " ADD val_3 int"));
        assertUnauthorizedQuery("User user has no DROP permission on <table " + table + "> or any of its parents",
                                formatQuery(KEYSPACE_PER_TEST, "DROP TABLE " + table));
        assertUnauthorizedQuery("User user has no ALTER permission on <all tables in " + KEYSPACE_PER_TEST + "> or any of its parents",
                                "ALTER TYPE " + type + " ADD c bigint");
        assertUnauthorizedQuery("User user has no DROP permission on <all tables in " + KEYSPACE_PER_TEST + "> or any of its parents",
                                "DROP TYPE " + type);
        assertUnauthorizedQuery("User user has no ALTER permission on <table " + table + "> or any of its parents",
                                "DROP MATERIALIZED VIEW " + mv);
        assertUnauthorizedQuery("User user has no ALTER permission on <table " + table + "> or any of its parents",
                                "DROP INDEX " + index);
    }

    @Test
    public void testWarnings() throws Throwable
    {
        useSuperUser();

        executeNet("CREATE KEYSPACE revoke_yeah WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}");
        executeNet("CREATE TABLE revoke_yeah.t1 (id int PRIMARY KEY, val text)");
        executeNet("CREATE USER '" + user + "' WITH PASSWORD '" + pass + "'");

        ResultSet res = executeNet("REVOKE CREATE ON KEYSPACE revoke_yeah FROM " + user);
        assertWarningsContain(res.getExecutionInfo().getWarnings(), "Role '" + user + "' was not granted CREATE on <keyspace revoke_yeah>");

        res = executeNet("GRANT SELECT ON KEYSPACE revoke_yeah TO " + user);
        assertTrue(res.getExecutionInfo().getWarnings().isEmpty());

        res = executeNet("GRANT SELECT ON KEYSPACE revoke_yeah TO " + user);
        assertWarningsContain(res.getExecutionInfo().getWarnings(), "Role '" + user + "' was already granted SELECT on <keyspace revoke_yeah>");

        res = executeNet("REVOKE SELECT ON TABLE revoke_yeah.t1 FROM " + user);
        assertWarningsContain(res.getExecutionInfo().getWarnings(), "Role '" + user + "' was not granted SELECT on <table revoke_yeah.t1>");

        res = executeNet("REVOKE SELECT, MODIFY ON KEYSPACE revoke_yeah FROM " + user);
        assertWarningsContain(res.getExecutionInfo().getWarnings(), "Role '" + user + "' was not granted MODIFY on <keyspace revoke_yeah>");
    }
}
