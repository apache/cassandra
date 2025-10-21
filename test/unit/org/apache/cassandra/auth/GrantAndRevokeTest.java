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

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import com.google.common.collect.Iterables;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;

import com.datastax.driver.core.ResultSet;
import org.apache.cassandra.ServerTestUtils;
import org.apache.cassandra.Util;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.CassandraDaemon;
import org.apache.cassandra.transport.ProtocolVersion;

import static java.lang.String.format;
import static org.apache.cassandra.schema.SchemaConstants.LOCAL_SYSTEM_KEYSPACE_NAMES;
import static org.apache.cassandra.schema.SchemaConstants.REPLICATED_SYSTEM_KEYSPACE_NAMES;
import static org.apache.cassandra.schema.SchemaConstants.SCHEMA_KEYSPACE_NAME;
import static org.apache.cassandra.schema.SchemaConstants.SYSTEM_KEYSPACE_NAME;
import static org.apache.cassandra.schema.SchemaConstants.TRACE_KEYSPACE_NAME;
import static org.junit.Assert.assertTrue;

public class GrantAndRevokeTest extends CQLTester
{
    private static final String user = "user";
    private static final String pass = "12345";

    @BeforeClass
    public static void setUpAuth()
    {
        ServerTestUtils.daemonInitialization();
        DatabaseDescriptor.setPermissionsValidity(0);
        DatabaseDescriptor.setRolesValidity(0);
        requireAuthentication();
        requireNetwork();
        CassandraDaemon.getInstanceForTesting().setupVirtualKeyspaces();
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

    @Test
    public void testCreateTableLikeAuthorize() throws Throwable
    {
        useSuperUser();

        // two keyspaces
        executeNet("CREATE KEYSPACE ks1 WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}");
        executeNet("CREATE KEYSPACE ks2 WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}");
        executeNet("CREATE TABLE ks1.sourcetb (id int PRIMARY KEY, val text)");
        executeNet("CREATE USER '" + user + "' WITH PASSWORD '" + pass + "'");

        // same keyspace
        // have no select permission on source table
        ResultSet res = executeNet("REVOKE SELECT ON TABLE ks1.sourcetb FROM " + user);
        assertWarningsContain(res.getExecutionInfo().getWarnings(), "Role '" + user + "' was not granted SELECT on <table ks1.sourcetb>");

        useUser(user, pass);
        // Spin assert for effective auth changes.
        Util.spinAssertEquals(false, () -> {
            try
            {
                assertUnauthorizedQuery("User user has no SELECT permission on <table ks1.sourcetb> or any of its parents",
                                        formatQuery("SELECT * FROM ks1.sourcetb LIMIT 1"));
            }
            catch (Throwable e)
            {
                return true;
            }
            return false;
        }, 10);

        assertUnauthorizedQuery("User user has no SELECT permission on <table ks1.sourcetb> or any of its parents",
                                "CREATE TABLE ks1.targetTb LIKE ks1.sourcetb");

        // have select permission on source table and do not have create permission on target keyspace
        useSuperUser();
        executeNet("GRANT SELECT ON TABLE ks1.sourcetb TO " + user);
        res = executeNet("REVOKE CREATE ON KEYSPACE ks1 FROM " + user);
        assertWarningsContain(res.getExecutionInfo().getWarnings(), "Role '" + user + "' was not granted CREATE on <keyspace ks1>");

        useUser(user, pass);
        Util.spinAssertEquals(false, () -> {
            try
            {
                assertUnauthorizedQuery("User user has no CREATE permission on <all tables in ks1> or any of its parents",
                                        formatQuery("CREATE TABLE ks1.targetTb LIKE ks1.sourcetb"));
            }
            catch (Throwable e)
            {
                return true;
            }
            return false;
        }, 10);

        assertUnauthorizedQuery("User user has no CREATE permission on <all tables in ks1> or any of its parents",
                                "CREATE TABLE ks1.targetTb LIKE ks1.sourcetb");

        // different keyspaces
        // have select permission on source table and do not have create permission on target keyspace
        useSuperUser();
        executeNet("GRANT SELECT ON TABLE ks1.sourcetb TO " + user);
        res = executeNet("REVOKE CREATE ON KEYSPACE ks2 FROM " + user);
        assertWarningsContain(res.getExecutionInfo().getWarnings(), "Role '" + user + "' was not granted CREATE on <keyspace ks2>");

        useUser(user, pass);
        Util.spinAssertEquals(false, () -> {
            try
            {
                assertUnauthorizedQuery("User user has no CREATE permission on <all tables in ks2> or any of its parents",
                                        formatQuery("CREATE TABLE ks2.targetTb LIKE ks1.sourcetb"));
            }
            catch (Throwable e)
            {
                return true;
            }
            return false;
        }, 10);

        assertUnauthorizedQuery("User user has no CREATE permission on <all tables in ks2> or any of its parents",
                                "CREATE TABLE ks2.targetTb LIKE ks1.sourcetb");

        // source keyspace and table do not exist
        assertUnauthorizedQuery("User user has no SELECT permission on <table ks1.tbnotexist> or any of its parents",
                                "CREATE TABLE ks2.targetTb LIKE ks1.tbnotexist");
        assertUnauthorizedQuery("User user has no SELECT permission on <table ksnotexists.sourcetb> or any of its parents",
                                "CREATE TABLE ks2.targetTb LIKE ksnotexists.sourcetb");
        // target keyspace does not exist
        assertUnauthorizedQuery("User user has no CREATE permission on <all tables in ksnotexists> or any of its parents",
                                "CREATE TABLE ksnotexists.targetTb LIKE ks1.sourcetb");
    }

    @Test
    public void testSpecificGrantsOnSystemKeyspaces() throws Throwable
    {
        // Granting specific permissions on system keyspaces should not be allowed if those permissions include any from
        // the denylist Permission.INVALID_FOR_SYSTEM_KEYSPACES. By this definition, GRANT ALL on any system keyspace,
        // or a table within one, should be rejected.
        useSuperUser();
        executeNet("CREATE ROLE '" + user + "'");
        String responseMsg = "Granting permissions on system keyspaces is strictly limited, this operation is not permitted";
        for (String keyspace : Iterables.concat(LOCAL_SYSTEM_KEYSPACE_NAMES, REPLICATED_SYSTEM_KEYSPACE_NAMES))
        {
            assertUnauthorizedQuery(responseMsg, format("GRANT ALL PERMISSIONS ON KEYSPACE %s TO %s", keyspace, user));
            DataResource keyspaceResource = DataResource.keyspace(keyspace);
            for (Permission p : keyspaceResource.applicablePermissions())
                maybeRejectGrant(p, responseMsg, format("GRANT %s ON KEYSPACE %s TO %s", p.name(), keyspace, user));

            assertUnauthorizedQuery(responseMsg, format("GRANT ALL PERMISSIONS ON ALL TABLES IN KEYSPACE %s TO %s", keyspace, user));
            for (TableMetadata table : Schema.instance.getKeyspaceMetadata(keyspace).tables)
            {
                DataResource tableResource = DataResource.table(keyspace, table.name);
                assertUnauthorizedQuery(responseMsg, format("GRANT ALL PERMISSIONS ON %s TO %s", table, user));
                for (Permission p : tableResource.applicablePermissions())
                    maybeRejectGrant(p, responseMsg, format("GRANT %s ON %s TO %s", p.name(), table, user));
            }
        }
    }

    @Test
    public void testGrantOnAllKeyspaces() throws Throwable
    {
        // Granting either specific or ALL permissions on ALL KEYSPACES is allowed, however these permissions are
        // effective for non-system keyspaces only. If for any reason it is necessary to modify permissions on
        // on a system keyspace, it must be done using keyspace specific grant statements.
        useSuperUser();
        executeNet(String.format("CREATE ROLE %s WITH LOGIN = TRUE AND password='%s'", user, pass));
        executeNet(String.format("ALTER KEYSPACE %s WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}", SchemaConstants.TRACE_KEYSPACE_NAME));
        executeNet("CREATE KEYSPACE user_keyspace WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}");
        executeNet("CREATE TABLE user_keyspace.t1 (k int PRIMARY KEY)");
        useUser(user, pass);

        assertUnauthorizedQuery("User user has no MODIFY permission on <table user_keyspace.t1> or any of its parents",
                                "INSERT INTO user_keyspace.t1 (k) VALUES (0)");
        assertUnauthorizedQuery("User user has no MODIFY permission on <table system.local> or any of its parents",
                                "INSERT INTO system.local(key) VALUES ('invalid')");

        useSuperUser();
        executeNet(ProtocolVersion.CURRENT, format("GRANT MODIFY ON ALL KEYSPACES TO %s", user));

        useUser(user, pass);
        // User now has write permission on non-system keyspaces only
        executeNet(ProtocolVersion.CURRENT, "INSERT INTO user_keyspace.t1 (k) VALUES (0)");
        assertUnauthorizedQuery("User user has no MODIFY permission on <table system.local> or any of its parents",
                                "INSERT INTO system.local(key) VALUES ('invalid')");

        // A non-superuser only has read access to a pre-defined set of system tables and all system_schema/traces
        // tables and granting ALL permissions on ALL keyspaces also does not affect this.
        maybeReadSystemTables(false);
        useSuperUser();
        executeNet(ProtocolVersion.CURRENT, format("GRANT ALL PERMISSIONS ON ALL KEYSPACES TO %s", user));
        maybeReadSystemTables(false);

        // A superuser can still read system tables
        useSuperUser();
        maybeReadSystemTables(true);
        // and also write to them, though this is still strongly discouraged
        executeNet(ProtocolVersion.CURRENT, "INSERT INTO system.peers_v2(peer, peer_port, data_center) VALUES ('127.0.100.100', 7012, 'invalid_dc')");
    }

    @Test
    public void testGrantOnVirtualKeyspaces() throws Throwable
    {
        useSuperUser();
        executeNet(String.format("CREATE ROLE %s WITH LOGIN = TRUE AND password='%s'", user, pass));

        executeNet(ProtocolVersion.CURRENT, format("GRANT SELECT PERMISSION ON KEYSPACE system_virtual_schema TO %s", user));
        executeNet(ProtocolVersion.CURRENT, format("GRANT SELECT PERMISSION ON KEYSPACE system_views TO %s", user));
        executeNet(ProtocolVersion.CURRENT, format("REVOKE SELECT PERMISSION ON KEYSPACE system_virtual_schema FROM %s", user));
        executeNet(ProtocolVersion.CURRENT, format("REVOKE SELECT PERMISSION ON KEYSPACE system_views FROM %s", user));
    }

    private void maybeReadSystemTables(boolean superuser) throws Throwable
    {
        if (superuser)
            useSuperUser();
        else
            useUser(user, pass);

        Set<String> readableKeyspaces = new HashSet<>(Arrays.asList(SCHEMA_KEYSPACE_NAME, TRACE_KEYSPACE_NAME));
        Set<String> readableSystemTables = new HashSet<>(Arrays.asList(SystemKeyspace.LOCAL,
                                                                       SystemKeyspace.PEERS_V2,
                                                                       SystemKeyspace.LEGACY_PEERS,
                                                                       SystemKeyspace.LEGACY_SIZE_ESTIMATES,
                                                                       SystemKeyspace.TABLE_ESTIMATES));

        for (String keyspace : Iterables.concat(LOCAL_SYSTEM_KEYSPACE_NAMES, REPLICATED_SYSTEM_KEYSPACE_NAMES))
        {
            for (TableMetadata table : Schema.instance.getKeyspaceMetadata(keyspace).tables)
            {
                if (superuser || (readableKeyspaces.contains(keyspace) || (keyspace.equals(SYSTEM_KEYSPACE_NAME) && readableSystemTables.contains(table.name))))
                {
                    executeNet(ProtocolVersion.CURRENT, ConsistencyLevel.ONE, format("SELECT * FROM %s LIMIT 1", table));
                }
                else
                {
                    assertUnauthorizedQuery(format("User %s has no SELECT permission on %s or any of its parents", user, table.resource),
                                            format("SELECT * FROM %s LIMIT 1", table));
                }
            }
        }
    }

    private void maybeRejectGrant(Permission p, String errorResponse, String grant) throws Throwable
    {
        if (Permission.INVALID_FOR_SYSTEM_KEYSPACES.contains(p))
            assertUnauthorizedQuery(errorResponse, grant);
        else
            executeNet(ProtocolVersion.CURRENT, grant);
    }
}
