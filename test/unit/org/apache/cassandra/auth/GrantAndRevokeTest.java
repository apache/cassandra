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
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import com.google.common.collect.Iterables;
import org.junit.After;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.PlainTextAuthProvider;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.exceptions.UnauthorizedException;
import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.Util;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.config.SchemaConstants;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.metrics.ClientMetrics;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.Types;
import org.apache.cassandra.transport.ConfiguredLimit;
import org.apache.cassandra.transport.Server;
import org.apache.cassandra.utils.Pair;

import static java.lang.String.format;
import static org.apache.cassandra.config.SchemaConstants.LOCAL_SYSTEM_KEYSPACE_NAMES;
import static org.apache.cassandra.config.SchemaConstants.REPLICATED_SYSTEM_KEYSPACE_NAMES;

public class GrantAndRevokeTest extends CQLTester
{
    private static final String user = "user";
    private static final String pass = "12345";
    private static final Pair<String, String> USER = Pair.create(user, pass);
    private static final Pair<String, String> SUPERUSER = Pair.create("cassandra", "cassandra");
    private static int counter = 0;

    @BeforeClass
    public static void setUpClass()
    {
        System.setProperty("cassandra.superuser_setup_delay_ms", "0");
        DatabaseDescriptor.setPermissionsValidity(0);
        CQLTester.setUpClass();
        SchemaLoader.createKeyspace(SchemaConstants.AUTH_KEYSPACE_NAME, KeyspaceParams.simple(1), AuthKeyspace.metadata().tables, Types.none());

        DatabaseDescriptor.setAuthenticator(new PasswordAuthenticator());
        DatabaseDescriptor.setRoleManager(new CassandraRoleManager());
        DatabaseDescriptor.setAuthorizer(new CassandraAuthorizer());
        // needed as the driver reads system.local to get the release version, which determines the
        // schema tables it attempts to read (current, or legacy in case it's connecting to a 2.2 node)
        SystemKeyspace.persistLocalMetadata();
        prepareNetwork();
        protocolVersionLimit = ConfiguredLimit.newLimit();
        server = new Server.Builder().withHost(nativeAddr)
                                     .withPort(nativePort)
                                     .withProtocolVersionLimit(protocolVersionLimit)
                                     .build();
        ClientMetrics.instance.init(Collections.singleton(server));
        server.start();
    }

    @After
    public void tearDown() throws Throwable
    {
        session(SUPERUSER).execute("DROP ROLE " + user);
    }

    private Session session(Pair<String, String> credentials)
    {
        Cluster cluster = Cluster.builder()
                                 .addContactPoints(nativeAddr)
                                 .withClusterName("Test Cluster " + counter++)
                                 .withPort(nativePort)
                                 .withAuthProvider(new PlainTextAuthProvider(credentials.left, credentials.right))
                                 .build();

        return cluster.connect();
    }

    @Test
    public void testGrantedKeyspace() throws Throwable
    {
        Session superuser = session(SUPERUSER);
        superuser.execute(String.format("CREATE ROLE %s WITH LOGIN = TRUE AND password='%s'", user, pass));
        superuser.execute("GRANT CREATE ON KEYSPACE " + KEYSPACE_PER_TEST + " TO " + user);
        String table = KEYSPACE_PER_TEST + '.' + createTable(KEYSPACE_PER_TEST, "CREATE TABLE %s (pk int, ck int, val int, val_2 text, PRIMARY KEY (pk, ck))");
        String index = KEYSPACE_PER_TEST + ".idx_01";
        createIndex(KEYSPACE_PER_TEST, "CREATE INDEX idx_01 ON %s (val_2)");
        String type = KEYSPACE_PER_TEST + '.' + createType(KEYSPACE_PER_TEST, "CREATE TYPE %s (a int, b text)");
        String mv = KEYSPACE_PER_TEST + ".ks_mv_01";
        superuser.execute("CREATE MATERIALIZED VIEW " + mv + " AS SELECT * FROM " + table + " WHERE val IS NOT NULL AND pk IS NOT NULL AND ck IS NOT NULL PRIMARY KEY (val, pk, ck)");

        Session nonsuperuser = session(USER);

        // ALTER and DROP tables created by somebody else
        // Spin assert for effective auth changes.
        final String spinAssertTable = table;
        Util.spinAssertEquals(false, () -> {
            try
            {
                assertUnauthorizedQuery(nonsuperuser, "User user has no MODIFY permission on <table " + spinAssertTable + "> or any of its parents",
                                        formatQuery(KEYSPACE_PER_TEST, "INSERT INTO %s (pk, ck, val, val_2) VALUES (1, 1, 1, '1')"));
            }
            catch(Throwable e)
            {
                return true;
            }
            return false;
        }, 10);
        assertUnauthorizedQuery(nonsuperuser, "User user has no MODIFY permission on <table " + table + "> or any of its parents",
                                formatQuery(KEYSPACE_PER_TEST, "UPDATE %s SET val = 1 WHERE pk = 1 AND ck = 1"));
        assertUnauthorizedQuery(nonsuperuser, "User user has no MODIFY permission on <table " + table + "> or any of its parents",
                                formatQuery(KEYSPACE_PER_TEST, "DELETE FROM %s WHERE pk = 1 AND ck = 2"));
        assertUnauthorizedQuery(nonsuperuser, "User user has no SELECT permission on <table " + table + "> or any of its parents",
                                formatQuery(KEYSPACE_PER_TEST, "SELECT * FROM %s WHERE pk = 1 AND ck = 1"));
        assertUnauthorizedQuery(nonsuperuser, "User user has no SELECT permission on <table " + table + "> or any of its parents",
                                "SELECT * FROM " + mv + " WHERE val = 1 AND pk = 1 AND ck = 1");
        assertUnauthorizedQuery(nonsuperuser, "User user has no MODIFY permission on <table " + table + "> or any of its parents",
                                formatQuery(KEYSPACE_PER_TEST, "TRUNCATE TABLE %s"));
        assertUnauthorizedQuery(nonsuperuser, "User user has no ALTER permission on <table " + table + "> or any of its parents",
                                formatQuery(KEYSPACE_PER_TEST, "ALTER TABLE %s ADD val_3 int"));
        assertUnauthorizedQuery(nonsuperuser, "User user has no DROP permission on <table " + table + "> or any of its parents",
                                formatQuery(KEYSPACE_PER_TEST, "DROP TABLE %s"));
        assertUnauthorizedQuery(nonsuperuser, "User user has no ALTER permission on <keyspace " + KEYSPACE_PER_TEST + "> or any of its parents",
                                "ALTER TYPE " + type + " ADD c bigint");
        assertUnauthorizedQuery(nonsuperuser, "User user has no DROP permission on <keyspace " + KEYSPACE_PER_TEST + "> or any of its parents",
                                "DROP TYPE " + type);
        assertUnauthorizedQuery(nonsuperuser, "User user has no ALTER permission on <table " + table + "> or any of its parents",
                                "DROP MATERIALIZED VIEW " + mv);
        assertUnauthorizedQuery(nonsuperuser, "User user has no ALTER permission on <table " + table + "> or any of its parents",
                                "DROP INDEX " + index);

        superuser.execute("GRANT ALTER ON KEYSPACE " + KEYSPACE_PER_TEST + " TO " + user);
        superuser.execute("GRANT DROP ON KEYSPACE " + KEYSPACE_PER_TEST + " TO " + user);
        superuser.execute("GRANT SELECT ON KEYSPACE " + KEYSPACE_PER_TEST + " TO " + user);
        superuser.execute("GRANT MODIFY ON KEYSPACE " + KEYSPACE_PER_TEST + " TO " + user);
        // Spin assert for effective auth changes.
        Util.spinAssertEquals(false, () -> {
            try
            {
                nonsuperuser.execute("ALTER KEYSPACE " + KEYSPACE_PER_TEST + " WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}");
            }
            catch(Throwable e)
            {
                return true;
            }
            return false;
        }, 10);

        nonsuperuser.execute(formatQuery(KEYSPACE_PER_TEST, "INSERT INTO %s (pk, ck, val, val_2) VALUES (1, 1, 1, '1')"));
        nonsuperuser.execute(formatQuery(KEYSPACE_PER_TEST, "UPDATE %s SET val = 1 WHERE pk = 1 AND ck = 1"));
        nonsuperuser.execute(formatQuery(KEYSPACE_PER_TEST, "DELETE FROM %s WHERE pk = 1 AND ck = 2"));
        nonsuperuser.execute(formatQuery(KEYSPACE_PER_TEST, "SELECT * FROM %s WHERE pk = 1 AND ck = 1"));
        nonsuperuser.execute("SELECT * FROM " + mv + " WHERE val = 1 AND pk = 1");
        nonsuperuser.execute(formatQuery(KEYSPACE_PER_TEST, "TRUNCATE TABLE %s"));
        nonsuperuser.execute(formatQuery(KEYSPACE_PER_TEST, "ALTER TABLE %s ADD val_3 int"));
        nonsuperuser.execute("DROP MATERIALIZED VIEW " + mv);
        nonsuperuser.execute("DROP INDEX " + index);
        nonsuperuser.execute(formatQuery(KEYSPACE_PER_TEST, "DROP TABLE %s"));
        nonsuperuser.execute("ALTER TYPE " + type + " ADD c bigint");
        nonsuperuser.execute("DROP TYPE " + type);

        // calling creatTableName to create a new table name that will be used by the formatQuery
        table = createTableName();
        type = KEYSPACE_PER_TEST + "." + createTypeName();
        mv = KEYSPACE_PER_TEST + ".ks_mv_02";
        nonsuperuser.execute("CREATE TYPE " + type + " (a int, b text)");
        nonsuperuser.execute(formatQuery(KEYSPACE_PER_TEST, "CREATE TABLE %s (pk int, ck int, val int, val_2 text, PRIMARY KEY (pk, ck))"));
        nonsuperuser.execute("CREATE MATERIALIZED VIEW " + mv + " AS SELECT * FROM " + table + " WHERE val IS NOT NULL AND pk IS NOT NULL AND ck IS NOT NULL PRIMARY KEY (val, pk, ck)");
        nonsuperuser.execute(formatQuery(KEYSPACE_PER_TEST, "INSERT INTO %s (pk, ck, val, val_2) VALUES (1, 1, 1, '1')"));
        nonsuperuser.execute(formatQuery(KEYSPACE_PER_TEST, "UPDATE %s SET val = 1 WHERE pk = 1 AND ck = 1"));
        nonsuperuser.execute(formatQuery(KEYSPACE_PER_TEST, "DELETE FROM %s WHERE pk = 1 AND ck = 2"));
        nonsuperuser.execute(formatQuery(KEYSPACE_PER_TEST, "SELECT * FROM %s WHERE pk = 1 AND ck = 1"));
        nonsuperuser.execute("SELECT * FROM " + mv + " WHERE val = 1 AND pk = 1");
        nonsuperuser.execute(formatQuery(KEYSPACE_PER_TEST, "TRUNCATE TABLE %s"));
        nonsuperuser.execute(formatQuery(KEYSPACE_PER_TEST, "ALTER TABLE %s ADD val_3 int"));
        nonsuperuser.execute("DROP MATERIALIZED VIEW " + mv);
        nonsuperuser.execute(formatQuery(KEYSPACE_PER_TEST, "DROP TABLE %s"));
        nonsuperuser.execute("ALTER TYPE " + type + " ADD c bigint");
        nonsuperuser.execute("DROP TYPE " + type);

        superuser.execute("REVOKE ALTER ON KEYSPACE " + KEYSPACE_PER_TEST + " FROM " + user);
        superuser.execute("REVOKE DROP ON KEYSPACE " + KEYSPACE_PER_TEST + " FROM " + user);
        superuser.execute("REVOKE SELECT ON KEYSPACE " + KEYSPACE_PER_TEST + " FROM " + user);
        superuser.execute("REVOKE MODIFY ON KEYSPACE " + KEYSPACE_PER_TEST + " FROM " + user);

        table = KEYSPACE_PER_TEST + "." + createTable(KEYSPACE_PER_TEST, "CREATE TABLE %s (pk int, ck int, val int, val_2 text, PRIMARY KEY (pk, ck))");
        type = KEYSPACE_PER_TEST + "." + createType(KEYSPACE_PER_TEST, "CREATE TYPE %s (a int, b text)");
        index = KEYSPACE_PER_TEST + ".idx_02";
        createIndex(KEYSPACE_PER_TEST, "CREATE INDEX idx_02 ON %s (val_2)");
        mv = KEYSPACE_PER_TEST + ".ks_mv_03";
        superuser.execute("CREATE MATERIALIZED VIEW " + mv + " AS SELECT * FROM " + table + " WHERE val IS NOT NULL AND pk IS NOT NULL AND ck IS NOT NULL PRIMARY KEY (val, pk, ck)");

        // Spin assert for effective auth changes.
        final String spinAssertTable2 = table;
        Util.spinAssertEquals(false, () -> {
            try
            {
                assertUnauthorizedQuery(nonsuperuser, "User user has no MODIFY permission on <table " + spinAssertTable2 + "> or any of its parents",
                                        "INSERT INTO " + spinAssertTable2 + " (pk, ck, val, val_2) VALUES (1, 1, 1, '1')");
            }
            catch(Throwable e)
            {
                return true;
            }
            return false;
        }, 10);
        assertUnauthorizedQuery(nonsuperuser, "User user has no MODIFY permission on <table " + table + "> or any of its parents",
                                "UPDATE " + table + " SET val = 1 WHERE pk = 1 AND ck = 1");
        assertUnauthorizedQuery(nonsuperuser, "User user has no MODIFY permission on <table " + table + "> or any of its parents",
                                "DELETE FROM " + table + " WHERE pk = 1 AND ck = 2");
        assertUnauthorizedQuery(nonsuperuser, "User user has no SELECT permission on <table " + table + "> or any of its parents",
                                "SELECT * FROM " + table + " WHERE pk = 1 AND ck = 1");
        assertUnauthorizedQuery(nonsuperuser, "User user has no SELECT permission on <table " + table + "> or any of its parents",
                                "SELECT * FROM " + mv + " WHERE val = 1 AND pk = 1 AND ck = 1");
        assertUnauthorizedQuery(nonsuperuser, "User user has no MODIFY permission on <table " + table + "> or any of its parents",
                                "TRUNCATE TABLE " + table);
        assertUnauthorizedQuery(nonsuperuser, "User user has no ALTER permission on <table " + table + "> or any of its parents",
                                formatQuery(KEYSPACE_PER_TEST, "ALTER TABLE " + table + " ADD val_3 int"));
        assertUnauthorizedQuery(nonsuperuser, "User user has no DROP permission on <table " + table + "> or any of its parents",
                                formatQuery(KEYSPACE_PER_TEST, "DROP TABLE " + table));
        assertUnauthorizedQuery(nonsuperuser, "User user has no ALTER permission on <keyspace " + KEYSPACE_PER_TEST + "> or any of its parents",
                                "ALTER TYPE " + type + " ADD c bigint");
        assertUnauthorizedQuery(nonsuperuser, "User user has no DROP permission on <keyspace " + KEYSPACE_PER_TEST + "> or any of its parents",
                                "DROP TYPE " + type);
        assertUnauthorizedQuery(nonsuperuser, "User user has no ALTER permission on <table " + table + "> or any of its parents",
                                "DROP MATERIALIZED VIEW " + mv);
        assertUnauthorizedQuery(nonsuperuser, "User user has no ALTER permission on <table " + table + "> or any of its parents",
                                "DROP INDEX " + index);
    }


    @Test
    public void testSpecificGrantsOnSystemKeyspaces() throws Throwable
    {
        // Granting specific permissions on system keyspaces should not be allowed if those permissions include any from
        // the denylist Permission.INVALID_FOR_SYSTEM_KEYSPACES. By this definition, GRANT ALL on any system keyspace,
        // or a table within one, should be rejected.
        Session superuser = session(SUPERUSER);
        superuser.execute("CREATE ROLE '" + user + "'");
        String responseMsg = "Granting permissions on system keyspaces is strictly limited, this operation is not permitted";
        for (String keyspace : Iterables.concat(LOCAL_SYSTEM_KEYSPACE_NAMES, REPLICATED_SYSTEM_KEYSPACE_NAMES))
        {
            assertUnauthorizedQuery(superuser, responseMsg, format("GRANT ALL PERMISSIONS ON KEYSPACE %s TO %s", keyspace, user));
            DataResource keyspaceResource = DataResource.keyspace(keyspace);
            for (Permission p : keyspaceResource.applicablePermissions())
                maybeRejectGrant(superuser, p, responseMsg, format("GRANT %s ON KEYSPACE %s TO %s", p.name(), keyspace, user));

            for (CFMetaData table : Schema.instance.getKSMetaData(keyspace).tables)
            {
                DataResource tableResource = DataResource.table(keyspace, table.cfName);
                assertUnauthorizedQuery(superuser, responseMsg, format("GRANT ALL PERMISSIONS ON %s.\"%s\" TO %s", table.ksName, table.cfName, user));
                for (Permission p : tableResource.applicablePermissions())
                    maybeRejectGrant(superuser, p, responseMsg, format("GRANT %s ON %s.\"%s\" TO %s", p.name(), table.ksName, table.cfName, user));
            }
        }
    }

    @Test
    public void testGrantOnAllKeyspaces() throws Throwable
    {
        // Granting either specific or ALL permissions on ALL KEYSPACES is allowed, however these permissions are
        // effective for non-system keyspaces only. If for any reason it is necessary to modify permissions on
        // on a system keyspace, it must be done using keyspace specific grant statements.
        Session superuser = session(SUPERUSER);
        superuser.execute(String.format("CREATE ROLE %s WITH LOGIN = TRUE AND password='%s'", user, pass));
        superuser.execute(String.format("ALTER KEYSPACE %s WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}", SchemaConstants.TRACE_KEYSPACE_NAME));
        superuser.execute("CREATE KEYSPACE user_keyspace WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}");
        superuser.execute("CREATE TABLE user_keyspace.t1 (k int PRIMARY KEY)");

        Session nonsuperuser = session(USER);
        assertUnauthorizedQuery(nonsuperuser, "User user has no MODIFY permission on <table user_keyspace.t1> or any of its parents",
                                "INSERT INTO user_keyspace.t1 (k) VALUES (0)");
        assertUnauthorizedQuery(nonsuperuser, "User user has no MODIFY permission on <table system.local> or any of its parents",
                                "INSERT INTO system.local(key) VALUES ('invalid')");

        superuser.execute(format("GRANT MODIFY ON ALL KEYSPACES TO %s", user));
        // User now has write permission on non-system keyspaces only
        nonsuperuser.execute("INSERT INTO user_keyspace.t1 (k) VALUES (0)");
        assertUnauthorizedQuery(nonsuperuser, "User user has no MODIFY permission on <table system.local> or any of its parents",
                                "INSERT INTO system.local(key) VALUES ('invalid')");

        // A non-superuser only has read access to a pre-defined set of system tables and all system_schema/traces
        // tables and granting ALL permissions on ALL keyspaces also does not affect this.
        maybeReadSystemTables(nonsuperuser, false);
        superuser.execute(format("GRANT ALL PERMISSIONS ON ALL KEYSPACES TO %s", user));
        maybeReadSystemTables(nonsuperuser, false);

        // A superuser can still read system tables
        maybeReadSystemTables(superuser, true);
        // and also write to them, though this is still strongly discouraged
        superuser.execute("INSERT INTO system.peers(peer, data_center) VALUES ('127.0.100.100', 'invalid_dc')");

    }

    private void maybeReadSystemTables(Session session, boolean isSuper) throws Throwable
    {
        Set<String> readableKeyspaces = new HashSet<>(Arrays.asList(SchemaConstants.SCHEMA_KEYSPACE_NAME, SchemaConstants.TRACE_KEYSPACE_NAME));
        Set<String> readableSystemTables = new HashSet<>(Arrays.asList(SystemKeyspace.LOCAL,
                                                                       SystemKeyspace.PEERS,
                                                                       SystemKeyspace.SIZE_ESTIMATES));

        for (String keyspace : Iterables.concat(LOCAL_SYSTEM_KEYSPACE_NAMES, REPLICATED_SYSTEM_KEYSPACE_NAMES))
        {
            for (CFMetaData table : Schema.instance.getKSMetaData(keyspace).tables)
            {
                if (isSuper || (readableKeyspaces.contains(keyspace) || (keyspace.equals(SchemaConstants.SYSTEM_KEYSPACE_NAME) && readableSystemTables.contains(table.cfName))))
                {
                    session.execute(format("SELECT * FROM %s.\"%s\" LIMIT 1", table.ksName, table.cfName));
                }
                else
                {
                    assertUnauthorizedQuery(session, format("User %s has no SELECT permission on %s or any of its parents", user, DataResource.table(table.ksName, table.cfName)),
                                            format("SELECT * FROM %s.\"%s\" LIMIT 1", table.ksName, table.cfName));
                }
            }
        }
    }

    private void maybeRejectGrant(Session session, Permission p, String errorResponse, String grant) throws Throwable
    {
        if (Permission.INVALID_FOR_SYSTEM_KEYSPACES.contains(p))
            assertUnauthorizedQuery(session, errorResponse, grant);
        else
            session.execute(grant);
    }

    private void assertUnauthorizedQuery(Session session, String errorMessage, String query) throws Throwable
    {
        try
        {
            session.execute(query);
            Assert.fail("Query should be invalid but no error was thrown. Query is: " + query);
        }
        catch (Exception e)
        {
            if (!UnauthorizedException.class.isAssignableFrom(e.getClass()))
            {
                Assert.fail("Query should be invalid but wrong error was thrown. " +
                            "Expected: " + UnauthorizedException.class.getName() + ", got: " + e.getClass().getName() + ". " +
                            "Query is: " + query);
            }
            if (errorMessage != null)
            {
                assertMessageContains(errorMessage, e);
            }
        }
    }

}
