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

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;

import org.assertj.core.api.Assertions;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.cql3.Attributes;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.VariableSpecifications;
import org.apache.cassandra.cql3.statements.BatchStatement;
import org.apache.cassandra.cql3.statements.ModificationStatement;
import org.apache.cassandra.exceptions.UnauthorizedException;
import org.apache.cassandra.service.ClientState;

import static org.apache.cassandra.auth.AuthTestUtils.auth;

public class BatchAuthTest extends CQLTester
{
    private String table1;
    private String table2;

    @BeforeClass
    public static void setUpAuth()
    {
        IRoleManager roleManager = new AuthTestUtils.LocalCassandraRoleManager();
        SchemaLoader.setupAuth(roleManager,
                               new AuthTestUtils.LocalPasswordAuthenticator(),
                               new AuthTestUtils.LocalCassandraAuthorizer(),
                               new AuthTestUtils.LocalCassandraNetworkAuthorizer(),
                               new AuthTestUtils.LocalCassandraCIDRAuthorizer());
        roleManager.setup();
        AuthCacheService.initializeAndRegisterCaches();
        AuthTestUtils.setupSuperUser();

        requireNetwork();
    }

    @Before
    public void setUpTest()
    {
        table1 = createTable("CREATE TABLE %s (k int PRIMARY KEY, v int)");
        table2 = createTable("CREATE TABLE %s (k int PRIMARY KEY, v int)");
    }

    /**
     * All statements target the same table
     */
    @Test
    public void singleTableBatchRequiresModify()
    {
        ClientState clientState = createUserAndLogin();
        BatchStatement batch = batch(clientState,
                                     insert(table1, 0),
                                     insert(table1, 1),
                                     insert(table1, 2));

        assertUnauthorized(batch, clientState, Permission.MODIFY, table1);

        grant(clientState, Permission.MODIFY, table1);
        batch.authorize(clientState);
    }

    /**
     * Every table in the batch needs its own MODIFY check: state retained for one table must not satisfy the
     * next one.
     */
    @Test
    public void multiTableBatchRequiresModifyOnEveryTable()
    {
        ClientState clientState = createUserAndLogin();
        BatchStatement batch = batch(clientState,
                                     insert(table1, 0),
                                     insert(table2, 0),
                                     insert(table1, 1));

        assertUnauthorized(batch, clientState, Permission.MODIFY, table1);

        // MODIFY on table1 alone must not let the table2 statement through
        grant(clientState, Permission.MODIFY, table1);
        assertUnauthorized(batch, clientState, Permission.MODIFY, table2);

        grant(clientState, Permission.MODIFY, table2);
        batch.authorize(clientState);
    }

    /**
     * The unconditional statement comes first and satisfies MODIFY for the table, but the conditional statement
     * that follows must still require SELECT, since a CAS update can be used to simulate a read.
     */
    @Test
    public void conditionalStatementAfterUnconditionalRequiresSelect()
    {
        ClientState clientState = createUserAndLogin();
        BatchStatement batch = batch(clientState,
                                     insert(table1, 0),
                                     updateIf(table1, 0));

        assertUnauthorized(batch, clientState, Permission.MODIFY, table1);

        grant(clientState, Permission.MODIFY, table1);
        assertUnauthorized(batch, clientState, Permission.SELECT, table1);

        grant(clientState, Permission.SELECT, table1);
        batch.authorize(clientState);
    }

    /**
     * The view lookup is only performed for the first statement of each table, but the permissions it requires -
     * SELECT on the base table, since the view update reads the current state, and MODIFY on every view - must
     * still be enforced for the batch.
     */
    @Test
    public void batchOnViewedTableRequiresModifyOnView()
    {
        // created last so that it is the current table the view is built on
        String base = createTable("CREATE TABLE %s (k int PRIMARY KEY, v int)");
        String view = createView("CREATE MATERIALIZED VIEW %s AS SELECT * FROM %s " +
                                 "WHERE k IS NOT NULL AND v IS NOT NULL PRIMARY KEY (v, k)");

        ClientState clientState = createUserAndLogin();
        BatchStatement batch = batch(clientState,
                                     insert(base, 0),
                                     insert(base, 1));

        assertUnauthorized(batch, clientState, Permission.MODIFY, base);

        grant(clientState, Permission.MODIFY, base);
        assertUnauthorized(batch, clientState, Permission.SELECT, base);

        grant(clientState, Permission.SELECT, base);
        assertUnauthorized(batch, clientState, Permission.MODIFY, view);

        grant(clientState, Permission.MODIFY, view);
        batch.authorize(clientState);
    }

    /**
     * SELECT ensured for one base table must not carry over to the next one, so the state has to be cleared when
     * the batch moves on to a different table.
     */
    @Test
    public void batchOnSeveralViewedTablesRequiresSelectOnEachBase()
    {
        String base1 = createTable("CREATE TABLE %s (k int PRIMARY KEY, v int)");
        String view1 = createView("CREATE MATERIALIZED VIEW %s AS SELECT * FROM %s " +
                                  "WHERE k IS NOT NULL AND v IS NOT NULL PRIMARY KEY (v, k)");
        String base2 = createTable("CREATE TABLE %s (k int PRIMARY KEY, v int)");
        String view2 = createView("CREATE MATERIALIZED VIEW %s AS SELECT * FROM %s " +
                                  "WHERE k IS NOT NULL AND v IS NOT NULL PRIMARY KEY (v, k)");

        ClientState clientState = createUserAndLogin();
        BatchStatement batch = batch(clientState,
                                     insert(base1, 0),
                                     insert(base2, 0));

        grant(clientState, Permission.MODIFY, base1);
        grant(clientState, Permission.MODIFY, base2);
        grant(clientState, Permission.SELECT, base1);
        grant(clientState, Permission.MODIFY, view1);

        // everything base1 needs is granted, but updating base2's view still requires reading base2
        assertUnauthorized(batch, clientState, Permission.SELECT, base2);

        grant(clientState, Permission.SELECT, base2);
        assertUnauthorized(batch, clientState, Permission.MODIFY, view2);

        grant(clientState, Permission.MODIFY, view2);
        batch.authorize(clientState);
    }

    private BatchStatement batch(ClientState clientState, String... queries)
    {
        // prepared one by one, as they are when a batch arrives over the native protocol
        List<ModificationStatement> statements = new ArrayList<>(queries.length);
        for (String query : queries)
            statements.add((ModificationStatement) QueryProcessor.getStatement(query, clientState));

        return new BatchStatement(BatchStatement.Type.LOGGED, VariableSpecifications.empty(), statements, Attributes.none());
    }

    private static String insert(String table, int k)
    {
        return String.format("INSERT INTO %s.%s (k, v) VALUES (%d, 0)", KEYSPACE, table, k);
    }

    private static String updateIf(String table, int k)
    {
        return String.format("UPDATE %s.%s SET v = 1 WHERE k = %d IF v = 0", KEYSPACE, table, k);
    }

    private void grant(ClientState clientState, Permission permission, String table)
    {
        AuthTestUtils.authorize("GRANT %s ON TABLE %s.%s TO %s",
                                permission, KEYSPACE, table, clientState.getUser().getName());
    }

    private void assertUnauthorized(BatchStatement batch, ClientState clientState, Permission permission, String table)
    {
        Assertions.assertThatThrownBy(() -> batch.authorize(clientState))
                  .isInstanceOf(UnauthorizedException.class)
                  .hasMessage("User %s has no %s permission on <table %s.%s> or any of its parents",
                              clientState.getUser().getName(), permission, KEYSPACE, table);
    }

    private ClientState createUserAndLogin()
    {
        String username = AuthTestUtils.createName();
        auth("CREATE ROLE %s WITH password = 'password' AND LOGIN = true", username);
        ClientState clientState = ClientState.forExternalCalls(InetSocketAddress.createUnresolved("127.0.0.1", 123));
        clientState.login(new AuthenticatedUser(username));
        return clientState;
    }
}
