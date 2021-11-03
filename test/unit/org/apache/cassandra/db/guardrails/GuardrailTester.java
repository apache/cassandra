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

package org.apache.cassandra.db.guardrails;

import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.List;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.junit.Before;
import org.junit.BeforeClass;

import com.datastax.driver.core.exceptions.InvalidQueryException;
import org.apache.cassandra.auth.AuthenticatedUser;
import org.apache.cassandra.auth.CassandraRoleManager;
import org.apache.cassandra.cql3.CQLStatement;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.db.view.View;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.index.sasi.SASIIndex;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.ClientWarn;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.transport.messages.ResultMessage;
import org.assertj.core.api.Assertions;

import static java.lang.String.format;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public abstract class GuardrailTester extends CQLTester
{
    // Name used when testing CREATE TABLE that should be aborted (we need to provide it as assertAborts, which
    // is used to assert the failure, does not know that it is a CREATE TABLE and would thus reuse the name of the
    // previously created table, which is not what we want).
    protected static final String ABORT_TABLE = "abort_table_creation_test";

    private static final String USERNAME = "guardrail_user";
    private static final String PASSWORD = "guardrail_password";

    protected static ClientState systemClientState, userClientState, superClientState;

    @BeforeClass
    public static void setUpClass()
    {
        CQLTester.setUpClass();
        requireAuthentication();
        requireNetwork();
        guardrails().setEnabled(true);

        systemClientState = ClientState.forInternalCalls();
        userClientState = ClientState.forExternalCalls(InetSocketAddress.createUnresolved("127.0.0.1", 123));
        superClientState = ClientState.forExternalCalls(InetSocketAddress.createUnresolved("127.0.0.1", 321));
        superClientState.login(new AuthenticatedUser(CassandraRoleManager.DEFAULT_SUPERUSER_NAME));
    }

    /**
     * Creates an ordinary user that is not excluded from guardrails, that is, a user that is not super not internal.
     */
    @Before
    public void beforeGuardrailTest() throws Throwable
    {
        useSuperUser();
        executeNet(format("CREATE USER IF NOT EXISTS %s WITH PASSWORD '%s'", USERNAME, PASSWORD));
        executeNet(format("GRANT ALL ON KEYSPACE %s TO %s", KEYSPACE, USERNAME));
        useUser(USERNAME, PASSWORD);

        String useKeyspaceQuery = "USE " + keyspace();
        execute(userClientState, useKeyspaceQuery);
        execute(systemClientState, useKeyspaceQuery);
        execute(superClientState, useKeyspaceQuery);
    }

    static Guardrails guardrails()
    {
        return Guardrails.instance;
    }

    protected <T> void assertValidProperty(BiConsumer<Guardrails, T> setter, T value)
    {
        setter.accept(guardrails(), value);
    }

    protected <T> void assertInvalidProperty(BiConsumer<Guardrails, T> setter,
                                             T value,
                                             String message,
                                             Object... messageArgs)
    {
        Assertions.assertThatThrownBy(() -> setter.accept(guardrails(), value))
                  .isInstanceOf(IllegalArgumentException.class)
                  .hasMessage(format(message, messageArgs));
    }

    @SafeVarargs
    protected final void testExcludedUsers(Supplier<String>... queries) throws Throwable
    {
        execute("USE " + keyspace());
        assertSuperuserIsExcluded(queries);
        assertInternalQueriesAreExcluded(queries);
    }

    @SafeVarargs
    private final void assertInternalQueriesAreExcluded(Supplier<String>... queries) throws Throwable
    {
        for (Supplier<String> query : queries)
        {
            assertValid(() -> execute(systemClientState, query.get()));
        }
    }

    @SafeVarargs
    private final void assertSuperuserIsExcluded(Supplier<String>... queries) throws Throwable
    {
        for (Supplier<String> query : queries)
        {
            assertValid(() -> execute(superClientState, query.get()));
        }
    }

    protected void assertValid(CheckedFunction function) throws Throwable
    {
        ClientWarn.instance.captureWarnings();
        try
        {
            function.apply();
            assertEmptyWarnings();
        }
        catch (InvalidRequestException e)
        {
            fail("Expected not to fail, but failed with error message: " + e.getMessage());
        }
        finally
        {
            ClientWarn.instance.resetWarnings();
        }
    }

    protected void assertValid(String query) throws Throwable
    {
        assertValid(() -> execute(userClientState, query));
    }

    protected void assertWarns(CheckedFunction function, String message) throws Throwable
    {
        // We use client warnings to check we properly warn as this is the most convenient. Technically,
        // this doesn't validate we also log the warning, but that's probably fine ...
        ClientWarn.instance.captureWarnings();
        try
        {
            function.apply();
            assertWarnings(message);
        }
        finally
        {
            ClientWarn.instance.resetWarnings();
        }
    }

    protected void assertWarns(String message, String query) throws Throwable
    {
        assertWarns(() -> execute(userClientState, query), message);
    }

    protected void assertAborts(CheckedFunction function, String message) throws Throwable
    {
        assertAborts(function, message, true);
    }

    protected void assertAborts(CheckedFunction function, String message, boolean thrown) throws Throwable
    {
        ClientWarn.instance.captureWarnings();
        try
        {
            function.apply();

            if (thrown)
                fail("Expected to fail, but it did not");
        }
        catch (InvalidRequestException | InvalidQueryException e)
        {
            assertTrue("Expect no exception thrown", thrown);

            assertTrue(format("Full error message '%s' does not contain expected message '%s'", e.getMessage(), message),
                       e.getMessage().contains(message));

            assertWarnings(message);
        }
        finally
        {
            ClientWarn.instance.resetWarnings();
        }
    }

    protected void assertAborts(String message, String query) throws Throwable
    {
        assertAborts(() -> execute(userClientState, query), message);
    }

    private void assertWarnings(String message)
    {
        List<String> warnings = getWarnings();

        assertFalse("Expected to warn, but no warning was received", warnings == null || warnings.isEmpty());
        assertEquals(format("Got more thant 1 warning (got %d => %s)", warnings.size(), warnings),
                     1,
                     warnings.size());

        String warning = warnings.get(0);
        assertTrue(format("Warning log message '%s' does not contain expected message '%s'", warning, message),
                   warning.contains(message));
    }

    private void assertEmptyWarnings()
    {
        List<String> warnings = getWarnings();

        if (warnings == null) // will always be the case in practice currently, but being defensive if this change
            warnings = Collections.emptyList();

        assertTrue(format("Expect no warning messages but got %s", warnings), warnings.isEmpty());
    }

    private List<String> getWarnings()
    {
        List<String> warnings = ClientWarn.instance.getWarnings();

        return warnings == null
               ? Collections.emptyList()
               : warnings.stream()
                         .filter(w -> !w.equals(View.USAGE_WARNING) && !w.equals(SASIIndex.USAGE_WARNING))
                         .collect(Collectors.toList());
    }

    protected void assertConfigFails(Consumer<Guardrails> consumer, String message)
    {
        try
        {
            consumer.accept(guardrails());
            fail("Expected failure");
        }
        catch (IllegalArgumentException e)
        {
            String actualMessage = e.getMessage();
            assertTrue(String.format("Failure message '%s' does not contain expected message '%s'", actualMessage, message),
                       actualMessage.contains(message));
        }
    }

    protected ResultMessage execute(ClientState state, String query)
    {
        QueryState queryState = new QueryState(state);

        String formattedQuery = formatQuery(query);
        CQLStatement statement = QueryProcessor.parseStatement(formattedQuery, queryState.getClientState());
        statement.validate(state);

        QueryOptions options = QueryOptions.forInternalCalls(Collections.emptyList());

        return statement.executeLocally(queryState, options);
    }
}
