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

import java.io.Serializable;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

import com.google.common.collect.ImmutableSet;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;

import org.apache.cassandra.auth.AuthenticatedUser;
import org.apache.cassandra.auth.CassandraRoleManager;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLStatement;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.guardrails.GuardrailEvent.GuardrailEventType;
import org.apache.cassandra.db.view.View;
import org.apache.cassandra.diag.DiagnosticEventService;
import org.apache.cassandra.index.sasi.SASIIndex;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.ClientWarn;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.transport.messages.ResultMessage;
import org.apache.cassandra.utils.Clock;
import org.assertj.core.api.Assertions;

import static java.lang.String.format;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * This class provides specific utility methods for testing Guardrails that should be used instead of the provided
 * {@link CQLTester} methods. Many of the methods in CQLTester don't respect the {@link ClientState} provided for a query
 * and instead use {@link ClientState#forInternalCalls()} which flags as an internal query and thus bypasses auth and
 * guardrail checks.
 *
 * Some GuardrailTester methods and their usage is as follows:
 *      {@link GuardrailTester#assertValid(String)} to confirm the query as structured is valid given the state of the db
 *      {@link GuardrailTester#assertWarns(String, String)} to confirm a query succeeds but warns the text provided
 *      {@link GuardrailTester#assertFails(String, String)} to confirm a query fails with the message provided
 *      {@link GuardrailTester#testExcludedUsers} to confirm superusers are excluded from application of the guardrail
 */
public abstract class GuardrailTester extends CQLTester
{
    // Name used when testing CREATE TABLE that should be aborted (we need to provide it as assertFails, which
    // is used to assert the failure, does not know that it is a CREATE TABLE and would thus reuse the name of the
    // previously created table, which is not what we want).
    protected static final String FAIL_TABLE = "abort_table_creation_test";

    private static final String USERNAME = "guardrail_user";
    private static final String PASSWORD = "guardrail_password";

    protected static ClientState systemClientState, userClientState, superClientState;

    /** The tested guardrail, if we are testing a specific one. */
    @Nullable
    protected final Guardrail guardrail;

    /** A listener for emitted diagnostic events. */
    protected final Listener listener;

    public GuardrailTester()
    {
        this(null);
    }

    public GuardrailTester(@Nullable Guardrail guardrail)
    {
        this.guardrail = guardrail;
        this.listener = new Listener();
    }

    @BeforeClass
    public static void setUpClass()
    {
        CQLTester.setUpClass();
        requireAuthentication();
        requireNetwork();
        DatabaseDescriptor.setDiagnosticEventsEnabled(true);

        systemClientState = ClientState.forInternalCalls();

        userClientState = ClientState.forExternalCalls(InetSocketAddress.createUnresolved("127.0.0.1", 123));
        AuthenticatedUser user = new AuthenticatedUser(USERNAME)
        {
            @Override
            public boolean canLogin() { return true; }
        };
        userClientState.login(user);

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

        DiagnosticEventService.instance().subscribe(GuardrailEvent.class, listener);
    }

    @After
    public void afterGuardrailTest() throws Throwable
    {
        DiagnosticEventService.instance().unsubscribe(listener);
    }

    static Guardrails guardrails()
    {
        return Guardrails.instance;
    }

    protected <T> void assertValidProperty(BiConsumer<Guardrails, T> setter, T value)
    {
        setter.accept(guardrails(), value);
    }

    protected <T> void assertValidProperty(BiConsumer<Guardrails, T> setter, Function<Guardrails, T> getter, T value)
    {
        setter.accept(guardrails(), value);
        assertEquals(value, getter.apply(guardrails()));
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
            listener.assertNotWarned();
            listener.assertNotFailed();
        }
        catch (GuardrailViolatedException e)
        {
            fail("Expected not to fail, but failed with error message: " + e.getMessage());
        }
        finally
        {
            ClientWarn.instance.resetWarnings();
            listener.clear();
        }
    }

    protected void assertValid(String query) throws Throwable
    {
        assertValid(() -> execute(userClientState, query));
    }

    protected void assertWarns(String query, String message) throws Throwable
    {
        assertWarns(query, message, message);
    }

    protected void assertWarns(String query, String message, String redactedMessage) throws Throwable
    {
        assertWarns(query, Collections.singletonList(message), Collections.singletonList(redactedMessage));
    }

    protected void assertWarns(String query, List<String> messages) throws Throwable
    {
        assertWarns(() -> execute(userClientState, query), messages, messages);
    }

    protected void assertWarns(String query, List<String> messages, List<String> redactedMessages) throws Throwable
    {
        assertWarns(() -> execute(userClientState, query), messages, redactedMessages);
    }

    protected void assertWarns(CheckedFunction function, String message) throws Throwable
    {
        assertWarns(function, message, message);
    }

    protected void assertWarns(CheckedFunction function, String message, String redactedMessage) throws Throwable
    {
        assertWarns(function, Collections.singletonList(message), Collections.singletonList(redactedMessage));
    }

    protected void assertWarns(CheckedFunction function, List<String> messages) throws Throwable
    {
        assertWarns(function, messages, messages);
    }

    protected void assertWarns(CheckedFunction function, List<String> messages, List<String> redactedMessages) throws Throwable
    {
        // We use client warnings to check we properly warn as this is the most convenient. Technically,
        // this doesn't validate we also log the warning, but that's probably fine ...
        ClientWarn.instance.captureWarnings();
        try
        {
            function.apply();
            assertWarnings(messages);
            listener.assertWarned(redactedMessages);
            listener.assertNotFailed();
        }
        finally
        {
            ClientWarn.instance.resetWarnings();
            listener.clear();
        }
    }

    protected void assertFails(String query, String message) throws Throwable
    {
        assertFails(query, message, message);
    }

    protected void assertFails(String query, String message, String redactedMessage) throws Throwable
    {
        assertFails(query, Collections.singletonList(message), Collections.singletonList(redactedMessage));
    }

    protected void assertFails(String query, List<String> messages) throws Throwable
    {
        assertFails(query, messages, messages);
    }

    protected void assertFails(String query, List<String> messages, List<String> redactedMessages) throws Throwable
    {
        assertFails(() -> execute(userClientState, query), messages, redactedMessages);
    }

    protected void assertFails(CheckedFunction function, String message) throws Throwable
    {
        assertFails(function, message, message);
    }

    protected void assertFails(CheckedFunction function, String message, String redactedMessage) throws Throwable
    {
        assertFails(function, true, message, redactedMessage);
    }

    protected void assertFails(CheckedFunction function, boolean thrown, String message) throws Throwable
    {
        assertFails(function, thrown, message, message);
    }

    protected void assertFails(CheckedFunction function, boolean thrown, String message, String redactedMessage) throws Throwable
    {
        assertFails(function, thrown, Collections.singletonList(message), Collections.singletonList(redactedMessage));
    }

    protected void assertFails(CheckedFunction function, List<String> messages) throws Throwable
    {
        assertFails(function, messages, messages);
    }

    protected void assertFails(CheckedFunction function, List<String> messages, List<String> redactedMessages) throws Throwable
    {
        assertFails(function, true, messages, redactedMessages);
    }

    /**
     * Unlike {@link CQLTester#assertInvalidThrowMessage}, the chain of methods ending here in {@link GuardrailTester}
     * respect the input ClientState so guardrails permissions will be correctly checked.
     */
    protected void assertFails(CheckedFunction function, boolean thrown, List<String> messages, List<String> redactedMessages) throws Throwable
    {
        ClientWarn.instance.captureWarnings();
        try
        {
            function.apply();

            if (thrown)
                fail("Expected to fail, but it did not");
        }
        catch (GuardrailViolatedException e)
        {
            assertTrue("Expect no exception thrown", thrown);

            // the last message is the one raising the guardrail failure, the previous messages are warnings
            String failMessage = messages.get(messages.size() - 1);

            if (guardrail != null)
            {
                String message = e.getMessage();
                String prefix = guardrail.decorateMessage("").replace(". " + guardrail.reason, "");
                assertTrue(format("Full error message '%s' doesn't start with the prefix '%s'", message, prefix),
                           message.startsWith(prefix));

                String reason = guardrail.reason;
                if (reason != null)
                {
                    assertTrue(format("Full error message '%s' doesn't end with the reason '%s'", message, reason),
                               message.endsWith(reason));
                }
            }

            assertTrue(format("Full error message '%s' does not contain expected message '%s'", e.getMessage(), failMessage),
                       e.getMessage().contains(failMessage));

            assertWarnings(messages);
            if (messages.size() > 1)
                listener.assertWarned(redactedMessages.subList(0, messages.size() - 1));
            else
                listener.assertNotWarned();
            listener.assertFailed(redactedMessages.get(messages.size() - 1));
        }
        finally
        {
            ClientWarn.instance.resetWarnings();
            listener.clear();
        }
    }

    protected void assertFails(String query, String... messages) throws Throwable
    {
        assertFails(() -> execute(userClientState, query), Arrays.asList(messages));
    }

    protected void assertThrows(CheckedFunction function, Class<? extends Throwable> exception, String message)
    {
        try
        {
            function.apply();
            fail("Expected to fail, but it did not");
        }
        catch (Throwable e)
        {
            if (!exception.isAssignableFrom(e.getClass()))
                Assert.fail(format("Expected to fail with %s but got %s", exception.getName(), e.getClass().getName()));

            assertTrue(format("Error message '%s' does not contain expected message '%s'", e.getMessage(), message),
                       e.getMessage().contains(message));
        }
    }

    private void assertWarnings(List<String> messages)
    {
        List<String> warnings = getWarnings();

        assertFalse("Expected to warn, but no warning was received", warnings == null || warnings.isEmpty());
        assertEquals(format("Expected %d warnings but got %d: %s", messages.size(), warnings.size(), warnings),
                     messages.size(),
                     warnings.size());

        for (int i = 0; i < messages.size(); i++)
        {
            String message = messages.get(i);
            String warning = warnings.get(i);
            if (guardrail != null)
            {
                String prefix = guardrail.decorateMessage("").replace(". " + guardrail.reason, "");
                assertTrue(format("Warning log message '%s' doesn't start with the prefix '%s'", warning, prefix),
                           warning.startsWith(prefix));

                String reason = guardrail.reason;
                if (reason != null)
                {
                    assertTrue(format("Warning log message '%s' doesn't end with the reason '%s'", warning, reason),
                               warning.endsWith(reason));
                }
            }

            assertTrue(format("Warning log message '%s' does not contain expected message '%s'", warning, message),
                       warning.contains(message));
        }
    }

    private void assertEmptyWarnings()
    {
        List<String> warnings = getWarnings();

        if (warnings == null) // will always be the case in practice currently, but being defensive if this change
            warnings = Collections.emptyList();

        assertTrue(format("Expect no warning messages but got %s", warnings), warnings.isEmpty());
    }

    protected List<String> getWarnings()
    {
        List<String> warnings = ClientWarn.instance.getWarnings();

        return warnings == null
               ? Collections.emptyList()
               : warnings.stream()
                         .filter(w -> !w.equals(View.USAGE_WARNING) && !w.equals(SASIIndex.USAGE_WARNING))
                         .collect(Collectors.toList());
    }

    protected void assertConfigValid(Consumer<Guardrails> consumer)
    {
        consumer.accept(guardrails());
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
        return execute(state, query, Collections.emptyList());
    }

    protected ResultMessage execute(ClientState state, String query, List<ByteBuffer> values)
    {
        QueryOptions options = QueryOptions.forInternalCalls(values);

        return execute(state, query, options);
    }

    protected ResultMessage execute(ClientState state, String query, ConsistencyLevel cl)
    {
        return execute(state, query, cl, null);
    }

    protected ResultMessage execute(ClientState state, String query, ConsistencyLevel cl, ConsistencyLevel serialCl)
    {
        QueryOptions options = QueryOptions.create(cl,
                                                   Collections.emptyList(),
                                                   false,
                                                   10,
                                                   null,
                                                   serialCl,
                                                   ProtocolVersion.CURRENT,
                                                   KEYSPACE);

        return execute(state, query, options);
    }

    /**
     * Performs execution of query using the input {@link ClientState} (i.e. unlike {@link ClientState#forInternalCalls()}
     * which may not) to ensure guardrails are approprieately applied to the query provided.
     */
    protected ResultMessage execute(ClientState state, String query, QueryOptions options)
    {
        QueryState queryState = new QueryState(state);

        String formattedQuery = formatQuery(query);
        CQLStatement statement = QueryProcessor.parseStatement(formattedQuery, queryState.getClientState());
        statement.validate(state);

        return statement.execute(queryState, options, Clock.Global.nanoTime());
    }

    protected static String sortCSV(String csv)
    {
        return String.join(",", (new TreeSet<>(ImmutableSet.copyOf((csv.split(","))))));
    }

    /**
     * A listener for guardrails diagnostic events.
     */
    public class Listener implements Consumer<GuardrailEvent>
    {
        private final List<String> warnings = new CopyOnWriteArrayList<>();
        private final List<String> failures = new CopyOnWriteArrayList<>();

        @Override
        public void accept(GuardrailEvent event)
        {
            assertNotNull(event);
            Map<String, Serializable> map = event.toMap();

            if (guardrail != null)
                assertEquals(guardrail.name, map.get("name"));

            GuardrailEventType type = (GuardrailEventType) event.getType();
            String message = map.toString();

            switch (type)
            {
                case WARNED:
                    warnings.add(message);
                    break;
                case FAILED:
                    failures.add(message);
                    break;
                default:
                    fail("Unexpected diagnostic event:" + type);
            }
        }

        public void clear()
        {
            warnings.clear();
            failures.clear();
        }

        public void assertNotWarned()
        {
            assertTrue(format("Expect no warning diagnostic events but got %s", warnings), warnings.isEmpty());
        }

        public void assertWarned(String message)
        {
            assertWarned(Collections.singletonList(message));
        }

        public void assertWarned(List<String> messages)
        {
            assertFalse("Expected to emit warning diagnostic event, but no warning was emitted", warnings.isEmpty());
            assertEquals(format("Expected %d warning diagnostic events but got %d: %s)", messages.size(), warnings.size(), warnings),
                         messages.size(), warnings.size());

            for (int i = 0; i < messages.size(); i++)
            {
                String message = messages.get(i);
                String warning = warnings.get(i);
                assertTrue(format("Warning diagnostic event '%s' does not contain expected message '%s'", warning, message),
                           warning.contains(message));
            }
        }

        public void assertNotFailed()
        {
            assertTrue(format("Expect no failure diagnostic events but got %s", failures), failures.isEmpty());
        }

        public void assertFailed(String... messages)
        {
            assertFalse("Expected to emit failure diagnostic event, but no failure was emitted", failures.isEmpty());
            assertEquals(format("Expected %d failure diagnostic events but got %d: %s)", messages.length, failures.size(), failures),
                         messages.length, failures.size());

            for (int i = 0; i < messages.length; i++)
            {
                String message = messages[i];
                String failure = failures.get(i);
                assertTrue(format("Failure diagnostic event '%s' does not contain expected message '%s'", failure, message),
                           failure.contains(message));
            }
        }
    }
}
