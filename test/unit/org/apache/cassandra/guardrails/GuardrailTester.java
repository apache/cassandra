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

package org.apache.cassandra.guardrails;

import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.BiConsumer;
import javax.annotation.Nullable;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

import com.datastax.driver.core.exceptions.InvalidQueryException;
import org.apache.cassandra.auth.AuthenticatedUser;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.service.StorageService;

import static java.lang.String.format;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public abstract class GuardrailTester extends CQLTester
{
    static final String USERNAME = "guardrail_user";
    static final String PASSWORD = "guardrail_password";

    private static boolean guardRailsEnabled;
    private static Set<String> tablePropertiesDisallowed;

    protected TestListener listener;

    @BeforeClass
    public static void setupGuardrailTester()
    {
        guardRailsEnabled = DatabaseDescriptor.getGuardrailsConfig().enabled;
        DatabaseDescriptor.getGuardrailsConfig().enabled = true;

        tablePropertiesDisallowed = DatabaseDescriptor.getGuardrailsConfig().table_properties_disallowed;
        DatabaseDescriptor.getGuardrailsConfig().table_properties_disallowed = Collections.emptySet();

        requireAuthentication();
        requireNetwork();
        StorageService.instance.doAuthSetup(true);
    }

    @AfterClass
    public static void tearDownGuardrailTester()
    {
        DatabaseDescriptor.getGuardrailsConfig().enabled = guardRailsEnabled;
        DatabaseDescriptor.getGuardrailsConfig().table_properties_disallowed = tablePropertiesDisallowed;
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

        listener = new TestListener(null);
        Guardrails.register(listener);

        execute("USE " + keyspace());
        executeNet("USE " + keyspace());
    }

    @After
    public void afterGuardrailTest() throws Throwable
    {
        Guardrails.unregister(listener);

        useSuperUser();
        executeNet("DROP USER " + USERNAME);
    }

    QueryState userQueryState()
    {
        return queryState(new AuthenticatedUser(USERNAME));
    }

    QueryState superQueryState()
    {
        return queryState(new AuthenticatedUser("cassandra"));
    }

    QueryState internalQueryState()
    {
        return QueryState.forInternalCalls();
    }

    private QueryState queryState(AuthenticatedUser user)
    {
        ClientState clientState = ClientState.forExternalCalls(user);
        return new QueryState(clientState);
    }

    static GuardrailsConfig config()
    {
        return DatabaseDescriptor.getGuardrailsConfig();
    }

    private void assertValidProperty(BiConsumer<GuardrailsConfig, Long> setter, long value)
    {
        setter.accept(config(), value);
        config().validate();
    }

    private void assertInvalidPositiveProperty(BiConsumer<GuardrailsConfig, Long> setter,
                                               long value,
                                               long maxValue,
                                               boolean allowZero,
                                               String name)
    {
        try
        {
            assertValidProperty(setter, value);
            fail(format("Expected configuration exception for guardrail %s value: %d", name, value));
        }
        catch (ConfigurationException e)
        {
            String expectedMessage = null;

            if (value > maxValue)
                expectedMessage = format("Invalid value %d for guardrail %s: maximum allowed value is %d",
                                         value, name, maxValue);
            if (value == 0 && !allowZero)
                expectedMessage = format("Invalid value for guardrail %s: 0 is not allowed", name);

            if (value < -1L)
                expectedMessage = format("Invalid value %d for guardrail %s: negative values are not "
                                         + "allowed, outside of -1 which disables the guardrail",
                                         value, name);

            assertEquals(format("Exception message '%s' does not contain '%s'", e.getMessage(), expectedMessage),
                         expectedMessage, e.getMessage());
        }
    }

    private void assertInvalidStrictlyPositiveProperty(BiConsumer<GuardrailsConfig, Long> setter, long value, String name)
    {
        assertInvalidPositiveProperty(setter, value, Integer.MAX_VALUE, false, name);
    }

    void testValidationOfStrictlyPositiveProperty(BiConsumer<GuardrailsConfig, Long> setter, String name)
    {
        assertInvalidStrictlyPositiveProperty(setter, Integer.MIN_VALUE, name);
        assertInvalidStrictlyPositiveProperty(setter, -2, name);
        assertValidProperty(setter, -1); // disabled
        assertInvalidStrictlyPositiveProperty(setter, 0, name);
        assertValidProperty(setter, 1);
        assertValidProperty(setter, 2);
        assertValidProperty(setter, Integer.MAX_VALUE);
    }

    protected void testExcludedUsers(String... queries) throws Throwable
    {
        assertSuperuserIsExcluded(queries);
        assertInternalQueriesAreExcluded(queries);
    }

    protected void assertInternalQueriesAreExcluded(String... queries) throws Throwable
    {
        for (String query : queries)
        {
            listener.clear();
            try
            {
                execute(query);
                listener.assertNotWarned();
                listener.assertNotFailed();
            }
            finally
            {
                listener.clear();
            }
        }
    }

    protected void assertSuperuserIsExcluded(String... queries) throws Throwable
    {
        useSuperUser();
        executeNet("USE " + keyspace());

        for (String query : queries)
            assertValid(query);

        useUser(USERNAME, PASSWORD);
        executeNet("USE " + keyspace());
    }

    protected void assertValid(CheckedFunction function) throws Throwable
    {
        try
        {
            function.apply();
            listener.assertNotWarned();
            listener.assertNotFailed();
        }
        finally
        {
            listener.clear();
        }
    }

    protected void assertValid(String query, Object... args) throws Throwable
    {
        assertValid(() -> executeNet(query, args));
    }

    protected void assertWarns(CheckedFunction function, String... messages) throws Throwable
    {
        listener.clear();
        try
        {
            function.apply();
            listener.assertWarned(messages);
            listener.assertNotFailed();
        }
        finally
        {
            listener.clear();
        }
    }

    void assertWarns(List<String> messages, String query, Object... args) throws Throwable
    {
        assertWarns(() -> executeNet(query, args), messages.toArray(new String[0]));
    }

    protected void assertWarns(String message, String query, Object... args) throws Throwable
    {
        assertWarns(() -> executeNet(query, args), message);
    }

    protected void assertFails(CheckedFunction function, String... messages) throws Throwable
    {
        listener.clear();
        try
        {
            function.apply();
            fail("Expected failure");
        }
        catch (InvalidQueryException e)
        {
            listener.assertFailed(messages);
            listener.assertNotWarned();
        }
        finally
        {
            listener.clear();
        }
    }

    protected void assertFails(String message, String query, Object... args) throws Throwable
    {
        assertFails(() -> executeNet(query, args), message);
    }

    protected void assertConfigFails(Runnable runnable, String message)
    {
        try
        {
            runnable.run();
            fail("Expected failure");
        }
        catch (ConfigurationException e)
        {
            String actualMessage = e.getMessage();
            assertTrue(String.format("Failure message '%s' does not contain expected message '%s'", actualMessage, message),
                       actualMessage.contains(message));
        }
    }

    static class TestListener implements Guardrails.Listener
    {
        @Nullable
        private final Guardrail guardrail;
        private List<String> failures = new CopyOnWriteArrayList<>();
        private List<String> warnings = new CopyOnWriteArrayList<>();

        private TestListener(@Nullable Guardrail guardrail)
        {
            this.guardrail = guardrail;
        }

        synchronized void assertFailed(String... expectedMessages)
        {
            assertFalse("Expected to fail, but no failures were received", failures == null || failures.isEmpty());
            assertEquals(format("Expected %d failures, but found %d messages: %s)",
                                expectedMessages.length, failures.size(), failures),
                         expectedMessages.length, failures.size());

            for (int i = 0; i < failures.size(); i++)
            {
                String actual = failures.get(i);
                String expected = expectedMessages[i];
                assertTrue(format("Failure message '%s' does not contain expected message '%s'", actual, expected),
                           actual.contains(expected));
            }
        }

        synchronized void assertNotFailed()
        {
            assertTrue(format("No failures expected, but found %d: %s", failures.size(), failures), failures.isEmpty());
        }

        synchronized void assertWarned(String... expectedMessages)
        {
            assertFalse("Expected to warn, but no warnings were received", warnings == null || warnings.isEmpty());
            assertEquals(format("Expected %d warnings, but found %d messages: %s)",
                                expectedMessages.length, warnings.size(), warnings),
                         expectedMessages.length, warnings.size());

            for (int i = 0; i < warnings.size(); i++)
            {
                String actual = warnings.get(i);
                String expected = expectedMessages[i];
                assertTrue(format("Warning message '%s' does not contain expected message '%s'", actual, expected),
                           actual.contains(expected));
            }
        }

        synchronized void assertContainsWarns(String... expectedMessages)
        {
            assertFalse(warnings.isEmpty());
            for (String msg : expectedMessages)
            {
                assertTrue(String.format("Warning messages '%s' don't contain the expected '%s'", warnings, msg),
                           warnings.stream().anyMatch(m -> m.contains(msg)));
            }
        }

        synchronized void assertNotWarned()
        {
            assertTrue(format("No warnings expected, but found %d: %s", warnings.size(), warnings), warnings.isEmpty());
        }

        synchronized void clear()
        {
            failures.clear();
            warnings.clear();
        }

        @Override
        public synchronized void onWarningTriggered(String guardrailName, String message)
        {
            if (guardrail == null || guardrailName.equals(guardrail.name))
            {
                warnings.add(message);
            }
        }

        @Override
        public void onFailureTriggered(String guardrailName, String message)
        {
            if (guardrail == null || guardrailName.equals(guardrail.name))
            {
                failures.add(message);
            }
        }
    }
}
