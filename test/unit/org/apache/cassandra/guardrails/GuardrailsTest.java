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

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.auth.AuthenticatedUser;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.ClientWarn;
import org.apache.cassandra.service.QueryState;

import static java.lang.String.format;
import static org.apache.cassandra.guardrails.Guardrail.DisableFlag;
import static org.apache.cassandra.guardrails.Guardrail.DisallowedValues;
import static org.apache.cassandra.guardrails.Guardrail.Threshold;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class GuardrailsTest extends GuardrailTester
{
    private static QueryState userQueryState, systemQueryState, superQueryState;

    @BeforeClass
    public static void setup()
    {
        systemQueryState = QueryState.forInternalCalls();
        userQueryState = new QueryState(ClientState.forExternalCalls(AuthenticatedUser.ANONYMOUS_USER));
        superQueryState = new QueryState(ClientState.forExternalCalls(new AuthenticatedUser("cassandra")));
    }

    private TriggerCollector createAndAddCollector()
    {
        TriggerCollector collector = new TriggerCollector();
        Guardrails.register(collector);
        return collector;
    }

    private void assertWarn(Runnable runnable, String fullMessage, String redactedMessage)
    {
        // We use client warnings and listeners to check we properly warn as this is the most convenient. Technically,
        // this doesn't validate we also log the warning, but that's probably fine ...
        ClientWarn.instance.captureWarnings();
        TriggerCollector collector = createAndAddCollector();
        try
        {
            runnable.run();

            // Client Warnings
            List<String> warnings = ClientWarn.instance.getWarnings();
            assertFalse("Expected to warn, but no warning was received", warnings == null || warnings.isEmpty());
            assertEquals(format("Got more thant 1 warning (got %d => %s)", warnings.size(), warnings),
                         1,
                         warnings.size());
            String warning = warnings.get(0);
            assertTrue(format("Warning log message '%s' does not contain expected message '%s'", warning, fullMessage),
                       warning.contains(fullMessage));

            // Listeners
            assertTrue("Expected to warn, but failure event(s) triggered: " + collector.failuresTriggered,
                       collector.failuresTriggered.isEmpty());
            assertFalse("Expected to warn, but no warning event triggered",
                        collector.warningsTriggered.isEmpty());
            assertEquals(format("Got more thant 1 warning event (got %s)", collector.warningsTriggered),
                         1,
                         collector.warningsTriggered.size());
            assertTrue(format("Redacted warning message '%s' does not contain expected message '%s'",
                              collector.warningsTriggered.entrySet().iterator().next().getValue(),
                              redactedMessage),
                       collector.warningsTriggered.containsValue(redactedMessage));
        }
        finally
        {
            ClientWarn.instance.resetWarnings();
            Guardrails.unregister(collector);
        }
    }

    private void assertWarn(Runnable runnable, String message)
    {
        assertWarn(runnable, message, message);
    }

    private void assertFails(Runnable runnable, String fullMessage, String redactedMessage)
    {
        assertFails(runnable, fullMessage, redactedMessage, true, true);
    }

    private void assertFails(Runnable runnable, String fullMessage, String redactedMessage, boolean notified, boolean thrown)
    {
        ClientWarn.instance.captureWarnings();
        TriggerCollector collector = createAndAddCollector();
        try
        {
            runnable.run();

            if (thrown)
                fail("Expected to fail, but it did not");
        }
        catch (InvalidRequestException e)
        {
            assertTrue("Expect no exception thrown", thrown);

            assertTrue(format("Full error message '%s' does not contain expected message '%s'", e.getMessage(), fullMessage),
                       e.getMessage().contains(fullMessage));

            // Listeners
            assertTrue("Expected to fail, but warn event(s) triggered: " + collector.warningsTriggered,
                       collector.warningsTriggered.isEmpty());

            if (notified)
            {
                assertFalse("Expected to fail, but no fail event triggered",
                            collector.failuresTriggered.isEmpty());
                assertEquals(format("Got more thant 1 fail event (got %s)", collector.failuresTriggered),
                             1,
                             collector.failuresTriggered.size());
                assertTrue(format("Redacted error message '%s' does not contain expected message '%s'",
                                  collector.failuresTriggered.entrySet().iterator().next().getValue(),
                                  redactedMessage),
                           collector.failuresTriggered.containsValue(redactedMessage));
            }
            else
            {
                assertTrue(format("Expected no fail event triggered, but got %s", collector.failuresTriggered),
                           collector.failuresTriggered.isEmpty());
            }
        }
        finally
        {
            Guardrails.unregister(collector);
        }

        try
        {

            List<String> warnings = ClientWarn.instance.getWarnings();
            if (warnings == null) // will always be the case in practice currently, but being defensive if this change
                warnings = Collections.emptyList();

            assertTrue(format("Expect no warning messages but got %s", warnings), warnings.isEmpty());
        }
        finally
        {
            ClientWarn.instance.resetWarnings();
        }
    }

    private void assertFails(Runnable runnable, String message)
    {
        assertFails(runnable, message, message);
    }

    private void assertNoWarnOrFails(Runnable runnable)
    {
        ClientWarn.instance.captureWarnings();
        TriggerCollector collector = createAndAddCollector();

        try
        {
            runnable.run();
            List<String> warnings = ClientWarn.instance.getWarnings();
            if (warnings == null) // will always be the case in practice currently, but being defensive if this change
                warnings = Collections.emptyList();
            assertTrue("Expected to not warning, but got the following warnings: " + warnings,
                       warnings.isEmpty());

            assertTrue("Expected to not warning, but got the following warnings: " + collector.warningsTriggered,
                       collector.warningsTriggered.isEmpty());

            assertTrue("Expected to not failure, but got the following failure: " + collector.warningsTriggered,
                       collector.failuresTriggered.isEmpty());
        }
        catch (InvalidRequestException e)
        {
            fail("Expected not to fail, but failed with error message: " + e.getMessage());
        }
        finally
        {
            ClientWarn.instance.resetWarnings();
            Guardrails.unregister(collector);
        }
    }

    @Test
    public void testDisabledThreshold()
    {
        Threshold.ErrorMessageProvider errorMessageProvider = (isWarn, what, v, t) -> "Should never trigger";
        testDisabledThreshold(new Threshold("e", () -> -1, () -> -1, errorMessageProvider));
    }

    private void testDisabledThreshold(Threshold guard)
    {
        assertFalse(guard.enabled(userQueryState));

        assertFalse(guard.triggersOn(1, userQueryState));
        assertFalse(guard.triggersOn(10, userQueryState));
        assertFalse(guard.triggersOn(11, userQueryState));
        assertFalse(guard.triggersOn(50, userQueryState));
        assertFalse(guard.triggersOn(110, userQueryState));

        for (boolean containsUserData : Arrays.asList(true, false))
        {
            assertNoWarnOrFails(() -> guard.guard(5, "Z", containsUserData, null));
            assertNoWarnOrFails(() -> guard.guard(25, "A", containsUserData, userQueryState));
            assertNoWarnOrFails(() -> guard.guard(100, "B", containsUserData, userQueryState));
            assertNoWarnOrFails(() -> guard.guard(101, "X", containsUserData, userQueryState));
            assertNoWarnOrFails(() -> guard.guard(200, "Y", containsUserData, userQueryState));
        }
    }

    @Test
    public void testThreshold()
    {
        Threshold guard = new Threshold("x",
                                        () -> 10,
                                        () -> 100,
                                        (isWarn, what, v, t) -> format("%s: for %s, %s > %s",
                                                                       isWarn ? "Warning" : "Failure", what, v, t));

        assertTrue(guard.enabled(userQueryState));

        assertFalse(guard.triggersOn(1, userQueryState));
        assertFalse(guard.triggersOn(10, userQueryState));
        assertTrue(guard.triggersOn(11, userQueryState));
        assertTrue(guard.triggersOn(50, userQueryState));
        assertTrue(guard.triggersOn(110, userQueryState));

        assertNoWarnOrFails(() -> guard.guard(5, "Z", false, userQueryState));
        assertNoWarnOrFails(() -> guard.guard(5, "Z", true, userQueryState));

        assertWarn(() -> guard.guard(25, "A", false, userQueryState), "Warning: for A, 25 > 10");
        assertWarn(() -> guard.guard(25, "A", true, userQueryState),
                   "Warning: for A, 25 > 10",
                   "Warning: for <redacted>, 25 > 10");

        assertWarn(() -> guard.guard(100, "B", false, userQueryState), "Warning: for B, 100 > 10");
        assertWarn(() -> guard.guard(100, "B", true, userQueryState),
                   "Warning: for B, 100 > 10",
                   "Warning: for <redacted>, 100 > 10");

        assertFails(() -> guard.guard(101, "X", false, userQueryState), "Failure: for X, 101 > 100");
        assertFails(() -> guard.guard(101, "X", true, userQueryState),
                    "Failure: for X, 101 > 100",
                    "Failure: for <redacted>, 101 > 100");

        assertFails(() -> guard.guard(200, "Y", false, userQueryState), "Failure: for Y, 200 > 100");
        assertFails(() -> guard.guard(200, "Y", true, userQueryState),
                    "Failure: for Y, 200 > 100",
                    "Failure: for <redacted>, 200 > 100");

        assertNoWarnOrFails(() -> guard.guard(5, "Z", false, userQueryState));
        assertNoWarnOrFails(() -> guard.guard(5, "Z", true, userQueryState));
    }

    @Test
    public void testWarnOnlyThreshold()
    {
        Threshold guard = new Threshold("x",
                                        () -> 10,
                                        () -> -1L,
                                        (isWarn, what, v, t) -> format("%s: for %s, %s > %s",
                                                                       isWarn ? "Warning" : "Failure", what, v, t));

        assertTrue(guard.enabled(userQueryState));

        assertFalse(guard.triggersOn(10, userQueryState));
        assertTrue(guard.triggersOn(11, userQueryState));

        assertNoWarnOrFails(() -> guard.guard(5, "Z", false, userQueryState));
        assertNoWarnOrFails(() -> guard.guard(5, "Z", true, userQueryState));

        assertWarn(() -> guard.guard(11, "A", false, userQueryState), "Warning: for A, 11 > 10");
        assertWarn(() -> guard.guard(11, "A", true, userQueryState),
                   "Warning: for A, 11 > 10",
                   "Warning: for <redacted>, 11 > 10");
    }

    @Test
    public void testFailureOnlyThreshold()
    {
        Threshold guard = new Threshold("x",
                                        () -> -1L,
                                        () -> 10,
                                        (isWarn, what, v, t) -> format("%s: for %s, %s > %s",
                                                                       isWarn ? "Warning" : "Failure", what, v, t));

        assertTrue(guard.enabled(userQueryState));

        assertFalse(guard.triggersOn(10, userQueryState));
        assertTrue(guard.triggersOn(11, userQueryState));

        assertNoWarnOrFails(() -> guard.guard(5, "Z", false, userQueryState));
        assertNoWarnOrFails(() -> guard.guard(5, "Z", true, userQueryState));
        assertFails(() -> guard.guard(11, "A", false, userQueryState), "Failure: for A, 11 > 10");
        assertFails(() -> guard.guard(11, "A", true, userQueryState),
                    "Failure: for A, 11 > 10",
                    "Failure: for <redacted>, 11 > 10");
    }

    @Test
    public void testNotThrowOnFailure()
    {
        Threshold guard = new Threshold("x",
                                        () -> 5L,
                                        () -> 10,
                                        (isWarn, what, v, t) -> format("%s: for %s, %s > %s",
                                                                       isWarn ? "Warning" : "Failure", what, v, t));
        guard.noExceptionOnFailure();

        assertTrue(guard.triggersOn(11, userQueryState));
        assertFails(() -> guard.guard(11, "A", true, userQueryState),
                    "Failure: for A, 11 > 10", "Failure: for <redacted>, 11 > 10", true, false);
    }

    @Test
    public void testMinLogInterval()
    {
        Threshold guard = new Threshold("x",
                                        () -> 5,
                                        () -> 10,
                                        (isWarn, what, v, t) -> format("%s: for %s, %s > %s",
                                                                       isWarn ? "Warning" : "Failure", what, v, t));

        guard.minNotifyIntervalInMs(TimeUnit.MINUTES.toMillis(30));

        // should trigger on first warn and error
        assertWarn(() -> guard.guard(6, "A", true, userQueryState), "Warning: for A, 6 > 5", "Warning: for <redacted>, 6 > 5");
        assertFails(() -> guard.guard(11, "B", true, userQueryState),
                    "Failure: for B, 11 > 10", "Failure: for <redacted>, 11 > 10", true, true);

        // should not trigger on second warn and error within minimum notify interval
        assertNoWarnOrFails(() -> guard.guard(6, "A", true, userQueryState));
        assertFails(() -> guard.guard(11, "B", true, userQueryState),
                    "Failure: for B, 11 > 10", "Failure: for <redacted>, 11 > 10", false, true);
    }

    @Test
    public void testThresholdUsers()
    {
        Threshold guard = new Threshold("x",
                                        () -> 10,
                                        () -> 100,
                                        (isWarn, what, v, t) -> format("%s: for %s, %s > %s",
                                                                       isWarn ? "Warning" : "Failure", what, v, t));

        // value under both thresholds
        assertNoWarnOrFails(() -> guard.guard(5, "x", true, null));
        assertNoWarnOrFails(() -> guard.guard(5, "x", true, userQueryState));
        assertNoWarnOrFails(() -> guard.guard(5, "x", true, systemQueryState));
        assertNoWarnOrFails(() -> guard.guard(5, "x", true, superQueryState));

        // value over warning threshold
        assertWarn(() -> guard.guard(100, "y", true, null),
                   "Warning: for y, 100 > 10", "Warning: for <redacted>, 100 > 10");
        assertWarn(() -> guard.guard(100, "y", true, userQueryState),
                   "Warning: for y, 100 > 10", "Warning: for <redacted>, 100 > 10");
        assertNoWarnOrFails(() -> guard.guard(100, "y", true, systemQueryState));
        assertNoWarnOrFails(() -> guard.guard(100, "y", true, superQueryState));

        // value over failure threshold
        assertFails(() -> guard.guard(101, "z", true, null),
                    "Failure: for z, 101 > 100", "Failure: for <redacted>, 101 > 100");
        assertFails(() -> guard.guard(101, "z", true, userQueryState),
                    "Failure: for z, 101 > 100", "Failure: for <redacted>, 101 > 100");
        assertNoWarnOrFails(() -> guard.guard(101, "z", true, systemQueryState));
        assertNoWarnOrFails(() -> guard.guard(101, "z", true, superQueryState));
    }

    @Test
    public void testDisableFlag()
    {
        assertFails(() -> new DisableFlag("x", () -> true, "X").ensureEnabled(userQueryState), "X is not allowed");
        assertNoWarnOrFails(() -> new DisableFlag("x", () -> false, "X").ensureEnabled(userQueryState));

        assertFails(() -> new DisableFlag("x", () -> true, "X").ensureEnabled("Y", userQueryState), "Y is not allowed");
        assertNoWarnOrFails(() -> new DisableFlag("x", () -> false, "X").ensureEnabled("Y", userQueryState));
    }

    @Test
    public void testDisableFlagUsers()
    {
        DisableFlag enabled = new DisableFlag("x", () -> false, "X");
        assertNoWarnOrFails(() -> enabled.ensureEnabled(null));
        assertNoWarnOrFails(() -> enabled.ensureEnabled(userQueryState));
        assertNoWarnOrFails(() -> enabled.ensureEnabled(systemQueryState));
        assertNoWarnOrFails(() -> enabled.ensureEnabled(superQueryState));

        DisableFlag disabled = new DisableFlag("x", () -> true, "X");
        assertFails(() -> disabled.ensureEnabled(null), "X is not allowed");
        assertFails(() -> disabled.ensureEnabled(userQueryState), "X is not allowed");
        assertNoWarnOrFails(() -> disabled.ensureEnabled(systemQueryState));
        assertNoWarnOrFails(() -> disabled.ensureEnabled(superQueryState));
    }

    @Test
    public void testDisallowedValues()
    {
        // Using a LinkedHashSet below to ensure the order in the error message checked below are not random
        DisallowedValues<Integer> disallowed = new DisallowedValues<>(
        "x",
        () -> new LinkedHashSet<>(Arrays.asList("4", "6", "20")),
        Integer::valueOf,
        "integer");

        assertNoWarnOrFails(() -> disallowed.ensureAllowed(3, userQueryState));
        assertFails(() -> disallowed.ensureAllowed(4, userQueryState),
                    "Provided value 4 is not allowed for integer (disallowed values are: [4, 6, 20])");
        assertNoWarnOrFails(() -> disallowed.ensureAllowed(10, userQueryState));
        assertFails(() -> disallowed.ensureAllowed(20, userQueryState),
                    "Provided value 20 is not allowed for integer (disallowed values are: [4, 6, 20])");
        assertNoWarnOrFails(() -> disallowed.ensureAllowed(200, userQueryState));
        assertNoWarnOrFails(() -> disallowed.ensureAllowed(set(1, 2, 3), userQueryState));

        assertFails(() -> disallowed.ensureAllowed(set(4, 6), null),
                    "Provided values [4, 6] are not allowed for integer (disallowed values are: [4, 6, 20])");
        assertFails(() -> disallowed.ensureAllowed(set(4, 5, 6, 7), null),
                    "Provided values [4, 6] are not allowed for integer (disallowed values are: [4, 6, 20])");
    }

    @Test
    public void testDisallowedValuesUsers()
    {
        DisallowedValues<Integer> disallowed = new DisallowedValues<>(
        "x",
        () -> Collections.singleton("2"),
        Integer::valueOf,
        "integer");

        assertNoWarnOrFails(() -> disallowed.ensureAllowed(1, null));
        assertNoWarnOrFails(() -> disallowed.ensureAllowed(1, userQueryState));
        assertNoWarnOrFails(() -> disallowed.ensureAllowed(1, systemQueryState));
        assertNoWarnOrFails(() -> disallowed.ensureAllowed(1, superQueryState));

        String message = "Provided value 2 is not allowed for integer (disallowed values are: [2])";
        assertFails(() -> disallowed.ensureAllowed(2, null), message);
        assertFails(() -> disallowed.ensureAllowed(2, userQueryState), message);
        assertNoWarnOrFails(() -> disallowed.ensureAllowed(2, systemQueryState));
        assertNoWarnOrFails(() -> disallowed.ensureAllowed(2, superQueryState));

        Set<Integer> allowedValues = set(1);
        assertNoWarnOrFails(() -> disallowed.ensureAllowed(allowedValues, null));
        assertNoWarnOrFails(() -> disallowed.ensureAllowed(allowedValues, userQueryState));
        assertNoWarnOrFails(() -> disallowed.ensureAllowed(allowedValues, systemQueryState));
        assertNoWarnOrFails(() -> disallowed.ensureAllowed(allowedValues, superQueryState));

        Set<Integer> disallowedValues = set(2);
        message = "Provided values [2] are not allowed for integer (disallowed values are: [2])";
        assertFails(() -> disallowed.ensureAllowed(disallowedValues, null), message);
        assertFails(() -> disallowed.ensureAllowed(disallowedValues, userQueryState), message);
        assertNoWarnOrFails(() -> disallowed.ensureAllowed(disallowedValues, systemQueryState));
        assertNoWarnOrFails(() -> disallowed.ensureAllowed(disallowedValues, superQueryState));
    }

    private Set<Integer> set(Integer... values)
    {
        return new HashSet<>(Arrays.asList(values));
    }

    private static class TriggerCollector implements Guardrails.Listener
    {
        final Map<String, String> warningsTriggered = new HashMap<>();
        final Map<String, String> failuresTriggered = new HashMap<>();

        @Override
        public void onWarningTriggered(String guardrailName, String message)
        {
            warningsTriggered.put(guardrailName, message);
        }

        @Override
        public void onFailureTriggered(String guardrailName, String message)
        {
            failuresTriggered.put(guardrailName, message);
        }
    }
}