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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Consumer;

import org.junit.Assert;
import org.junit.Test;

import static java.lang.String.format;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class GuardrailsTest extends GuardrailTester
{
    public static final int DISABLED = -1;
    public static final String REASON = "Testing";
    public static final String FEATURE = "Feature name";

    private void testDisabledThreshold(Threshold guard) throws Throwable
    {
        assertFalse(guard.enabled(userClientState));

        for (boolean containsUserData : Arrays.asList(true, false))
        {
            assertValid(() -> guard.guard(5, "Z", containsUserData, null));
            assertValid(() -> guard.guard(25, "A", containsUserData, userClientState));
            assertValid(() -> guard.guard(100, "B", containsUserData, userClientState));
            assertValid(() -> guard.guard(101, "X", containsUserData, userClientState));
            assertValid(() -> guard.guard(200, "Y", containsUserData, userClientState));
        }
    }

    @Test
    public void testDisabledMaxThreshold() throws Throwable
    {
        Threshold.ErrorMessageProvider errorMessageProvider = (isWarn, what, v, t) -> "Should never trigger";
        testDisabledThreshold(new MaxThreshold("x", REASON, state -> DISABLED, state -> DISABLED, errorMessageProvider));
    }

    @Test
    public void testMaxThreshold() throws Throwable
    {
        MaxThreshold guard = new MaxThreshold("x",
                                              REASON,
                                              state -> 10,
                                              state -> 100,
                                              (isWarn, featureName, v, t) -> format("%s: for %s, %s > %s",
                                                                             isWarn ? "Warning" : "Aborting", featureName, v, t));

        assertTrue(guard.enabled(userClientState));

        assertValid(() -> guard.guard(5, "Z", false, userClientState));
        assertWarns(() -> guard.guard(25, "A", false, userClientState), "Warning: for A, 25 > 10");
        assertWarns(() -> guard.guard(100, "B", false, userClientState), "Warning: for B, 100 > 10");
        assertFails(() -> guard.guard(101, "X", false, userClientState), "Aborting: for X, 101 > 100");
        assertFails(() -> guard.guard(200, "Y", false, userClientState), "Aborting: for Y, 200 > 100");
        assertValid(() -> guard.guard(5, "Z", false, userClientState));

        assertValid(() -> guard.guard(5, "Z", true, userClientState));
        assertWarns(() -> guard.guard(25, "A", true, userClientState), "Warning: for A, 25 > 10", "Warning: for <redacted>, 25 > 10");
        assertWarns(() -> guard.guard(100, "B", true, userClientState), "Warning: for B, 100 > 10", "Warning: for <redacted>, 100 > 10");
        assertFails(() -> guard.guard(101, "X", true, userClientState), "Aborting: for X, 101 > 100", "Aborting: for <redacted>, 101 > 100");
        assertFails(() -> guard.guard(200, "Y", true, userClientState), "Aborting: for Y, 200 > 100", "Aborting: for <redacted>, 200 > 100");
        assertValid(() -> guard.guard(5, "Z", true, userClientState));
    }

    @Test
    public void testWarnOnlyMaxThreshold() throws Throwable
    {
        MaxThreshold guard = new MaxThreshold("x",
                                              REASON,
                                              state -> 10,
                                              state -> DISABLED,
                                              (isWarn, featureName, v, t) -> format("%s: for %s, %s > %s",
                                                                             isWarn ? "Warning" : "Aborting", featureName, v, t));

        assertTrue(guard.enabled(userClientState));

        assertValid(() -> guard.guard(5, "Z", false, userClientState));
        assertWarns(() -> guard.guard(11, "A", false, userClientState), "Warning: for A, 11 > 10");

        assertValid(() -> guard.guard(5, "Z", true, userClientState));
        assertWarns(() -> guard.guard(11, "A", true, userClientState), "Warning: for A, 11 > 10", "Warning: for <redacted>, 11 > 10");
    }

    @Test
    public void testFailOnlyMaxThreshold() throws Throwable
    {
        MaxThreshold guard = new MaxThreshold("x",
                                              REASON,
                                              state -> DISABLED,
                                              state -> 10,
                                              (isWarn, featureName, v, t) -> format("%s: for %s, %s > %s",
                                                                             isWarn ? "Warning" : "Aborting", featureName, v, t));

        assertTrue(guard.enabled(userClientState));

        assertValid(() -> guard.guard(5, "Z", false, userClientState));
        assertFails(() -> guard.guard(11, "A", false, userClientState), "Aborting: for A, 11 > 10");

        assertValid(() -> guard.guard(5, "Z", true, userClientState));
        assertFails(() -> guard.guard(11, "A", true, userClientState), "Aborting: for A, 11 > 10", "Aborting: for <redacted>, 11 > 10");
    }

    @Test
    public void testMaxThresholdUsers() throws Throwable
    {
        MaxThreshold guard = new MaxThreshold("x",
                                              REASON,
                                              state -> 10,
                                              state -> 100,
                                              (isWarn, featureName, v, t) -> format("%s: for %s, %s > %s",
                                                                             isWarn ? "Warning" : "Failure", featureName, v, t));

        // value under both thresholds
        assertValid(() -> guard.guard(5, "x", false, null));
        assertValid(() -> guard.guard(5, "x", false, userClientState));
        assertValid(() -> guard.guard(5, "x", false, systemClientState));
        assertValid(() -> guard.guard(5, "x", false, superClientState));

        // value over warning threshold
        assertWarns(() -> guard.guard(100, "y", false, null), "Warning: for y, 100 > 10");
        assertWarns(() -> guard.guard(100, "y", false, userClientState), "Warning: for y, 100 > 10");
        assertValid(() -> guard.guard(100, "y", false, systemClientState));
        assertValid(() -> guard.guard(100, "y", false, superClientState));

        // value over fail threshold. An undefined user means that the check comes from a background process, so we
        // still emit failure messages and events, but we don't throw an exception to prevent interrupting that process.
        assertFails(() -> guard.guard(101, "z", false, null), false, "Failure: for z, 101 > 100");
        assertFails(() -> guard.guard(101, "z", false, userClientState), "Failure: for z, 101 > 100");
        assertValid(() -> guard.guard(101, "z", false, systemClientState));
        assertValid(() -> guard.guard(101, "z", false, superClientState));
    }

    @Test
    public void testDisabledMinThreshold() throws Throwable
    {
        Threshold.ErrorMessageProvider errorMessageProvider = (isWarn, what, v, t) -> "Should never trigger";
        testDisabledThreshold(new MinThreshold("x", REASON, state -> DISABLED, state -> DISABLED, errorMessageProvider));
    }

    @Test
    public void testMinThreshold() throws Throwable
    {
        MinThreshold guard = new MinThreshold("x",
                                              REASON,
                                              state -> 100,
                                              state -> 10,
                                              (isWarn, what, v, t) -> format("%s: for %s, %s < %s",
                                                                             isWarn ? "Warning" : "Aborting", what, v, t));

        assertTrue(guard.enabled(userClientState));

        assertValid(() -> guard.guard(200, "Z", false, userClientState));
        assertWarns(() -> guard.guard(25, "A", false, userClientState), "Warning: for A, 25 < 100");
        assertWarns(() -> guard.guard(10, "B", false, userClientState), "Warning: for B, 10 < 100");
        assertFails(() -> guard.guard(9, "X", false, userClientState), "Aborting: for X, 9 < 10");
        assertFails(() -> guard.guard(1, "Y", false, userClientState), "Aborting: for Y, 1 < 10");
        assertValid(() -> guard.guard(200, "Z", false, userClientState));

        assertValid(() -> guard.guard(200, "Z", true, userClientState));
        assertWarns(() -> guard.guard(25, "A", true, userClientState), "Warning: for A, 25 < 100", "Warning: for <redacted>, 25 < 100");
        assertWarns(() -> guard.guard(10, "B", true, userClientState), "Warning: for B, 10 < 100", "Warning: for <redacted>, 10 < 100");
        assertFails(() -> guard.guard(9, "X", true, userClientState), "Aborting: for X, 9 < 10", "Aborting: for <redacted>, 9 < 10");
        assertFails(() -> guard.guard(1, "Y", true, userClientState), "Aborting: for Y, 1 < 10", "Aborting: for <redacted>, 1 < 10");
        assertValid(() -> guard.guard(200, "Z", true, userClientState));
    }

    @Test
    public void testWarnOnlyMinThreshold() throws Throwable
    {
        MinThreshold guard = new MinThreshold("x",
                                              REASON,
                                              state -> 10,
                                              state -> DISABLED,
                                              (isWarn, what, v, t) -> format("%s: for %s, %s < %s",
                                                                             isWarn ? "Warning" : "Aborting", what, v, t));

        assertTrue(guard.enabled(userClientState));

        assertValid(() -> guard.guard(11, "Z", false, userClientState));
        assertWarns(() -> guard.guard(5, "A", false, userClientState), "Warning: for A, 5 < 10");

        assertValid(() -> guard.guard(11, "Z", true, userClientState));
        assertWarns(() -> guard.guard(5, "A", true, userClientState), "Warning: for A, 5 < 10", "Warning: for <redacted>, 5 < 10");
    }

    @Test
    public void testFailOnlyMinThreshold() throws Throwable
    {
        MinThreshold guard = new MinThreshold("x",
                                              REASON,
                                              state -> DISABLED,
                                              state -> 10,
                                              (isWarn, what, v, t) -> format("%s: for %s, %s < %s",
                                                                             isWarn ? "Warning" : "Aborting", what, v, t));

        assertTrue(guard.enabled(userClientState));

        assertValid(() -> guard.guard(11, "Z", false, userClientState));
        assertFails(() -> guard.guard(5, "A", false, userClientState), "Aborting: for A, 5 < 10");

        assertValid(() -> guard.guard(11, "Z", true, userClientState));
        assertFails(() -> guard.guard(5, "A", true, userClientState), "Aborting: for A, 5 < 10", "Aborting: for <redacted>, 5 < 10");
    }

    @Test
    public void testMinThresholdUsers() throws Throwable
    {
        MinThreshold guard = new MinThreshold("x",
                                              REASON,
                                              state -> 100,
                                              state -> 10,
                                              (isWarn, what, v, t) -> format("%s: for %s, %s < %s",
                                                                             isWarn ? "Warning" : "Failure", what, v, t));

        // value above both thresholds
        assertValid(() -> guard.guard(200, "x", false, null));
        assertValid(() -> guard.guard(200, "x", false, userClientState));
        assertValid(() -> guard.guard(200, "x", false, systemClientState));
        assertValid(() -> guard.guard(200, "x", false, superClientState));

        // value under warning threshold
        assertWarns(() -> guard.guard(10, "y", false, null), "Warning: for y, 10 < 100");
        assertWarns(() -> guard.guard(10, "y", false, userClientState), "Warning: for y, 10 < 100");
        assertValid(() -> guard.guard(10, "y", false, systemClientState));
        assertValid(() -> guard.guard(10, "y", false, superClientState));

        // value under fail threshold. An undefined user means that the check comes from a background process, so we
        // still emit failure messages and events, but we don't throw an exception to prevent interrupting that process.
        assertFails(() -> guard.guard(9, "z", false, null), false, "Failure: for z, 9 < 10");
        assertFails(() -> guard.guard(9, "z", false, userClientState), "Failure: for z, 9 < 10");
        assertValid(() -> guard.guard(9, "z", false, systemClientState));
        assertValid(() -> guard.guard(9, "z", false, superClientState));
    }

    @Test
    public void testEnableFlag() throws Throwable
    {
        assertFails(() -> new EnableFlag("x", REASON, state -> false, "X").ensureEnabled(userClientState), "X is not allowed");
        assertValid(() -> new EnableFlag("x", REASON, state -> true, "X").ensureEnabled(userClientState));

        assertFails(() -> new EnableFlag("x", REASON, state -> false, "X").ensureEnabled("Y", userClientState), "Y is not allowed");
        assertValid(() -> new EnableFlag("x", REASON, state -> true, "X").ensureEnabled("Y", userClientState));
    }

    @Test
    public void testEnableFlagUsers() throws Throwable
    {
        EnableFlag enabled = new EnableFlag("x", REASON, state -> true, "X");
        assertValid(() -> enabled.ensureEnabled(null));
        assertValid(() -> enabled.ensureEnabled(userClientState));
        assertValid(() -> enabled.ensureEnabled(systemClientState));
        assertValid(() -> enabled.ensureEnabled(superClientState));

        EnableFlag disabled = new EnableFlag("x", REASON, state -> false, "X");
        assertFails(() -> disabled.ensureEnabled(userClientState), "X is not allowed");
        assertValid(() -> disabled.ensureEnabled(systemClientState));
        assertValid(() -> disabled.ensureEnabled(superClientState));
    }

    @Test
    public void testEnableFlagWarn() throws Throwable
    {
        EnableFlag disabledGuard = new EnableFlag("x", null, state -> true, state -> false, FEATURE);

        assertFails(() -> disabledGuard.ensureEnabled(null), false, FEATURE + " is not allowed");
        assertFails(() -> disabledGuard.ensureEnabled(userClientState), FEATURE + " is not allowed");
        assertValid(() -> disabledGuard.ensureEnabled(systemClientState));
        assertValid(() -> disabledGuard.ensureEnabled(superClientState));

        EnableFlag enabledGuard = new EnableFlag("x", null, state -> true, state -> true, FEATURE);
        assertWarns(() -> enabledGuard.ensureEnabled(null), FEATURE + " is not recommended");
        assertWarns(() -> enabledGuard.ensureEnabled(userClientState), FEATURE + " is not recommended");
        assertValid(() -> enabledGuard.ensureEnabled(systemClientState));
        assertValid(() -> enabledGuard.ensureEnabled(superClientState));
    }

    @Test
    public void testValuesWarned() throws Throwable
    {
        // Using a sorted set below to ensure the order in the warning message checked below is not random
        Values<Integer> warned = new Values<>("x",
                                              REASON,
                                              state -> insertionOrderedSet(4, 6, 20),
                                              state -> Collections.emptySet(),
                                              state -> Collections.emptySet(),
                                              "integer");

        Consumer<Integer> action = i -> Assert.fail("The ignore action shouldn't have been triggered");
        assertValid(() -> warned.guard(set(3), action, userClientState));
        assertWarns(() -> warned.guard(set(4), action, userClientState),
                    "Provided values [4] are not recommended for integer (warned values are: [4, 6, 20])");
        assertWarns(() -> warned.guard(set(4, 6), action, null),
                    "Provided values [4, 6] are not recommended for integer (warned values are: [4, 6, 20])");
        assertWarns(() -> warned.guard(set(4, 5, 6, 7), action, null),
                    "Provided values [4, 6] are not recommended for integer (warned values are: [4, 6, 20])");
    }

    @Test
    public void testValuesIgnored() throws Throwable
    {
        // Using a sorted set below to ensure the order in the error message checked below are not random
        Values<Integer> ignored = new Values<>("x",
                                               REASON,
                                               state -> Collections.emptySet(),
                                               state -> insertionOrderedSet(4, 6, 20),
                                               state -> Collections.emptySet(),
                                               "integer");

        Set<Integer> triggeredOn = set();
        assertValid(() -> ignored.guard(set(3), triggeredOn::add, userClientState));
        assertEquals(set(), triggeredOn);

        assertWarns(() -> ignored.guard(set(4), triggeredOn::add, userClientState),
                    "Ignoring provided values [4] as they are not supported for integer (ignored values are: [4, 6, 20])");
        assertEquals(set(4), triggeredOn);
        triggeredOn.clear();

        assertWarns(() -> ignored.guard(set(4, 6), triggeredOn::add, null),
                    "Ignoring provided values [4, 6] as they are not supported for integer (ignored values are: [4, 6, 20])");
        assertEquals(set(4, 6), triggeredOn);
        triggeredOn.clear();

        assertWarns(() -> ignored.guard(set(4, 5, 6, 7), triggeredOn::add, null),
                    "Ignoring provided values [4, 6] as they are not supported for integer (ignored values are: [4, 6, 20])");
        assertEquals(set(4, 6), triggeredOn);
        triggeredOn.clear();

        assertThrows(() -> ignored.guard(set(4), userClientState),
                     AssertionError.class,
                     "There isn't an ignore action for integer, but value 4 is setup to be ignored");
    }

    @Test
    public void testValuesDisallowed() throws Throwable
    {
        // Using a sorted set below to ensure the order in the error message checked below are not random
        Values<Integer> disallowed = new Values<>("x",
                                                  REASON,
                                                  state -> Collections.emptySet(),
                                                  state -> Collections.emptySet(),
                                                  state -> insertionOrderedSet(4, 6, 20),
                                                  "integer");

        Consumer<Integer> action = i -> Assert.fail("The ignore action shouldn't have been triggered");
        assertValid(() -> disallowed.guard(set(3), action, userClientState));
        assertFails(() -> disallowed.guard(set(4), action, userClientState),
                    "Provided values [4] are not allowed for integer (disallowed values are: [4, 6, 20])");
        assertValid(() -> disallowed.guard(set(10), action, userClientState));
        assertFails(() -> disallowed.guard(set(20), action, userClientState),
                    "Provided values [20] are not allowed for integer (disallowed values are: [4, 6, 20])");
        assertValid(() -> disallowed.guard(set(200), action, userClientState));
        assertValid(() -> disallowed.guard(set(1, 2, 3), action, userClientState));

        assertFails(() -> disallowed.guard(set(4, 6), action, null), false,
                    "Provided values [4, 6] are not allowed for integer (disallowed values are: [4, 6, 20])");
        assertFails(() -> disallowed.guard(set(4, 5, 6, 7), action, null), false,
                    "Provided values [4, 6] are not allowed for integer (disallowed values are: [4, 6, 20])");
    }

    @Test
    public void testValuesUsers() throws Throwable
    {
        Values<Integer> disallowed = new Values<>("x",
                                                  REASON,
                                                  state -> Collections.singleton(2),
                                                  state -> Collections.singleton(3),
                                                  state -> Collections.singleton(4),
                                                  "integer");

        Consumer<Integer> action = i -> Assert.fail("The ignore action shouldn't have been triggered");

        assertValid(() -> disallowed.guard(set(1), action, null));
        assertValid(() -> disallowed.guard(set(1), action, userClientState));
        assertValid(() -> disallowed.guard(set(1), action, systemClientState));
        assertValid(() -> disallowed.guard(set(1), action, superClientState));

        String message = "Provided values [2] are not recommended for integer (warned values are: [2])";
        assertWarns(() -> disallowed.guard(set(2), action, null), message);
        assertWarns(() -> disallowed.guard(set(2), action, userClientState), message);
        assertValid(() -> disallowed.guard(set(2), action, systemClientState));
        assertValid(() -> disallowed.guard(set(2), action, superClientState));

        message = "Ignoring provided values [3] as they are not supported for integer (ignored values are: [3])";
        List<Integer> triggeredOn = new ArrayList<>();
        assertWarns(() -> disallowed.guard(set(3), triggeredOn::add, null), message);
        assertWarns(() -> disallowed.guard(set(3), triggeredOn::add, userClientState), message);
        assertValid(() -> disallowed.guard(set(3), triggeredOn::add, systemClientState));
        assertValid(() -> disallowed.guard(set(3), triggeredOn::add, superClientState));
        Assert.assertEquals(list(3, 3), triggeredOn);

        message = "Provided values [4] are not allowed for integer (disallowed values are: [4])";
        assertFails(() -> disallowed.guard(set(4), action, null), false, message);
        assertFails(() -> disallowed.guard(set(4), action, userClientState), message);
        assertValid(() -> disallowed.guard(set(4), action, systemClientState));
        assertValid(() -> disallowed.guard(set(4), action, superClientState));
    }

    @Test
    public void testPredicates() throws Throwable
    {
        Predicates<Integer> guard = new Predicates<>("x",
                                                     REASON,
                                                     state -> x -> x > 10,
                                                     state -> x -> x > 100,
                                                     (isWarn, value) -> format("%s: %s", isWarn ? "Warning" : "Aborting", value));

        assertValid(() -> guard.guard(5, userClientState));
        assertWarns(() -> guard.guard(25, userClientState), "Warning: 25");
        assertWarns(() -> guard.guard(100,  userClientState), "Warning: 100");
        assertFails(() -> guard.guard(101,  userClientState), "Aborting: 101");
        assertFails(() -> guard.guard(200,  userClientState), "Aborting: 200");
        assertValid(() -> guard.guard(5,  userClientState));
    }

    @Test
    public void testPredicatesUsers() throws Throwable
    {
        Predicates<Integer> guard = new Predicates<>("x",
                                                     REASON,
                                                     state -> x -> x > 10,
                                                     state -> x -> x > 100,
                                                     (isWarn, value) -> format("%s: %s", isWarn ? "Warning" : "Aborting", value));

        assertTrue(guard.enabled());
        assertTrue(guard.enabled(null));
        assertTrue(guard.enabled(userClientState));
        assertFalse(guard.enabled(systemClientState));
        assertFalse(guard.enabled(superClientState));

        assertValid(() -> guard.guard(5, null));
        assertValid(() -> guard.guard(5, userClientState));
        assertValid(() -> guard.guard(5, systemClientState));
        assertValid(() -> guard.guard(5, superClientState));

        assertWarns(() -> guard.guard(25, null), "Warning: 25");
        assertWarns(() -> guard.guard(25, userClientState), "Warning: 25");
        assertValid(() -> guard.guard(25, systemClientState));
        assertValid(() -> guard.guard(25, superClientState));

        assertFails(() -> guard.guard(101,  null), false, "Aborting: 101");
        assertFails(() -> guard.guard(101,  userClientState), "Aborting: 101");
        assertValid(() -> guard.guard(101, systemClientState));
        assertValid(() -> guard.guard(101, superClientState));
    }

    private static Set<Integer> set(Integer value)
    {
        return Collections.singleton(value);
    }

    private static Set<Integer> set(Integer... values)
    {
        return new HashSet<>(Arrays.asList(values));
    }

    @SafeVarargs
    private static <T> Set<T> insertionOrderedSet(T... values)
    {
        return new LinkedHashSet<>(Arrays.asList(values));
    }
}
