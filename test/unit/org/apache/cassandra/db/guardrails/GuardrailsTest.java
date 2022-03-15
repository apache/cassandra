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

    @Test
    public void testDisabledThreshold() throws Throwable
    {
        Threshold.ErrorMessageProvider errorMessageProvider = (isWarn, what, v, t) -> "Should never trigger";
        testDisabledThreshold(new Threshold("x", state -> DISABLED, state -> DISABLED, errorMessageProvider));
    }

    private void testDisabledThreshold(Threshold guard) throws Throwable
    {
        assertFalse(guard.enabled(userClientState));

        assertValid(() -> guard.guard(5, "Z", null));
        assertValid(() -> guard.guard(25, "A", userClientState));
        assertValid(() -> guard.guard(100, "B", userClientState));
        assertValid(() -> guard.guard(101, "X", userClientState));
        assertValid(() -> guard.guard(200, "Y", userClientState));
    }

    @Test
    public void testThreshold() throws Throwable
    {
        Threshold guard = new Threshold("x", 
                                        state -> 10,
                                        state -> 100,
                                        (isWarn, what, v, t) -> format("%s: for %s, %s > %s",
                                                                       isWarn ? "Warning" : "Aborting", what, v, t));

        assertTrue(guard.enabled(userClientState));

        assertValid(() -> guard.guard(5, "Z", userClientState));
        assertWarns(() -> guard.guard(25, "A", userClientState), "Warning: for A, 25 > 10");
        assertWarns(() -> guard.guard(100, "B", userClientState), "Warning: for B, 100 > 10");
        assertFails(() -> guard.guard(101, "X", userClientState), "Aborting: for X, 101 > 100");
        assertFails(() -> guard.guard(200, "Y", userClientState), "Aborting: for Y, 200 > 100");
        assertValid(() -> guard.guard(5, "Z", userClientState));
    }

    @Test
    public void testWarnOnlyThreshold() throws Throwable
    {
        Threshold guard = new Threshold("x",
                                        state -> 10,
                                        state -> DISABLED,
                                        (isWarn, what, v, t) -> format("%s: for %s, %s > %s",
                                                                       isWarn ? "Warning" : "Aborting", what, v, t));

        assertTrue(guard.enabled(userClientState));

        assertValid(() -> guard.guard(5, "Z", userClientState));
        assertWarns(() -> guard.guard(11, "A", userClientState), "Warning: for A, 11 > 10");
    }

    @Test
    public void testFailOnlyThreshold() throws Throwable
    {
        Threshold guard = new Threshold("x",
                                        state -> DISABLED,
                                        state -> 10,
                                        (isWarn, what, v, t) -> format("%s: for %s, %s > %s",
                                                                       isWarn ? "Warning" : "Aborting", what, v, t));

        assertTrue(guard.enabled(userClientState));

        assertValid(() -> guard.guard(5, "Z", userClientState));
        assertFails(() -> guard.guard(11, "A", userClientState), "Aborting: for A, 11 > 10");
    }

    @Test
    public void testThresholdUsers() throws Throwable
    {
        Threshold guard = new Threshold("x",
                                        state -> 10,
                                        state -> 100,
                                        (isWarn, what, v, t) -> format("%s: for %s, %s > %s",
                                                                       isWarn ? "Warning" : "Failure", what, v, t));

        // value under both thresholds
        assertValid(() -> guard.guard(5, "x", null));
        assertValid(() -> guard.guard(5, "x", userClientState));
        assertValid(() -> guard.guard(5, "x", systemClientState));
        assertValid(() -> guard.guard(5, "x", superClientState));

        // value over warning threshold
        assertWarns(() -> guard.guard(100, "y", null), "Warning: for y, 100 > 10");
        assertWarns(() -> guard.guard(100, "y", userClientState), "Warning: for y, 100 > 10");
        assertValid(() -> guard.guard(100, "y", systemClientState));
        assertValid(() -> guard.guard(100, "y", superClientState));

        // value over fail threshold. An undefined user means that the check comes from a background process,
        // so we warn instead of failing to prevent interrupting that process.
        assertWarns(() -> guard.guard(101, "z", null), "Failure: for z, 101 > 100");
        assertFails(() -> guard.guard(101, "z", userClientState), "Failure: for z, 101 > 100");
        assertValid(() -> guard.guard(101, "z", systemClientState));
        assertValid(() -> guard.guard(101, "z", superClientState));
    }

    @Test
    public void testDisableFlag() throws Throwable
    {
        assertFails(() -> new DisableFlag("x", state -> true, "X").ensureEnabled(userClientState), "X is not allowed");
        assertValid(() -> new DisableFlag("x", state -> false, "X").ensureEnabled(userClientState));

        assertFails(() -> new DisableFlag("x", state -> true, "X").ensureEnabled("Y", userClientState), "Y is not allowed");
        assertValid(() -> new DisableFlag("x", state -> false, "X").ensureEnabled("Y", userClientState));
    }

    @Test
    public void testDisableFlagUsers() throws Throwable
    {
        DisableFlag enabled = new DisableFlag("x", state -> false, "X");
        assertValid(() -> enabled.ensureEnabled(null));
        assertValid(() -> enabled.ensureEnabled(userClientState));
        assertValid(() -> enabled.ensureEnabled(systemClientState));
        assertValid(() -> enabled.ensureEnabled(superClientState));

        DisableFlag disabled = new DisableFlag("x", state -> true, "X");
        assertFails(() -> disabled.ensureEnabled(userClientState), "X is not allowed");
        assertValid(() -> disabled.ensureEnabled(systemClientState));
        assertValid(() -> disabled.ensureEnabled(superClientState));
    }

    @Test
    public void testValuesWarned() throws Throwable
    {
        // Using a sorted set below to ensure the order in the warning message checked below is not random
        Values<Integer> warned = new Values<>("x", 
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

        assertWarns(() -> disallowed.guard(set(4, 6), action, null),
                    "Provided values [4, 6] are not allowed for integer (disallowed values are: [4, 6, 20])");
        assertWarns(() -> disallowed.guard(set(4, 5, 6, 7), action, null),
                    "Provided values [4, 6] are not allowed for integer (disallowed values are: [4, 6, 20])");
    }

    @Test
    public void testValuesUsers() throws Throwable
    {
        Values<Integer> disallowed = new Values<>("x", 
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
        assertWarns(() -> disallowed.guard(set(4), action, null), message);
        assertFails(() -> disallowed.guard(set(4), action, userClientState), message);
        assertValid(() -> disallowed.guard(set(4), action, systemClientState));
        assertValid(() -> disallowed.guard(set(4), action, superClientState));
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
