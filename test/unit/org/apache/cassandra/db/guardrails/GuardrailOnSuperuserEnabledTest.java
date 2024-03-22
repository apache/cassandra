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

import java.util.Collections;

import com.google.common.collect.ImmutableList;
import org.junit.Before;
import org.junit.Test;

import static java.lang.String.format;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;

public class GuardrailOnSuperuserEnabledTest extends GuardrailTester
{
    ImmutableList<Guardrail> guards;

    @Before
    public void setup()
    {
        // by default, guardrails are applied to superuser
        Guardrails.instance.setGuardrailsOnSuperuserEnabled(true);
        MaxThreshold maxThresholdGuard = new MaxThreshold("x",
                                                          state -> 10,
                                                          state -> 100,
                                                          (isWarn, what, v, t) -> format("%s: for %s, %s > %s",
                                                                                         isWarn ? "Warning" : "Aborting", what, v, t));
        MinThreshold minThresholdGuard = new MinThreshold("x",
                                                          state -> 100,
                                                          state -> 10,
                                                          (isWarn, what, v, t) -> format("%s: for %s, %s < %s",
                                                                                         isWarn ? "Warning" : "Aborting", what, v, t));
        DisableFlag disableFlagGuard = new DisableFlag("x", state -> true, "X");
        Values<Integer> valueGaurd = new Values<>("x",
                                                  state -> Collections.singleton(2),
                                                  state -> Collections.singleton(3),
                                                  state -> Collections.singleton(4),
                                                  "integer");
        Predicates<Integer> predicateGuard = new Predicates<>("x",
                                                              state -> x -> x > 10,
                                                              state -> x -> x > 100,
                                                              (isWarn, value) -> format("%s: %s",
                                                                                        isWarn ? "Warning" : "Aborting", value));
        guards = ImmutableList.of(maxThresholdGuard, minThresholdGuard, disableFlagGuard, valueGaurd, predicateGuard);
    }

    @Test
    public void testGuardrailsShouldNotApplyOnSuperuserWhenFeatureDisabled()
    {
        Guardrails.instance.setGuardrailsOnSuperuserEnabled(false);
        assertFalse(Guardrails.instance.getGuardrailsOnSuperuserEnabled());
        // are not applied to superuser when feature disabled
        assertFalse(guards.stream().anyMatch(x -> x.enabled(superClientState)));
        // are not applied to system calls
        assertFalse(guards.stream().anyMatch(x -> x.enabled(systemClientState)));
        // are applied to anonymous calls (if any)
        assertTrue(guards.stream().allMatch(x -> x.enabled(null)));
        // are applied to user state
        assertTrue(guards.stream().allMatch(x -> x.enabled(userClientState)));
    }

    @Test
    public void testGuardrailsShouldApplyOnSuperuserWhenFeatureEnabled()
    {
        Guardrails.instance.setGuardrailsOnSuperuserEnabled(true);
        assertTrue(Guardrails.instance.getGuardrailsOnSuperuserEnabled());
        // are applied to superuser when feature enabled
        assertTrue(guards.stream().allMatch(x -> x.enabled(superClientState)));
        // are not applied to system calls
        assertFalse(guards.stream().anyMatch(x -> x.enabled(systemClientState)));
        // are applied to anonymous calls (if any)
        assertTrue(guards.stream().allMatch(x -> x.enabled(null)));
        // are applied to user state
        assertTrue(guards.stream().allMatch(x -> x.enabled(userClientState)));
    }
}
