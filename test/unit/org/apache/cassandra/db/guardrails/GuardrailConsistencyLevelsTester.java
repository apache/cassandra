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
import java.util.EnumSet;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.db.ConsistencyLevel;

/**
 * Utilty class for testing the guardrails for read/write consistency levels.
 */
public abstract class GuardrailConsistencyLevelsTester extends GuardrailTester
{
    private final String warnedPropertyName;
    private final String disallowePropertyName;
    private final Function<Guardrails, Set<String>> warnedGetter;
    private final Function<Guardrails, Set<String>> disallowedGetter;
    private final Function<Guardrails, String> warnedCSVGetter;
    private final Function<Guardrails, String> disallowedCSVGetter;
    private final BiConsumer<Guardrails, Set<String>> warnedSetter;
    private final BiConsumer<Guardrails, Set<String>> disallowedSetter;
    private final BiConsumer<Guardrails, String> warnedCSVSetter;
    private final BiConsumer<Guardrails, String> disallowedCSVSetter;

    public GuardrailConsistencyLevelsTester(String warnedPropertyName,
                                            String disallowePropertyName,
                                            Values<ConsistencyLevel> guardrail,
                                            Function<Guardrails, Set<String>> warnedGetter,
                                            Function<Guardrails, Set<String>> disallowedGetter,
                                            Function<Guardrails, String> warnedCSVGetter,
                                            Function<Guardrails, String> disallowedCSVGetter,
                                            BiConsumer<Guardrails, Set<String>> warnedSetter,
                                            BiConsumer<Guardrails, Set<String>> disallowedSetter,
                                            BiConsumer<Guardrails, String> warnedCSVSetter,
                                            BiConsumer<Guardrails, String> disallowedCSVSetter)
    {
        super(guardrail);
        this.warnedPropertyName = warnedPropertyName;
        this.disallowePropertyName = disallowePropertyName;
        this.warnedGetter = warnedGetter;
        this.disallowedGetter = disallowedGetter;
        this.warnedCSVGetter = g -> sortCSV(warnedCSVGetter.apply(g));
        this.disallowedCSVGetter = g -> sortCSV(disallowedCSVGetter.apply(g));
        this.warnedSetter = warnedSetter;
        this.disallowedSetter = disallowedSetter;
        this.warnedCSVSetter = warnedCSVSetter;
        this.disallowedCSVSetter = disallowedCSVSetter;
    }

    @Before
    public void before()
    {
        warnConsistencyLevels();
        disableConsistencyLevels();
    }

    protected void warnConsistencyLevels(ConsistencyLevel... consistencyLevels)
    {
        warnedSetter.accept(guardrails(), Stream.of(consistencyLevels).map(ConsistencyLevel::name).collect(Collectors.toSet()));
    }

    protected void disableConsistencyLevels(ConsistencyLevel... consistencyLevels)
    {
        disallowedSetter.accept(guardrails(), Stream.of(consistencyLevels).map(ConsistencyLevel::name).collect(Collectors.toSet()));
    }

    @Test
    public void testConfigValidation()
    {
        String message = "Invalid value for %s: null is not allowed";
        assertInvalidProperty(warnedSetter, null, message, warnedPropertyName);
        assertInvalidProperty(disallowedSetter, null, message, disallowePropertyName);

        assertValidProperty(Collections.emptySet());
        assertValidProperty(EnumSet.allOf(ConsistencyLevel.class));

        assertValidPropertyCSV("");
        assertValidPropertyCSV(EnumSet.allOf(ConsistencyLevel.class)
                                      .stream()
                                      .map(ConsistencyLevel::toString)
                                      .collect(Collectors.joining(",")));

        assertInvalidPropertyCSV("invalid", "INVALID");
        assertInvalidPropertyCSV("ONE,invalid1,invalid2", "INVALID1");
        assertInvalidPropertyCSV("invalid1,invalid2,ONE", "INVALID1");
        assertInvalidPropertyCSV("invalid1,ONE,invalid2", "INVALID1");
    }

    private void assertValidProperty(Set<ConsistencyLevel> input)
    {
        Set<String> properties = input.stream().map(ConsistencyLevel::name).collect(Collectors.toSet());
        assertValidProperty(warnedSetter, warnedGetter, properties);
        assertValidProperty(disallowedSetter, disallowedGetter, properties);
    }

    private void assertValidPropertyCSV(String csv)
    {
        csv = sortCSV(csv);
        assertValidProperty(warnedCSVSetter, warnedCSVGetter, csv);
        assertValidProperty(disallowedCSVSetter, disallowedCSVGetter, csv);
    }

    private void assertInvalidPropertyCSV(String properties, String rejected)
    {
        String message = "No enum constant org.apache.cassandra.db.ConsistencyLevel.%s";
        assertInvalidProperty(warnedCSVSetter, properties, message, rejected);
        assertInvalidProperty(disallowedCSVSetter, properties, message, rejected);
    }
}
