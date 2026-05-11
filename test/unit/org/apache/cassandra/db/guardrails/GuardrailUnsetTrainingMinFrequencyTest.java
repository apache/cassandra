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

import org.junit.Test;

public class GuardrailUnsetTrainingMinFrequencyTest extends GuardrailTester
{
    private static final String UNSET_FREQUENCY = "CREATE TABLE IF NOT EXISTS tb1 (k int PRIMARY KEY, a int, b int) " +
                                                  "WITH compression = {'class': 'ZstdDictionaryCompressor', " +
                                                  "'training_min_frequency': '0m' }";

    private static final String SET_FREQUENCY = "CREATE TABLE IF NOT EXISTS tb1 (k int PRIMARY KEY, a int, b int) " +
                                                "WITH compression = {'class': 'ZstdDictionaryCompressor', " +
                                                "'training_min_frequency': '1d' }";

    private static final String NOT_DICT_COMPRESSOR = "CREATE TABLE IF NOT EXISTS tb1 (k int PRIMARY KEY, a int, b int) " +
                                                      "WITH compression = {'class': 'ZstdCompressor' }";

    public GuardrailUnsetTrainingMinFrequencyTest()
    {
        super(Guardrails.unsetTrainingMinFrequency);
    }

    @Test
    public void testGuardrailDisabled() throws Throwable
    {
        prepareTest(false, true);
        assertFails(UNSET_FREQUENCY, "unset minimum frequency of training for dictionary compressor is not allowed");
    }

    @Test
    public void testGuardrailEnabledWarnEnabled() throws Throwable
    {
        prepareTest(true, true);
        assertWarns(UNSET_FREQUENCY, "unset minimum frequency of training for dictionary compressor is not recommended");
    }

    @Test
    public void testGuardrailEnabledWarnDisabled() throws Throwable
    {
        prepareTest(true, false);
        assertValid(SET_FREQUENCY);
        assertValid(UNSET_FREQUENCY);
    }

    @Test
    public void testGuardrailNotTriggered() throws Throwable
    {
        prepareTest(true, true);
        assertValid(SET_FREQUENCY);
        assertValid(NOT_DICT_COMPRESSOR);

        prepareTest(false, true);
        assertValid(SET_FREQUENCY);
        assertValid(NOT_DICT_COMPRESSOR);
    }

    @Test
    public void testExcludedUsers() throws Throwable
    {
        for (boolean enabled : new boolean[]{ false, true })
        {
            for (boolean warned : new boolean[]{ false, true })
            {
                prepareTest(enabled, warned);
                testExcludedUsers(() -> UNSET_FREQUENCY,
                                  () -> SET_FREQUENCY,
                                  () -> NOT_DICT_COMPRESSOR);
            }
        }
    }

    private void prepareTest(boolean enabled, boolean warned)
    {
        guardrails().setUnsetTrainingMinFrequencyEnabled(enabled);
        guardrails().setUnsetTrainingMinFrequencyWarned(warned);
    }
}
