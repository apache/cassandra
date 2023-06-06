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

import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.GuardrailsOptions;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.service.ClientState;
import org.assertj.core.api.Assertions;

import static java.lang.String.format;

public class GuardrailsConfigProviderTest extends GuardrailTester
{
    @Test
    public void testBuildCustom() throws Throwable
    {
        String name = getClass().getCanonicalName() + '$' + CustomProvider.class.getSimpleName();
        GuardrailsConfigProvider provider = GuardrailsConfigProvider.build(name);
        MaxThreshold guard = new MaxThreshold("test_guardrail",
                                        "Some reason",
                                        state -> provider.getOrCreate(state).getTablesWarnThreshold(),
                                        state -> provider.getOrCreate(state).getTablesFailThreshold(),
                                        (isWarn, what, v, t) -> format("%s: for %s, %s > %s",
                                                                       isWarn ? "Warning" : "Aborting", what, v, t));

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

        Assertions.assertThatThrownBy(() -> GuardrailsConfigProvider.build("unexistent_class"))
                  .isInstanceOf(ConfigurationException.class)
                  .hasMessageContaining("Unable to find custom guardrails config provider class 'unexistent_class'");
    }

    /**
     * Custom {@link GuardrailsConfigProvider} implementation that simply duplicates the threshold values.
     */
    public static class CustomProvider extends GuardrailsConfigProvider.Default
    {
        public GuardrailsConfig getOrCreate(ClientState state)
        {
            return new CustomConfig(DatabaseDescriptor.getRawConfig());
        }
    }

    public static class CustomConfig extends GuardrailsOptions
    {
        public CustomConfig(Config config)
        {
            super(config);
        }

        @Override
        public int getTablesWarnThreshold()
        {
            return 10;
        }

        @Override
        public int getTablesFailThreshold()
        {
            return 100;
        }
    }
}
