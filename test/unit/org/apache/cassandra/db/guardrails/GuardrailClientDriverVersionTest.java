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
import java.util.HashMap;
import java.util.Map;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.apache.cassandra.config.GuardrailsOptions.validateAndSanitizeClientDriverVersions;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class GuardrailClientDriverVersionTest extends GuardrailTester
{
    private String originalWarned;
    private String originalDisallowed;

    @Before
    public void setup()
    {
        originalWarned = guardrails().getMinimumClientDriverVersionsWarned();
        originalDisallowed = guardrails().getMinimumClientDriverVersionsDisallowed();
    }

    @After
    public void teardown()
    {
        guardrails().setMinimumClientDriverVersionsWarned(originalWarned != null ? originalWarned : "{}");
        guardrails().setMinimumClientDriverVersionsDisallowed(originalDisallowed != null ? originalDisallowed : "{}");
    }

    @Test
    public void testVersionEqual()
    {
        assertFalse(ClientDriverVersionGuardrail.isBelowMinimum("4.18.0", "4.18.0"));
    }

    @Test
    public void testVersionLessThan()
    {
        assertTrue(ClientDriverVersionGuardrail.isBelowMinimum("4.17.0", "4.18.0"));
    }

    @Test
    public void testVersionGreaterThan()
    {
        assertFalse(ClientDriverVersionGuardrail.isBelowMinimum("4.19.0", "4.18.0"));
    }

    @Test
    public void testVersionMajorDifference()
    {
        assertTrue(ClientDriverVersionGuardrail.isBelowMinimum("3.11.0", "4.0.0"));
    }

    @Test
    public void testVersionMinorDifference()
    {
        assertTrue(ClientDriverVersionGuardrail.isBelowMinimum("4.2.0", "4.18.0"));
    }

    @Test
    public void testVersionWithVPrefix() throws Throwable
    {
        guardrails().setMinimumClientDriverVersionsWarned("{\"driver\":\"v2.0.1\"}");
        guardrails().setMinimumClientDriverVersionsDisallowed("{}");

        // above minimum — no warn
        assertValid(() -> Guardrails.minimumClientDriverVersion.guard("driver", "v2.0.2", userClientState));
        assertValid(() -> Guardrails.minimumClientDriverVersion.guard("driver", "2.0.2", userClientState));

        // below minimum — warn
        assertWarns(() -> Guardrails.minimumClientDriverVersion.guard("driver", "v2.0.0", userClientState),
                    "Client driver driver is below recommended minimum version 2.0.1");
        assertWarns(() -> Guardrails.minimumClientDriverVersion.guard("driver", "2.0.0", userClientState),
                    "Client driver driver is below recommended minimum version 2.0.1");
    }

    @Test
    public void testGuardrailNotTriggeredWhenEmpty() throws Throwable
    {
        guardrails().setMinimumClientDriverVersionsWarned("{}");
        guardrails().setMinimumClientDriverVersionsDisallowed("{}");

        assertValid(() -> Guardrails.minimumClientDriverVersion.guard("DataStax Java Driver", "3.0.0", userClientState));
    }

    @Test
    public void testGuardrailNotTriggeredWhenVersionAboveMinimum() throws Throwable
    {
        guardrails().setMinimumClientDriverVersionsWarned("{\"DataStax Java Driver\":\"4.18.0\"}");
        guardrails().setMinimumClientDriverVersionsDisallowed("{}");

        assertValid(() -> Guardrails.minimumClientDriverVersion.guard("DataStax Java Driver", "4.20.0", userClientState));
    }

    @Test
    public void testGuardrailNotTriggeredForUnknownDriver() throws Throwable
    {
        guardrails().setMinimumClientDriverVersionsWarned("{\"DataStax Java Driver\":\"4.18.0\"}");
        guardrails().setMinimumClientDriverVersionsDisallowed("{\"DataStax Java Driver\":\"4.0.0\"}");

        assertValid(() -> Guardrails.minimumClientDriverVersion.guard("gocql", "1.0.0", userClientState));
    }

    @Test
    public void testGuardrailNotTriggeredForNullDriverName() throws Throwable
    {
        guardrails().setMinimumClientDriverVersionsWarned("{\"DataStax Java Driver\":\"4.18.0\"}");
        assertValid(() -> Guardrails.minimumClientDriverVersion.guard(null, "4.15.0", userClientState));
    }

    @Test
    public void testGuardrailNotTriggeredForNullDriverVersion() throws Throwable
    {
        guardrails().setMinimumClientDriverVersionsWarned("{\"DataStax Java Driver\":\"4.18.0\"}");
        assertValid(() -> Guardrails.minimumClientDriverVersion.guard("DataStax Java Driver", null, userClientState));
    }

    @Test
    public void testGuardrailNotTriggeredForBothNullDriverAndVersion() throws Throwable
    {
        guardrails().setMinimumClientDriverVersionsWarned("{\"DataStax Java Driver\":\"4.18.0\"}");
        assertValid(() -> Guardrails.minimumClientDriverVersion.guard(null, null, userClientState));
    }

    @Test
    public void testGuardrailNotTriggeredForEmptyDriverName() throws Throwable
    {
        guardrails().setMinimumClientDriverVersionsWarned("{\"DataStax Java Driver\":\"4.18.0\"}");

        assertValid(() -> Guardrails.minimumClientDriverVersion.guard("", "4.15.0", userClientState));
        assertValid(() -> Guardrails.minimumClientDriverVersion.guard("  ", "4.15.0", userClientState));
    }

    @Test
    public void testGuardrailNotTriggeredForEmptyDriverVersion() throws Throwable
    {
        guardrails().setMinimumClientDriverVersionsWarned("{\"DataStax Java Driver\":\"4.18.0\"}");

        assertValid(() -> Guardrails.minimumClientDriverVersion.guard("DataStax Java Driver", "", userClientState));
        assertValid(() -> Guardrails.minimumClientDriverVersion.guard("DataStax Java Driver", "  ", userClientState));
    }

    @Test
    public void testGuardrailWarns() throws Throwable
    {
        guardrails().setMinimumClientDriverVersionsWarned("{\"DataStax Java Driver\":\"4.18.0\"}");
        guardrails().setMinimumClientDriverVersionsDisallowed("{}");

        assertWarns(() -> Guardrails.minimumClientDriverVersion.guard("DataStax Java Driver", "4.15.0", userClientState),
                    "Client driver DataStax Java Driver is below recommended minimum version 4.18.0");
    }

    @Test
    public void testGuardrailFails() throws Throwable
    {
        guardrails().setMinimumClientDriverVersionsWarned("{}");
        guardrails().setMinimumClientDriverVersionsDisallowed("{\"DataStax Java Driver\":\"4.0.0\"}");

        assertFails(() -> Guardrails.minimumClientDriverVersion.guard("DataStax Java Driver", "3.11.0", userClientState),
                    "Client driver DataStax Java Driver is below required minimum version 4.0.0, connection rejected");
    }

    @Test
    public void testGuardrailFailTakesPrecedenceOverWarn() throws Throwable
    {
        guardrails().setMinimumClientDriverVersionsWarned("{\"DataStax Java Driver\":\"4.18.0\"}");
        guardrails().setMinimumClientDriverVersionsDisallowed("{\"DataStax Java Driver\":\"4.0.0\"}");

        assertFails(() -> Guardrails.minimumClientDriverVersion.guard("DataStax Java Driver", "3.11.0", userClientState),
                    "Client driver DataStax Java Driver is below required minimum version 4.0.0, connection rejected");
    }

    @Test
    public void testGuardrailWarnOnly() throws Throwable
    {
        guardrails().setMinimumClientDriverVersionsWarned("{\"DataStax Java Driver\":\"4.18.0\"}");
        guardrails().setMinimumClientDriverVersionsDisallowed("{\"DataStax Java Driver\":\"4.0.0\"}");

        assertWarns(() -> Guardrails.minimumClientDriverVersion.guard("DataStax Java Driver", "4.15.0", userClientState),
                    "Client driver DataStax Java Driver is below recommended minimum version 4.18.0");
    }

    @Test
    public void testGuardrailWithVPrefixVersion() throws Throwable
    {
        guardrails().setMinimumClientDriverVersionsWarned("{\"DataStax Java Driver\":\"4.18.0\"}");
        guardrails().setMinimumClientDriverVersionsDisallowed("{}");

        assertWarns(() -> Guardrails.minimumClientDriverVersion.guard("DataStax Java Driver", "v4.15.0", userClientState),
                    "Client driver DataStax Java Driver is below recommended minimum version 4.18.0");
    }

    @Test
    public void testGuardrailColonFormatWarns() throws Throwable
    {
        guardrails().setMinimumClientDriverVersionsWarned("{\"DataStax Java Driver\":\"4.18.0\"}");
        guardrails().setMinimumClientDriverVersionsDisallowed("{}");

        assertWarns(() -> Guardrails.minimumClientDriverVersion.guard("DataStax Java Driver:4.15.0", userClientState),
                    "Client driver DataStax Java Driver is below recommended minimum version 4.18.0");
    }

    @Test
    public void testGuardrailColonFormatNull() throws Throwable
    {
        guardrails().setMinimumClientDriverVersionsWarned("{\"DataStax Java Driver\":\"4.18.0\"}");

        assertValid(() -> Guardrails.minimumClientDriverVersion.guard(null, userClientState));
    }

    @Test
    public void testGuardrailColonFormatNoSeparator() throws Throwable
    {
        guardrails().setMinimumClientDriverVersionsWarned("{\"DataStax Java Driver\":\"4.18.0\"}");

        assertValid(() -> Guardrails.minimumClientDriverVersion.guard("DataStax Java Driver", userClientState));
    }

    @Test
    public void testJmxGetSetWarned()
    {
        guardrails().setMinimumClientDriverVersionsWarned("{\"DataStax Java Driver\":\"4.18.0\"}");
        String json = guardrails().getMinimumClientDriverVersionsWarned();
        assertTrue(json.contains("DataStax Java Driver"));
        assertTrue(json.contains("4.18.0"));
    }

    @Test
    public void testJmxGetSetDisallowed()
    {
        guardrails().setMinimumClientDriverVersionsDisallowed("{\"DataStax Java Driver\":\"4.0.0\",\"DataStax Python Driver\":\"3.0.0\"}");
        String json = guardrails().getMinimumClientDriverVersionsDisallowed();
        assertTrue(json.contains("DataStax Java Driver"));
        assertTrue(json.contains("DataStax Python Driver"));
    }

    @Test
    public void testValidateValidVersions()
    {
        validateAndSanitizeClientDriverVersions(new HashMap<>(Map.of("driver", "4.18.0")), "test");
        validateAndSanitizeClientDriverVersions(new HashMap<>(Map.of("driver", "v4.18.0")), "test");
        validateAndSanitizeClientDriverVersions(new HashMap<>(Map.of("driver", "V4.18.0")), "test");
        validateAndSanitizeClientDriverVersions(new HashMap<>(Map.of("a", "1.0.0", "b", "2.0.0")), "test");
    }

    @Test
    public void testValidateNullMap()
    {
        validateAndSanitizeClientDriverVersions(null, "test");
    }

    @Test
    public void testValidateEmptyMap()
    {
        validateAndSanitizeClientDriverVersions(Collections.emptyMap(), "test");
    }

    @Test
    public void testValidateInvalidVersion()
    {
        assertThatThrownBy(() -> validateAndSanitizeClientDriverVersions(Map.of("driver", "not-a-version"), "test"))
        .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void testValidateEmptyVersion()
    {
        assertThatThrownBy(() -> validateAndSanitizeClientDriverVersions(Map.of("driver", ""), "test"))
        .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void testValidateBlankVersion()
    {
        assertThatThrownBy(() -> validateAndSanitizeClientDriverVersions(Map.of("driver", "  "), "test"))
        .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void testValidateMixedValidAndInvalid()
    {
        Map<String, String> map = new HashMap<>();
        map.put("good-driver", "4.18.0");
        map.put("bad-driver", "xyz");
        assertThatThrownBy(() -> validateAndSanitizeClientDriverVersions(map, "test"))
        .isInstanceOf(IllegalArgumentException.class);
    }
}
