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

import java.util.Map;
import java.util.function.Function;

import javax.annotation.Nullable;

import com.vdurmont.semver4j.Semver;
import com.vdurmont.semver4j.Semver.SemverType;

import org.apache.cassandra.config.GuardrailsOptions;
import org.apache.cassandra.service.ClientState;

/**
 * A guardrail that warns or rejects client connections whose driver version
 * is below a configured minimum per driver name.
 * <p>
 * The guardrail is configured with maps of driver name to minimum version string.
 * Connections that do not report a driver name or version are considered valid
 * and are not subject to this guardrail.
 * <p>
 * Version comparison uses semantic versioning via {@link Semver} with loose parsing
 * to handle non-standard version strings reported by drivers.
 * <p>
 * This guadrail also reacts on "unknown: 0.0.0" and "unset: 0.0.0" entries in the configuration map.
 * When either is set, when a driver does not set its driver id or driver id is not among explicitly enumerated
 * in the configuration map, the connection will fail / will be warned respectively.
 */
public class ClientDriverVersionGuardrail extends Predicates<String>
{
    private final Function<ClientState, Map<String, String>> warnVersions;
    private final Function<ClientState, Map<String, String>> disallowVersions;

    public ClientDriverVersionGuardrail(Function<ClientState, Map<String, String>> warnVersions,
                                        Function<ClientState, Map<String, String>> disallowVersions)
    {
        super("minimum_client_driver_versions", null, null, null, null);
        this.warnVersions = warnVersions;
        this.disallowVersions = disallowVersions;
    }

    public void guard(@Nullable String driverName, @Nullable String driverVersion, @Nullable ClientState state)
    {
        if (!enabled(state))
            return;

        Map<String, String> disallowed = disallowVersions.apply(state);
        Map<String, String> warned = warnVersions.apply(state);

        String sanitizedDriverId = driverName == null ? null : driverName.trim();
        if (sanitizedDriverId == null || sanitizedDriverId.isBlank())
        {
            logger.debug("minimum_client_driver_versions guardrail identified empty driver id to check the minimum version of");
            if (disallowed != null && disallowed.containsKey("unset"))
            {
                fail("Connections with unset driver id's are forbidden.", state);
                return;
            }

            if (warned != null && warned.containsKey("unset"))
            {
                warn("Connection with unset driver id was detected.");
            }
            return;
        }

        String sanitizedDriverVersion = GuardrailsOptions.sanitizeVersion(driverVersion);
        if (sanitizedDriverVersion == null)
        {
            sanitizedDriverVersion = "unset";
            logger.debug("minimum_client_driver_versions guardrail identified empty driver version to check the minimum version of");
        }
        else if (!GuardrailsOptions.isValidVersion(sanitizedDriverVersion))
        {
            logger.debug("minimum_client_driver_versions guardrail identified driver version which is not compliant semver version");
            return;
        }

        if (disallowed != null && !disallowed.isEmpty())
        {
            String minimumVersionFail = disallowed.get(sanitizedDriverId);
            if (minimumVersionFail != null && (sanitizedDriverVersion.equals("unset") || isBelowMinimum(sanitizedDriverVersion, minimumVersionFail)))
            {
                fail(String.format("Client driver %s, version %s is below required minimum version %s, connection rejected",
                                   sanitizedDriverId,
                                   sanitizedDriverVersion,
                                   minimumVersionFail),
                     state);
                return;
            }
            else if (minimumVersionFail == null && disallowed.containsKey("unknown"))
            {
                fail(String.format("Detected unknown driver id: %s.", sanitizedDriverId), state);
                return;
            }
        }

        if (warned != null && !warned.isEmpty())
        {
            String minimumVersionWarn = warned.get(sanitizedDriverId);
            if (minimumVersionWarn != null && (sanitizedDriverVersion.equals("unset") || isBelowMinimum(sanitizedDriverVersion, minimumVersionWarn)))
            {
                warn(String.format("Client driver %s, version %s is below recommended minimum version %s",
                                   sanitizedDriverId,
                                   sanitizedDriverVersion,
                                   minimumVersionWarn));
            }
            else if (minimumVersionWarn == null && warned.containsKey("unknown"))
            {
                warn(String.format("Detected unknown driver id: %s", sanitizedDriverId));
            }
        }
    }

    /**
     * This method should not be used in normal circumstances as {@link #guard(String, String, ClientState)}
     * should be called directly instead.
     *
     * @param rawDriverId the value to check, driver name and version delimited by a colon.
     * @param state       client state
     */
    @Override
    public void guard(String rawDriverId, @Nullable ClientState state)
    {
        if (rawDriverId == null)
        {
            guard(null, null, state);
        }
        else
        {
            String[] pair = rawDriverId.trim().split(":");
            if (pair.length != 2)
                return;

            String sanitizedDriverName = pair[0].trim();
            if (sanitizedDriverName.isBlank())
                return;

            String sanitizedDriverVersion = pair[1].trim();
            if (sanitizedDriverVersion.isBlank())
                return;

            guard(sanitizedDriverName, sanitizedDriverVersion, state);
        }
    }

    /**
     * Checks if the driver version is below the minimum version
     * specified in the config map. If driver name is not in minimum versions,
     * such connection is considered to be allowed.
     * <p>
     *
     * @param driverVersion  version of a driver
     * @param minimumVersion minimum allowed version
     * @return true if the driver version is lower than the configured minimum
     */
    static boolean isBelowMinimum(String driverVersion, String minimumVersion)
    {
        Semver versionToCheck = new Semver(driverVersion, SemverType.LOOSE);
        Semver minimumVersionAllowed = new Semver(minimumVersion, SemverType.LOOSE);
        return versionToCheck.isLowerThan(minimumVersionAllowed);
    }
}
