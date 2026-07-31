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

package org.apache.cassandra.auth;

import java.util.Map;
import java.util.Set;

import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.exceptions.RequestExecutionException;

/**
 * Creates the initial role on a cluster which has no roles yet, so that there is some
 * identity available to authenticate as and grant permissions from. Selected via
 * {@code default_role_initializer} option in cassandra.yaml and instantiated by
 * {@link AuthConfig#applyAuth()}
 * <p>
 * Implementations decide both what the role is called and how it is authenticated.
 * See {@link PasswordDefaultRoleInitializer} which gives the role a password, and
 * {@link MutualTlsDefaultRoleInitializer} which gives no password and instead
 * maps a client certificate identity to itself.
 */
public interface IDefaultRoleInitializer
{
    /**
     * Creates the default role.
     * When using this in connection with CassandraRoleManager, every node runs this independently during initial
     * startup so implementations must write at {@link CassandraRoleManager#consistencyForRoleWrite(String)} to
     * avoid concurrent duplicate creation and must use {@code USING TIMESTAMP 0} so that any operator changes
     * to the role later supersede it.
     * <p>
     * The caller retries on failure so this may be invoked more than once on a node: it must not fail
     *
     * @throws RequestExecutionException if not enough nodes are available
     *                                   yet which the caller treats as a signal to reschedule
     */
    void createDefaultRole();

    /**
     * The name of the role {@link #createDefaultRole()} creates.
     */
    String defaultRoleName();

    /**
     * Validates configuration of the IDefaultRoleInitializer implementation (if configurable).
     * <p>
     * Called by {@link AuthConfig#applyAuth()} after the authenticator, authorizer and role manager have been
     * set, so implementations may inspect those to reject combinations which would leave the cluster with no
     * usable login.
     *
     * @throws ConfigurationException when there is a configuration error.
     */
    void validateConfiguration() throws ConfigurationException;

    /**
     * Checks {@link #hasExistingRoles()} and, if the cluster has none yet, calls {@link #createDefaultRole()}.
     * Implemented once, sealed, by {@link AbstractDefaultRoleInitializer} — see that class for the logic.
     */
    void initializeDefaultRoleIfNeeded();

    /**
     * @return true if the cluster already has at least one role (or the configured default role specifically).
     * Implemented once, sealed, by {@link AbstractDefaultRoleInitializer} — see that class for the logic.
     */
    boolean hasExistingRoles();

    /**
     * @param manager manager to check the support of
     * @return true if this role initializer conceptually works together with specified role manager, false otherwise
     */
    boolean supportsRoleManager(IRoleManager manager);

    static void validateSupportedParams(Map<String, String> parameters, Set<String> supportedParams, Class<?> implClass)
    {
        for (String param : parameters.keySet())
        {
            if (!supportedParams.contains(param))
                throw new ConfigurationException(String.format("Unsupported parameter '%s' for %s, supported parameters are %s",
                                                               param, implClass.getSimpleName(), supportedParams));
        }
    }
}
