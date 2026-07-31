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

import org.apache.cassandra.exceptions.ConfigurationException;

/**
 * Creates the initial role on a cluster which has no roles yet, so that there is some
 * identity available to authenticate as and grant permissions from. Selected via
 * {@code default_role_initializer} option in cassandra.yaml and instantiated by
 * {@link AuthConfig#applyAuth()}
 *
 * Implementations decide both what the role is called and how it is authenticated.
 * See {@link PasswordDefaultRoleInitializer} which gives the role a password, and
 * {@link MutualTlsDefaultRoleInitializer} which gives no password and instead
 * maps a client certificate identity to itself.
 */
public interface IDefaultRoleInitializer
{
    /**
     * Creates the default role. Called from {@link CassandraRoleManager#setup(boolean)} only after
     * {@link CassandraRoleManager#hasExistingRoles()} has established that the cluster has no rules.
     *
     * Every node runs this independently during initial startup so implementations must write at
     * {@link CassandraRoleManager#consistencyForRoleWrite(String)} to avoid concurrent duplicate creation
     * and must use {@code USING TIMESTAMP 0} so that any operator changes to the role later supersede it.
     *
     * The caller retries on failure so this may be invoked more than once on a node: it must not fail
     * @throws org.apache.cassandra.exceptions.RequestExecutionException if not enough nodes are available
     * yet which the caller treats as a signal to reschedule
     */
    void initialize();

    /**
     * The name of the role {@link #initialize()} creates.
     *
     * The default role is a special case during startup: reads and writes of it are performed at
     * {@link CassandraRoleManager#DEFAULT_SUPERUSER_CONSISTENCY_LEVEL} rather than at the configured auth
     * consistency levels, and {@link CassandraRoleManager#hasExistingRoles()} looks it up by name before
     * falling back to a range query. Both need to know the configured name, not assume
     * {@link CassandraRoleManager#DEFAULT_SUPERUSER_NAME}.
     */
    String defaultRoleName();

    /**
     * Validates configuration of the IDefaultRoleInitializer implementation (if configurable).
     *
     * Called by {@link AuthConfig#applyAuth()} after the authenticator, authorizer and role manager have been
     * set, so implementations may inspect those to reject combinations which would leave the cluster with no
     * usable login.
     *
     * @throws ConfigurationException when there is a configuration error.
     */
    default void validateConfiguration() throws ConfigurationException {}
}
