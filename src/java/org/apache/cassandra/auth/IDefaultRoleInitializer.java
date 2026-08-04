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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.exceptions.RequestExecutionException;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.tcm.ClusterMetadata;

import static org.apache.cassandra.auth.CassandraRoleManager.escape;

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
    Logger logger = LoggerFactory.getLogger(IDefaultRoleInitializer.class);

    String DEFAULT_ROLE_INITIALIZER_CLASS_NAME = "default_role_initializer_class_name";

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
    default void validateConfiguration() throws ConfigurationException
    {
    }

    /*
     * Create the default superuser role to bootstrap role creation on a clean system. Preemptively
     * gives the role the default password so PasswordAuthenticator can be used to log in (if
     * configured)
     */
    default void setupDefaultRole()
    {
        if (ClusterMetadata.current().tokenMap.tokens().isEmpty())
            throw new IllegalStateException("CassandraRoleManager skipped default role setup: no known tokens in ring");

        try
        {
            if (!hasExistingRoles())
            {
                createDefaultRole();
            }
        }
        catch (RequestExecutionException e)
        {
            logger.warn("CassandraRoleManager skipped default role setup: some nodes were not ready");
            throw e;
        }
    }

    default boolean hasExistingRoles()
    {
        // Try looking up the configured default role first, to avoid the range query if possible.
        String defaultRoleQuery = String.format("SELECT * FROM %s.%s WHERE role = '%s'", SchemaConstants.AUTH_KEYSPACE_NAME, AuthKeyspace.ROLES, escape(defaultRoleName()));
        String allUsersQuery = String.format("SELECT * FROM %s.%s LIMIT 1", SchemaConstants.AUTH_KEYSPACE_NAME, AuthKeyspace.ROLES);
        return !QueryProcessor.process(defaultRoleQuery, ConsistencyLevel.ONE).isEmpty()
               || !QueryProcessor.process(defaultRoleQuery, ConsistencyLevel.QUORUM).isEmpty()
               || !QueryProcessor.process(allUsersQuery, ConsistencyLevel.QUORUM).isEmpty();
    }
}
