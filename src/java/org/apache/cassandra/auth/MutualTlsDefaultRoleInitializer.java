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

import com.google.common.base.Strings;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.schema.SchemaConstants;

import static org.apache.cassandra.auth.CassandraRoleManager.consistencyForRoleWrite;
import static org.apache.cassandra.auth.CassandraRoleManager.escape;

public class MutualTlsDefaultRoleInitializer implements IDefaultRoleInitializer
{
    private static final Logger logger = LoggerFactory.getLogger(MutualTlsDefaultRoleInitializer.class);
    public static final String ROLE = "role";
    public static final String IDENTITY = "identity";

    private static final Set<String> SUPPORTED_PARAMS = Set.of(ROLE, IDENTITY);
    private final String role;
    private final String identity;

    public MutualTlsDefaultRoleInitializer(Map<String, String> parameters)
    {
        for (String params : parameters.keySet())
        {
            if (!SUPPORTED_PARAMS.contains(params))
            {
                throw new ConfigurationException(String.format("Unsupported parameter %s for %s, supported parameters are %s", params, getClass().getSimpleName(), SUPPORTED_PARAMS));
            }
        }
        role = parameters.get(ROLE);
        identity = parameters.get(IDENTITY);
    }

    @Override
    public void createDefaultRole()
    {
        QueryProcessor.process(String.format("INSERT INTO %s.%s (role, is_superuser, can_login) " +
                                             "VALUES ('%s', true, true) USING TIMESTAMP 0",
                                             SchemaConstants.AUTH_KEYSPACE_NAME,
                                             AuthKeyspace.ROLES,
                                             escape(role)),
                               consistencyForRoleWrite(role));

        QueryProcessor.process(String.format("INSERT INTO %s.%s (identity, role) " +
                                             "VALUES ('%s', '%s') USING TIMESTAMP 0",
                                             SchemaConstants.AUTH_KEYSPACE_NAME,
                                             AuthKeyspace.IDENTITY_TO_ROLES,
                                             escape(identity),
                                             escape(role)),
                               consistencyForRoleWrite(role));

        logger.info("Created passwordless default superuser role '{}' with identity '{}'", role, identity);
    }

    @Override
    public String defaultRoleName()
    {
        return role;
    }

    @Override
    public void validateConfiguration() throws ConfigurationException
    {
        if (Strings.isNullOrEmpty(role))
            throw new ConfigurationException(String.format("%s requires a non-empty '%s' parameter",
                                                           getClass().getSimpleName(), ROLE));

        if (Strings.isNullOrEmpty(identity))
            throw new ConfigurationException(String.format("%s requires a non-empty '%s' parameter",
                                                           getClass().getSimpleName(), IDENTITY));

        // The role this creates has no password, so an authenticator which cannot authenticate by certificate
        // would leave a freshly bootstrapped cluster with no way to log in at all.
        IAuthenticator authenticator = DatabaseDescriptor.getAuthenticator();
        Set<IAuthenticator.AuthenticationMode> modes = authenticator.getSupportedAuthenticationModes();
        if (authenticator.requireAuthentication() && !modes.isEmpty() && !modes.contains(IAuthenticator.AuthenticationMode.MTLS))
        {
            throw new ConfigurationException(String.format("%s creates a role with no password, which %s cannot " +
                                                           "authenticate (supported modes: %s). Configure an " +
                                                           "authenticator supporting mutual TLS, such as %s.",
                                                           getClass().getSimpleName(),
                                                           authenticator.getClass().getSimpleName(),
                                                           modes,
                                                           MutualTlsAuthenticator.class.getSimpleName()));
        }
    }
}
