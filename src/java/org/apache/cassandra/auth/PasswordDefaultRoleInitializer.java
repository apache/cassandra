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

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.schema.SchemaConstants;

import static org.apache.cassandra.auth.CassandraRoleManager.DEFAULT_SUPERUSER_NAME;
import static org.apache.cassandra.auth.CassandraRoleManager.DEFAULT_SUPERUSER_PASSWORD;
import static org.apache.cassandra.auth.CassandraRoleManager.consistencyForRoleWrite;
import static org.apache.cassandra.auth.CassandraRoleManager.escape;
import static org.apache.cassandra.auth.CassandraRoleManager.hashpw;

/**
 * Creates the default role with a password, so that it can be authenticated with
 * {@link PasswordAuthenticator}. This is the default {@link IDefaultRoleInitializer} and reproduces the
 * historical bootstrap behaviour of creating a {@code cassandra} superuser whose password is also
 * {@code cassandra}.
 *
 * Because that password is a well known constant, deployments which can authenticate by other means should
 * prefer an initializer which does not create a password at all, such as
 * {@link MutualTlsDefaultRoleInitializer}. Deployments which do use this initializer should rotate or drop the
 * created role before the native transport is reachable.
 */
public class PasswordDefaultRoleInitializer implements IDefaultRoleInitializer
{
    private static final Logger logger = LoggerFactory.getLogger(PasswordDefaultRoleInitializer.class);
    @VisibleForTesting
    public static final String ROLE = "default_role_initializer_role";
    @VisibleForTesting
    public static final String PASSWORD = "default_role_initializer_password";
    @VisibleForTesting
    public static final String PASSWORD_HASH = "default_role_initializer_password_hash";

    private static final Set<String> SUPPORTED_PARAMS = Set.of(ROLE, PASSWORD, PASSWORD_HASH);

    private final String role;
    private final String password;
    private final String passwordHash;
    private final Map<String, String> parameters;

    public static final PasswordDefaultRoleInitializer instance = new PasswordDefaultRoleInitializer();

    public PasswordDefaultRoleInitializer()
    {
        this(Map.of());
    }

    public PasswordDefaultRoleInitializer(Map<String, String> parameters)
    {
        for (String param: parameters.keySet())
        {
            if (!SUPPORTED_PARAMS.contains(param))
                throw new ConfigurationException(String.format("Unsupported parameter '%s' for %s, supported parameters are %s", param, getClass().getSimpleName(), SUPPORTED_PARAMS));
        }

        role = parameters.getOrDefault(ROLE, DEFAULT_SUPERUSER_NAME);
        passwordHash = parameters.get(PASSWORD_HASH);
        Map<String, String> santizedParams = new HashMap<>();
        santizedParams.put(ROLE, role);
        if (passwordHash != null)
            santizedParams.put(PASSWORD_HASH, passwordHash);
        if (passwordHash == null)
            password = parameters.getOrDefault(PASSWORD, DEFAULT_SUPERUSER_PASSWORD);
        else
            password = null;

        this.parameters = santizedParams;
    }

    @Override
    public void createDefaultRole()
    {
        QueryProcessor.process(String.format("INSERT INTO %s.%s (role, is_superuser, can_login, salted_hash) "
                                             +
                                             "VALUES ('%s', true, true, '%s') USING TIMESTAMP 0",
                                             SchemaConstants.AUTH_KEYSPACE_NAME,
                                             AuthKeyspace.ROLES,
                                             escape(role),
                                             escape(password == null ? passwordHash : hashpw(password))),
                               consistencyForRoleWrite(role));
        logger.info("Created default superuser role '{}'", role);
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
            throw new ConfigurationException(String.format("%s requires a non-empty %s parameter", getClass().getSimpleName(), ROLE));

        boolean specifiedPassword = !Strings.isNullOrEmpty(password);
        boolean specifiedPasswordHash = !Strings.isNullOrEmpty(passwordHash);

        if (!specifiedPassword && !specifiedPasswordHash)
            throw new ConfigurationException(String.format("There has to be one of %s, %s specified.", PASSWORD, PASSWORD_HASH));
        else if (specifiedPassword && specifiedPasswordHash)
            throw new ConfigurationException(String.format("Only one of %s, %s can be specified.", PASSWORD, PASSWORD_HASH));
    }

    @Override
    public Map<String, String> parameters()
    {
        return parameters;
    }
}
