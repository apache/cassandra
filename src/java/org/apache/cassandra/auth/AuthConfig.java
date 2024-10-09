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

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.ParameterizedClass;
import org.apache.cassandra.exceptions.ConfigurationException;

/**
 * Only purpose is to Initialize authentication/authorization via {@link #applyAuth()}.
 * This is in this separate class as it implicitly initializes schema stuff (via classes referenced in here).
 */
public final class AuthConfig
{
    private static final Logger logger = LoggerFactory.getLogger(AuthConfig.class);

    private static boolean initialized;

    public static void applyAuth()
    {
        // some tests need this
        if (initialized)
            return;

        initialized = true;

        Config conf = DatabaseDescriptor.getRawConfig();


        /* Authentication, authorization and role management backend, implementing IAuthenticator, I*Authorizer & IRoleManager */

        IAuthenticator authenticator = authInstantiate(conf.authenticator, AllowAllAuthenticator.class);

        // the configuration options regarding credentials caching are only guaranteed to
        // work with PasswordAuthenticator, so log a message if some other authenticator
        // is in use and non-default values are detected
        if (!(authenticator instanceof PasswordAuthenticator || authenticator instanceof MutualTlsAuthenticator)
            && (conf.credentials_update_interval != null
                || conf.credentials_validity.toMilliseconds() != 2000
                || conf.credentials_cache_max_entries != 1000))
        {
            logger.info("Configuration options credentials_update_interval, credentials_validity and " +
                        "credentials_cache_max_entries may not be applicable for the configured authenticator ({})",
                        authenticator.getClass().getName());
        }

        DatabaseDescriptor.setAuthenticator(authenticator);

        // authorizer

        IAuthorizer authorizer = authInstantiate(conf.authorizer, AllowAllAuthorizer.class);

        if (!authenticator.requireAuthentication() && authorizer.requireAuthorization())
            throw new ConfigurationException(conf.authenticator.class_name + " can't be used with " + conf.authorizer, false);

        DatabaseDescriptor.setAuthorizer(authorizer);

        // role manager

        IRoleManager roleManager = authInstantiate(conf.role_manager, CassandraRoleManager.class);

        if (authenticator instanceof PasswordAuthenticator && !(roleManager instanceof CassandraRoleManager))
            throw new ConfigurationException("CassandraRoleManager must be used with PasswordAuthenticator", false);

        DatabaseDescriptor.setRoleManager(roleManager);

        // authenticator

        IInternodeAuthenticator internodeAuthenticator = authInstantiate(conf.internode_authenticator,
                                                                         AllowAllInternodeAuthenticator.class);
        DatabaseDescriptor.setInternodeAuthenticator(internodeAuthenticator);

        // network authorizer

        INetworkAuthorizer networkAuthorizer = authInstantiate(conf.network_authorizer, AllowAllNetworkAuthorizer.class);

        if (networkAuthorizer.requireAuthorization() && !authenticator.requireAuthentication())
        {
            throw new ConfigurationException(conf.network_authorizer + " can't be used with " + conf.authenticator.class_name, false);
        }

        DatabaseDescriptor.setNetworkAuthorizer(networkAuthorizer);

        // cidr authorizer

        ICIDRAuthorizer cidrAuthorizer = authInstantiate(conf.cidr_authorizer, AllowAllCIDRAuthorizer.class);

        if (cidrAuthorizer.requireAuthorization() && !authenticator.requireAuthentication())
        {
            throw new ConfigurationException(conf.cidr_authorizer + " can't be used with " + conf.authenticator, false);
        }

        DatabaseDescriptor.setCIDRAuthorizer(cidrAuthorizer);

        // Validate at last to have authenticator, authorizer, role-manager and internode-auth setup
        // in case these rely on each other.

        authenticator.validateConfiguration();
        authorizer.validateConfiguration();
        roleManager.validateConfiguration();
        networkAuthorizer.validateConfiguration();
        cidrAuthorizer.validateConfiguration();
        DatabaseDescriptor.getInternodeAuthenticator().validateConfiguration();
    }

    private static <T> T authInstantiate(ParameterizedClass authCls, Class<T> defaultCls) {
        if (authCls != null && authCls.class_name != null)
        {
            String authPackage = AuthConfig.class.getPackage().getName();
            return ParameterizedClass.newInstance(authCls, List.of("", authPackage));
        }

        try
        {
            return defaultCls.newInstance();
        }
        catch (InstantiationException | IllegalAccessException  e)
        {
            throw new ConfigurationException("Failed to instantiate " + defaultCls.getName(), e);
        }
    }
}
