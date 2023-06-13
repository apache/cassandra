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

import java.util.Arrays;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.ParameterizedClass;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.utils.FBUtilities;

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

        IAuthenticator authenticator = new AllowAllAuthenticator();

        /* Authentication, authorization and role management backend, implementing IAuthenticator, IAuthorizer & IRoleMapper*/
        if (conf.authenticator != null)
        {
            authenticator = ParameterizedClass.newInstance(conf.authenticator,
                                                           Arrays.asList("", AuthConfig.class.getPackage().getName()));
        }

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

        IAuthorizer authorizer = new AllowAllAuthorizer();

        if (conf.authorizer != null)
            authorizer = FBUtilities.newAuthorizer(conf.authorizer);

        if (!authenticator.requireAuthentication() && authorizer.requireAuthorization())
            throw new ConfigurationException(conf.authenticator.class_name + " can't be used with " + conf.authorizer, false);

        DatabaseDescriptor.setAuthorizer(authorizer);

        // role manager

        IRoleManager roleManager;
        if (conf.role_manager != null)
            roleManager = FBUtilities.newRoleManager(conf.role_manager);
        else
            roleManager = new CassandraRoleManager();

        if (authenticator instanceof PasswordAuthenticator && !(roleManager instanceof CassandraRoleManager))
            throw new ConfigurationException("CassandraRoleManager must be used with PasswordAuthenticator", false);

        DatabaseDescriptor.setRoleManager(roleManager);

        // authenticator

        if (conf.internode_authenticator != null)
        {
            DatabaseDescriptor.setInternodeAuthenticator(ParameterizedClass.newInstance(conf.internode_authenticator,
                                                                                        Arrays.asList("", AuthConfig.class.getPackage().getName())));
        }


        // network authorizer
        INetworkAuthorizer networkAuthorizer = FBUtilities.newNetworkAuthorizer(conf.network_authorizer);
        DatabaseDescriptor.setNetworkAuthorizer(networkAuthorizer);
        if (networkAuthorizer.requireAuthorization() && !authenticator.requireAuthentication())
        {
            throw new ConfigurationException(conf.network_authorizer + " can't be used with " + conf.authenticator.class_name, false);
        }

        // cidr authorizer
        ICIDRAuthorizer cidrAuthorizer = ICIDRAuthorizer.newCIDRAuthorizer(conf.cidr_authorizer);
        DatabaseDescriptor.setCIDRAuthorizer(cidrAuthorizer);
        if (cidrAuthorizer.requireAuthorization() && !authenticator.requireAuthentication())
        {
            throw new ConfigurationException(conf.cidr_authorizer + " can't be used with " + conf.authenticator, false);
        }

        // Validate at last to have authenticator, authorizer, role-manager and internode-auth setup
        // in case these rely on each other.

        authenticator.validateConfiguration();
        authorizer.validateConfiguration();
        roleManager.validateConfiguration();
        networkAuthorizer.validateConfiguration();
        cidrAuthorizer.validateConfiguration();
        DatabaseDescriptor.getInternodeAuthenticator().validateConfiguration();
    }
}
