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

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import com.google.common.annotations.VisibleForTesting;

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

    private static final String AUTH_PACKAGE = AuthConfig.class.getPackage().getName();

    private static boolean initialized;

    /**
     * Resets the initialized flag, enabling AuthConfig to be reconfigured multiple times within a single
     * test case.
     */
    @VisibleForTesting
    static void reset()
    {
        initialized = false;
    }

    public static void applyAuth()
    {
        // some tests need this
        if (initialized)
            return;

        initialized = true;

        Config conf = DatabaseDescriptor.getRawConfig();

        /* Authentication, authorization and role management backend, implementing IAuthenticator, I*Authorizer & IRoleManager */

        // Determine if authenticator_negotiation section was configured
        boolean negotiationConfigured = conf.authenticator_negotiation.default_authenticator != null
                                        || conf.authenticator_negotiation.enabled
                                        || !conf.authenticator_negotiation.authenticators.isEmpty();

        // Determine default authenticator based on configuration precedence
        IAuthenticator defaultAuthenticator;

        if (negotiationConfigured)
        {
            ParameterizedClass defaultAuthConfig = conf.authenticator_negotiation.default_authenticator;

            if (defaultAuthConfig == null)
                // authenticator_negotiation is configured but default_authenticator is missing - fail to start
                throw new ConfigurationException(
                    "authenticator_negotiation section requires default_authenticator to be specified", false);

            defaultAuthenticator = (IAuthenticator) authInstantiate(defaultAuthConfig)
                                                    .orElseThrow(() -> new ConfigurationException(
                                                    "Unable to load default_authenticator from authenticator_negotiation section: "
                                                    + conf.authenticator_negotiation.default_authenticator.class_name, false
                                                    ));
        }
        else
        {
            // Fall back to legacy authenticator config
            defaultAuthenticator = authInstantiate(conf.authenticator, AllowAllAuthenticator.class);
        }

        // the configuration options regarding credentials caching are only guaranteed to
        // work with PasswordAuthenticator, so log a message if some other authenticator
        // is in use and non-default values are detected
        if (!(defaultAuthenticator instanceof PasswordAuthenticator || defaultAuthenticator instanceof MutualTlsAuthenticator)
            && (conf.credentials_update_interval != null
                || conf.credentials_validity.toMilliseconds() != 2000
                || conf.credentials_cache_max_entries != 1000))
        {
            logger.info("Configuration options credentials_update_interval, credentials_validity and " +
                        "credentials_cache_max_entries may not be applicable for the configured authenticator ({})",
                        defaultAuthenticator.getClass().getName());
        }

        DatabaseDescriptor.setDefaultAuthenticator(defaultAuthenticator);

        List<IAuthenticator> negotiableAuthenticators = new ArrayList<>();

        if (negotiationConfigured)
        {
            logger.info("Authentication negotiation enabled: initializing authenticators");
            List<ParameterizedClass> authenticators = conf.authenticator_negotiation.authenticators;

            if (authenticators != null)
            {
                for (ParameterizedClass clazz: authenticators)
                {
                    // We generally can't instantiate multiple instances of an authenticator, so if the
                    // default is also in the list of negotiaable authenticators, just re-use the instance
                    // that we have.
                    // TODO - This comparison is potentially broken because depending on how the authenticator
                    //  is configured in YAML, this equals() may or may not work. The parameterized class created
                    //  from 'default_authenticator: PasswordAuthenticator' is different from that created from
                    //  'default_authenticator:\n\t- class_name: PasswordAuthenticator'. The ParameterizedClass
                    //  instance created by SnakeYAML will have an _empty_ parameters member with the first form,
                    //  and a _null_ parameters member with the second form. This causes ParameterizedClass.equals()
                    //  to return 'false' when those two ParameterizedClass objects are compared ... which is probably
                    //  incorrect. I'm not sure how to resolve this right now. For now the unit tests pass but this
                    //  does introduce the risk of trying to instantiate the same authenticator twice, and not all
                    //  authenticators can tolerate that.
                    //if (clazz.equals(conf.authenticator_negotiation.default_authenticator))
                    //{
                    //    negotiableAuthenticators.add(defaultAuthenticator);
                    //    continue;
                    //}

                    Optional<IAuthenticator> authenticator = authInstantiate(clazz);

                    if (authenticator.isEmpty())
                    {
                        logger.warn("Unable to instantiate configured authenticator {}", clazz.class_name);
                    }
                    else
                    {
                        negotiableAuthenticators.add(authenticator.get());
                    }
                }
            }

            logger.info("Configured negotiable authenticators {}", negotiableAuthenticators);
        }

        // Ensure default authenticator is available for negotiation (as lowest priority) so clients can
        // explicitly signal support for it, rather than falling back to it blindly when negotiation fails.
        if (!negotiableAuthenticators.contains(defaultAuthenticator))
        {
            logger.info("Adding default authenticator as least-preferred for negotiation: {}",
                        defaultAuthenticator.getClass().getName());
            negotiableAuthenticators.add(defaultAuthenticator);
        }

        DatabaseDescriptor.setNegotiableAuthenticators(negotiableAuthenticators);

        // authorizer

        IAuthorizer authorizer = authInstantiate(conf.authorizer, AllowAllAuthorizer.class);

        if (!defaultAuthenticator.requireAuthentication() && authorizer.requireAuthorization())
        {
            throw new ConfigurationException(authorizer.getClass().getName() + " has authorization enabled which requires " +
                                             defaultAuthenticator.getClass().getName() + " to enable authentication", false);
        }

        DatabaseDescriptor.setAuthorizer(authorizer);

        // role manager

        IRoleManager roleManager = authInstantiate(conf.role_manager, CassandraRoleManager.class);

        if (defaultAuthenticator instanceof PasswordAuthenticator && !(roleManager instanceof CassandraRoleManager))
            throw new ConfigurationException(defaultAuthenticator.getClass().getName() + " requires " + CassandraRoleManager.class.getName(), false);

        DatabaseDescriptor.setRoleManager(roleManager);

        // authenticator

        IInternodeAuthenticator internodeAuthenticator = authInstantiate(conf.internode_authenticator,
                                                                         AllowAllInternodeAuthenticator.class);
        DatabaseDescriptor.setInternodeAuthenticator(internodeAuthenticator);

        // network authorizer

        INetworkAuthorizer networkAuthorizer = authInstantiate(conf.network_authorizer, AllowAllNetworkAuthorizer.class);

        if (networkAuthorizer.requireAuthorization() && !defaultAuthenticator.requireAuthentication())
        {
            throw new ConfigurationException(conf.network_authorizer + " can't be used with " + conf.authenticator.class_name, false);
        }

        DatabaseDescriptor.setNetworkAuthorizer(networkAuthorizer);

        // cidr authorizer

        ICIDRAuthorizer cidrAuthorizer = authInstantiate(conf.cidr_authorizer, AllowAllCIDRAuthorizer.class);

        if (cidrAuthorizer.requireAuthorization() && !defaultAuthenticator.requireAuthentication())
        {
            throw new ConfigurationException(conf.cidr_authorizer + " can't be used with " + conf.authenticator, false);
        }

        DatabaseDescriptor.setCIDRAuthorizer(cidrAuthorizer);

        // Validate at last to have authenticator, authorizer, role-manager and internode-auth setup
        // in case these rely on each other.

        defaultAuthenticator.validateConfiguration();
        authorizer.validateConfiguration();
        roleManager.validateConfiguration();
        networkAuthorizer.validateConfiguration();
        cidrAuthorizer.validateConfiguration();
        DatabaseDescriptor.getInternodeAuthenticator().validateConfiguration();
    }

    private static <T> T authInstantiate(ParameterizedClass authCls, Class<T> defaultCls) {
        return (T) authInstantiate(authCls).orElseGet(() -> defaultAuthInstantiate(defaultCls));
    }

    private static <T> Optional<T> authInstantiate(ParameterizedClass authCls) {
        if (authCls != null && authCls.class_name != null)
        {
            return Optional.of(ParameterizedClass.newInstance(authCls, List.of("", AUTH_PACKAGE)));
        }

        return Optional.empty();
    }

    private static <T> T defaultAuthInstantiate(Class<T> defaultCls) {
        // for now, this has to stay and can not be replaced by ParameterizedClass.newInstance as above
        // due to that failing for simulator dtests. See CASSANDRA-20450 for more information.
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
