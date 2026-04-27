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
     * Normalized authenticator configuration that abstracts away the difference between
     * legacy single-authenticator config and negotiated multi-authenticator config.
     */
    private static class AuthenticatorConfig
    {
        final IAuthenticator defaultAuthenticator;
        final List<IAuthenticator> negotiableAuthenticators;
        final boolean requireAuthentication;
        final boolean isNegotiationEnabled;

        AuthenticatorConfig(IAuthenticator defaultAuthenticator,
                           List<IAuthenticator> negotiableAuthenticators,
                           boolean requireAuthentication,
                           boolean isNegotiationEnabled)
        {
            this.defaultAuthenticator = defaultAuthenticator;
            this.negotiableAuthenticators = negotiableAuthenticators;
            this.requireAuthentication = requireAuthentication;
            this.isNegotiationEnabled = isNegotiationEnabled;
        }
    }

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

        // Load and normalize authenticator configuration
        AuthenticatorConfig authConfig = loadAuthenticatorConfig(conf);

        // the configuration options regarding credentials caching are only guaranteed to
        // work with PasswordAuthenticator, so log a message if some other authenticator
        // is in use and non-default values are detected
        if (!(authConfig.defaultAuthenticator instanceof PasswordAuthenticator || authConfig.defaultAuthenticator instanceof MutualTlsAuthenticator)
            && (conf.credentials_update_interval != null
                || conf.credentials_validity.toMilliseconds() != 2000
                || conf.credentials_cache_max_entries != 1000))
        {
            logger.info("Configuration options credentials_update_interval, credentials_validity and " +
                        "credentials_cache_max_entries may not be applicable for the configured authenticator ({})",
                        authConfig.defaultAuthenticator.getClass().getName());
        }

        DatabaseDescriptor.setDefaultAuthenticator(authConfig.defaultAuthenticator);
        DatabaseDescriptor.setNegotiableAuthenticators(authConfig.negotiableAuthenticators);

        // Validate require_authentication setting if negotiation is configured
        if (authConfig.isNegotiationEnabled)
        {
            validateRequireAuthentication(authConfig);
        }

        // authorizer

        IAuthorizer authorizer = authInstantiate(conf.authorizer, AllowAllAuthorizer.class);
        validateAuthenticatorAuthorizerCompatibility(authConfig, authorizer);
        DatabaseDescriptor.setAuthorizer(authorizer);

        // role manager

        IRoleManager roleManager = authInstantiate(conf.role_manager, CassandraRoleManager.class);

        // PasswordAuthenticator requires CassandraRoleManager. Check if any negotiable authenticator is
        // a PasswordAuthenticator.
        boolean hasPasswordAuth = authConfig.negotiableAuthenticators.stream()
            .anyMatch(auth -> auth instanceof PasswordAuthenticator);

        if (hasPasswordAuth && !(roleManager instanceof CassandraRoleManager))
            throw new ConfigurationException("PasswordAuthenticator requires " + CassandraRoleManager.class.getName(), false);

        DatabaseDescriptor.setRoleManager(roleManager);

        // internode authenticator

        IInternodeAuthenticator internodeAuthenticator = authInstantiate(conf.internode_authenticator,
                                                                         AllowAllInternodeAuthenticator.class);
        DatabaseDescriptor.setInternodeAuthenticator(internodeAuthenticator);

        // network authorizer

        INetworkAuthorizer networkAuthorizer = authInstantiate(conf.network_authorizer, AllowAllNetworkAuthorizer.class);
        validateAuthenticatorNetworkAuthorizerCompatibility(authConfig, networkAuthorizer);
        DatabaseDescriptor.setNetworkAuthorizer(networkAuthorizer);

        // cidr authorizer

        ICIDRAuthorizer cidrAuthorizer = authInstantiate(conf.cidr_authorizer, AllowAllCIDRAuthorizer.class);
        validateAuthenticatorCIDRAuthorizerCompatibility(authConfig, cidrAuthorizer);
        DatabaseDescriptor.setCIDRAuthorizer(cidrAuthorizer);

        // Validate at last to have authenticator, authorizer, role-manager and internode-auth setup
        // in case these rely on each other.

        authConfig.defaultAuthenticator.validateConfiguration();
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

    /**
     * Validates the require_authentication setting when authenticator negotiation is configured. If
     * require_authentication is true, all authenticators must require authentication. If require_authentication is
     * false and non-authenticating authenticators are present, logs a warning and continues.
     */
    private static void validateRequireAuthentication(AuthenticatorConfig authConfig)
    {
        // Check all negotiable authenticators (includes default)
        for (IAuthenticator auth : authConfig.negotiableAuthenticators)
        {
            if (!auth.requireAuthentication())
            {
                if (authConfig.requireAuthentication)
                {
                    throw new ConfigurationException(
                        "require_authentication is true but authenticator doesn't require authentication: " 
                        + auth.getClass().getName(), false);
                }
                else
                {
                    logger.warn("require_authentication is false and authenticator doesn't require authentication: {}. " +
                               "This allows unauthenticated access. Set require_authentication: true to enforce authentication.",
                               auth.getClass().getName());
                }
            }
        }
    }

    /**
     * Validates compatibility between authenticators and authorizer when negotiation is configured.
     * If any authenticator doesn't require authentication and the authorizer requires authorization:
     * - require_authentication: true -> fail (strict mode)
     * - require_authentication: false -> warn (permissive mode for migration)
     */
    private static void validateAuthenticatorAuthorizerCompatibility(AuthenticatorConfig authConfig,
                                                                     IAuthorizer authorizer)
    {
        if (!authorizer.requireAuthorization())
            return;

        // Legacy mode: simple check
        if (!authConfig.isNegotiationEnabled)
        {
            if (!authConfig.defaultAuthenticator.requireAuthentication())
            {
                throw new ConfigurationException(authorizer.getClass().getName() + " has authorization enabled which requires " +
                                               authConfig.defaultAuthenticator.getClass().getName() + " to enable authentication", false);
            }
            return;
        }
        
        // Negotiation mode: check all authenticators
        validateAuthorizerCompatibility(authConfig, authorizer.getClass().getName(), 
                                       "limited access based on 'anonymous' role permissions");
    }

    /**
     * Validates compatibility between authenticators and network authorizer when negotiation is configured.
     */
    private static void validateAuthenticatorNetworkAuthorizerCompatibility(AuthenticatorConfig authConfig,
                                                                            INetworkAuthorizer networkAuthorizer)
    {
        if (!networkAuthorizer.requireAuthorization())
            return;

        // Legacy mode: simple check
        if (!authConfig.isNegotiationEnabled)
        {
            if (!authConfig.defaultAuthenticator.requireAuthentication())
            {
                throw new ConfigurationException(networkAuthorizer.getClass().getName() + " can't be used with " + 
                                               authConfig.defaultAuthenticator.getClass().getName(), false);
            }
            return;
        }
        
        // Negotiation mode: check all authenticators
        validateAuthorizerCompatibility(authConfig, networkAuthorizer.getClass().getName(), 
                                       "limited network access");
    }

    /**
     * Validates compatibility between authenticators and CIDR authorizer when negotiation is configured.
     */
    private static void validateAuthenticatorCIDRAuthorizerCompatibility(AuthenticatorConfig authConfig,
                                                                         ICIDRAuthorizer cidrAuthorizer)
    {
        if (!cidrAuthorizer.requireAuthorization())
            return;

        // Legacy mode: simple check
        if (!authConfig.isNegotiationEnabled)
        {
            if (!authConfig.defaultAuthenticator.requireAuthentication())
            {
                throw new ConfigurationException(cidrAuthorizer.getClass().getName() + " can't be used with " + 
                                               authConfig.defaultAuthenticator.getClass().getName(), false);
            }
            return;
        }
        
        // Negotiation mode: check all authenticators
        validateAuthorizerCompatibility(authConfig, cidrAuthorizer.getClass().getName(), 
                                       "limited CIDR-based access");
    }

    /**
     * Common validation logic for authorizer compatibility.
     * Checks if any authenticator doesn't require authentication when an authorizer requires authorization.
     */
    private static void validateAuthorizerCompatibility(AuthenticatorConfig authConfig,
                                                        String authorizerName,
                                                        String accessDescription)
    {
        boolean hasNonAuthenticating = authConfig.negotiableAuthenticators.stream()
            .anyMatch(auth -> !auth.requireAuthentication());
        
        if (hasNonAuthenticating)
        {
            if (authConfig.requireAuthentication)
            {
                throw new ConfigurationException(
                    "require_authentication is true but some negotiable authenticators don't require authentication. " +
                    "This is incompatible with " + authorizerName + " which requires authorization.", false);
            }
            else
            {
                logger.warn("{} requires authorization but some negotiable authenticators don't require authentication. " +
                           "Unauthenticated clients will have {}. " +
                           "Set require_authentication: true to enforce authentication.",
                           authorizerName, accessDescription);
            }
        }
    }

    /**
     * Loads and normalizes authenticator configuration from either legacy or negotiation config.
     * Returns a normalized structure containing default authenticator, negotiable authenticators list,
     * and configuration flags.
     */
    private static AuthenticatorConfig loadAuthenticatorConfig(Config conf)
    {
        // Determine if authenticator_negotiation was configured
        boolean negotiationConfigured = conf.authenticator_negotiation.enabled;

        // Determine default authenticator based on configuration precedence
        IAuthenticator defaultAuthenticator;

        if (negotiationConfigured)
        {
            ParameterizedClass defaultAuthenticatorConfig = conf.authenticator_negotiation.default_authenticator;

            if (defaultAuthenticatorConfig == null)
                // authenticator_negotiation is configured but default_authenticator is missing - fail to start
                throw new ConfigurationException(
                    "authenticator_negotiation section requires default_authenticator to be specified", false);

            defaultAuthenticator = (IAuthenticator) authInstantiate(defaultAuthenticatorConfig)
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
                    // default is also in the list of negotiable authenticators, just re-use the instance
                    // that we have.
                    // TODO - This comparison is potentially broken because depending on how the authenticator
                    //  is configured in YAML, this equals() may or may not work. The parameterized class created
                    //  from 'default_authenticator: PasswordAuthenticator' or
                    //  'default_authenticator:\n\tclass_name: PasswordAuthenticator'
                    //  is different from that created from
                    //  'default_authenticator:\n\t- class_name: PasswordAuthenticator'.
                    //  The latter, inadvertantly, attempts to set default_authenticator to a list (indicated by the '-').
                    //  Since default_authenticator isn't a list, SnakeYAML grabs the first list element and constructs
                    //  the ParameterizedClass instance using the default constructor and reflection, which results in
                    //  'parameters' being null instead of being an empty map which is what the first two forms will
                    //  create. This causes ParameterizedClass.equals() to return 'false' when a ParameterizedClass
                    //  generated from the 'authenticators' list is compared to the default_authenticator if the default
                    //  authenticator was specified using that third form (list style). This results in the default
                    //  authenticator being placed at the end of the authenticators list instead of in its
                    //  configured order, which can lead to the negotiator picking a weaker authenticator than the order
                    //  of authenticators in the configuration indicates. I'm not sure how to resolve this right now.
                    //  One possibility is to 'fix' the equals() and hashCode() methods in ParameterizedClass to treat
                    //  null and empty 'parameters' the same (by coercing null to empty).
                    //  https://issues.apache.org/jira/browse/CASSANDRA-21238
                    if (clazz.equals(conf.authenticator_negotiation.default_authenticator))
                    {
                        negotiableAuthenticators.add(defaultAuthenticator);
                        continue;
                    }

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

        return new AuthenticatorConfig(
            defaultAuthenticator,
            negotiableAuthenticators,
            conf.authenticator_negotiation.require_authentication,
            negotiationConfigured
        );
    }
}
