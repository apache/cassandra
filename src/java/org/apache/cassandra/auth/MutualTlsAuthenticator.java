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

import java.net.InetAddress;
import java.security.cert.Certificate;
import java.util.Arrays;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.helpers.MessageFormatter;

import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.ParameterizedClass;
import org.apache.cassandra.exceptions.AuthenticationException;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.utils.NoSpamLogger;

/*
 * Performs mTLS authentication for client connections by extracting identities from client certificate
 * and verifying them against the authorized identities in IdentityCache. IdentityCache is a loading cache that
 * refreshes values on timely basis.
 *
 * During a client connection, after SSL handshake, identity of certificate is extracted using the certificate validator
 * and is verified whether the value exists in the cache or not. If it exists access is granted, otherwise, the connection
 * is rejected.
 *
 * Authenticator & Certificate validator can be configured using cassandra.yaml, one can write their own mTLS certificate
 * validator and configure it in cassandra.yaml.Below is an example on how to configure validator.
 * note that this example uses SPIFFE based validator, It could be any other validator with any defined identifier format.
 *
 * Example:
 * authenticator:
 *   class_name : org.apache.cassandra.auth.MutualTlsAuthenticator
 *   parameters :
 *     validator_class_name: org.apache.cassandra.auth.SpiffeCertificateValidator
 */
public class MutualTlsAuthenticator implements IAuthenticator
{
    private static final Logger logger = LoggerFactory.getLogger(MutualTlsAuthenticator.class);
    private static final NoSpamLogger nospamLogger = NoSpamLogger.getLogger(logger, 1L, TimeUnit.MINUTES);
    private static final String VALIDATOR_CLASS_NAME = "validator_class_name";
    private static final String CACHE_NAME = "IdentitiesCache";
    private final IdentityCache identityCache = new IdentityCache();
    private final MutualTlsCertificateValidator certificateValidator;

    public MutualTlsAuthenticator(Map<String, String> parameters)
    {
        final String certificateValidatorClassName = parameters.get(VALIDATOR_CLASS_NAME);
        if (StringUtils.isEmpty(certificateValidatorClassName))
        {
            String message ="authenticator.parameters.validator_class_name is not set";
            logger.error(message);
            throw new ConfigurationException(message);
        }
        certificateValidator = ParameterizedClass.newInstance(new ParameterizedClass(certificateValidatorClassName),
                                                              Arrays.asList("", AuthConfig.class.getPackage().getName()));
        checkMtlsConfigurationIsValid(DatabaseDescriptor.getRawConfig());
        AuthCacheService.instance.register(identityCache);
    }

    @Override
    public boolean requireAuthentication()
    {
        return true;
    }

    @Override
    public Set<? extends IResource> protectedResources()
    {
        return ImmutableSet.of(DataResource.table(SchemaConstants.AUTH_KEYSPACE_NAME, AuthKeyspace.ROLES));
    }

    @Override
    public void validateConfiguration() throws ConfigurationException
    {

    }

    @Override
    public void setup()
    {
        identityCache.warm();
    }

    @Override
    public SaslNegotiator newSaslNegotiator(InetAddress clientAddress)
    {
        return null;
    }

    @Override
    public SaslNegotiator newSaslNegotiator(InetAddress clientAddress, Certificate[] certificates)
    {
        return new CertificateNegotiator(certificates);
    }

    @Override
    public AuthenticatedUser legacyAuthenticate(Map<String, String> credentials) throws AuthenticationException
    {
        throw new AuthenticationException("mTLS authentication is not supported for CassandraLoginModule");
    }

    @VisibleForTesting
    class CertificateNegotiator implements SaslNegotiator
    {
        private final Certificate[] clientCertificateChain;

        private CertificateNegotiator(final Certificate[] clientCertificateChain)
        {
            this.clientCertificateChain = clientCertificateChain;
        }

        @Override
        public byte[] evaluateResponse(byte[] clientResponse) throws AuthenticationException
        {
            return null;
        }

        @Override
        public boolean isComplete()
        {
            return true;
        }

        @Override
        public AuthenticatedUser getAuthenticatedUser() throws AuthenticationException
        {
            if (!certificateValidator.isValidCertificate(clientCertificateChain))
            {
                String message = "Invalid or not supported certificate";
                nospamLogger.error(message);
                throw new AuthenticationException(message);
            }

            final String identity = certificateValidator.identity(clientCertificateChain);
            if (StringUtils.isEmpty(identity))
            {
                String msg = "Unable to extract client identity from certificate for authentication";
                nospamLogger.error(msg);
                throw new AuthenticationException(msg);
            }
            String role = identityCache.get(identity);
            if (role == null)
            {
                String msg = "Certificate identity '{}' not authorized";
                nospamLogger.error(msg, identity);
                throw new AuthenticationException(MessageFormatter.format(msg, identity).getMessage());
            }
            return new AuthenticatedUser(role);
        }
    }

    private void checkMtlsConfigurationIsValid(Config config)
    {
        if (!config.client_encryption_options.getEnabled() || !config.client_encryption_options.require_client_auth)
        {
            String msg = "MutualTlsAuthenticator requires client_encryption_options.enabled to be true" +
                         " & client_encryption_options.require_client_auth to be true";
            logger.error(msg);
            throw new ConfigurationException(msg);
        }
    }

    static class IdentityCache extends AuthCache<String, String>
    {
        IdentityCache()
        {
            super(CACHE_NAME,
                  DatabaseDescriptor::setCredentialsValidity,
                  DatabaseDescriptor::getCredentialsValidity,
                  DatabaseDescriptor::setCredentialsUpdateInterval,
                  DatabaseDescriptor::getCredentialsUpdateInterval,
                  DatabaseDescriptor::setCredentialsCacheMaxEntries,
                  DatabaseDescriptor::getCredentialsCacheMaxEntries,
                  DatabaseDescriptor::setCredentialsCacheActiveUpdate,
                  DatabaseDescriptor::getCredentialsCacheActiveUpdate,
                  identity -> DatabaseDescriptor.getRoleManager().roleForIdentity(identity),
                  () -> DatabaseDescriptor.getRoleManager().authorizedIdentities(),
                  () -> true,
                  (k, v) -> v == null);
        }
    }
}
