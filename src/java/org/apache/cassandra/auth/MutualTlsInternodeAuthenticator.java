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

import java.io.InputStream;
import java.net.InetAddress;
import java.nio.file.Files;
import java.nio.file.Paths; // checkstyle: permit this import
import java.security.KeyStore;
import java.security.cert.Certificate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Enumeration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.EncryptionOptions;
import org.apache.cassandra.config.ParameterizedClass;
import org.apache.cassandra.exceptions.AuthenticationException;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.utils.NoSpamLogger;

/*
 * Performs mTLS authentication for internode connections by extracting identities from the certificates of incoming
 * connection and verifying them against a list of authorized peers. Authorized peers can be configured in
 * trusted_peer_identities in cassandra yaml, otherwise authenticator trusts connections from peers which has the same
 * identity as the one that the node uses for making outbound connections.
 *
 * Optionally cassandra can validate the identity extracted from outbound keystore with node_identity that is configured
 * in cassandra.yaml to avoid any configuration errors.
 *
 * Authenticator & Certificate validator can be configured using cassandra.yaml, operators can write their own mTLS
 * certificate validator and configure it in cassandra.yaml.Below is an example on how to configure validator.
 * Note that this example uses SPIFFE based validator, it could be any other validator with any defined identifier format.
 *
 * internode_authenticator:
 *   class_name : org.apache.cassandra.auth.AllowAllInternodeAuthenticator
 *   parameters :
 *     validator_class_name: org.apache.cassandra.auth.SpiffeCertificateValidator
 *     trusted_peer_identities: "spiffe1,spiffe2"
 *     node_identity: "spiffe1"
 */
public class MutualTlsInternodeAuthenticator implements IInternodeAuthenticator
{
    private static final String VALIDATOR_CLASS_NAME = "validator_class_name";
    private static final String TRUSTED_PEER_IDENTITIES = "trusted_peer_identities";
    private static final String NODE_IDENTITY = "node_identity";
    private final Logger logger = LoggerFactory.getLogger(this.getClass());
    private final NoSpamLogger noSpamLogger = NoSpamLogger.getLogger(logger, 30L, TimeUnit.SECONDS);
    private final MutualTlsCertificateValidator certificateValidator;
    private final List<String> trustedIdentities;

    public MutualTlsInternodeAuthenticator(Map<String, String> parameters)
    {
        String certificateValidatorClassName = parameters.get(VALIDATOR_CLASS_NAME);
        if (StringUtils.isEmpty(certificateValidatorClassName))
        {
            String message = "internode_authenticator.parameters.validator_class_name is not set";
            logger.error(message);
            throw new ConfigurationException(message);
        }

        certificateValidator = ParameterizedClass.newInstance(new ParameterizedClass(certificateValidatorClassName),
                                                              Arrays.asList("", AuthConfig.class.getPackage().getName()));
        Config config = DatabaseDescriptor.getRawConfig();
        checkInternodeMtlsConfigurationIsValid(config);

        if (parameters.containsKey(TRUSTED_PEER_IDENTITIES))
        {
            // If trusted_peer_identities identities is configured in cassandra.yaml trust only those identities
            trustedIdentities = Arrays.stream(parameters.get(TRUSTED_PEER_IDENTITIES).split(","))
                                      .collect(Collectors.toList());
        }
        else
        {
            // Otherwise, trust the identities extracted from outbound keystore which is the identity that the node uses
            // for making outbound connections.
            trustedIdentities = getIdentitiesFromKeyStore(config.server_encryption_options.outbound_keystore,
                                                          config.server_encryption_options.outbound_keystore_password,
                                                          config.server_encryption_options.store_type);
            // optionally, if node_identity is configured in the yaml, validate the identity extracted from outbound
            // keystore to avoid any configuration errors
            if(parameters.containsKey(NODE_IDENTITY))
            {
                String nodeIdentity = parameters.get(NODE_IDENTITY);
                if(!trustedIdentities.contains(nodeIdentity))
                {
                    throw new ConfigurationException("Configured node identity is not matching identity extracted" +
                                                     "from the keystore");
                }
                trustedIdentities.retainAll(Collections.singleton(nodeIdentity));
            }
        }

        if (!trustedIdentities.isEmpty())
        {
            logger.info("Initializing internode authenticator with identities {}", trustedIdentities);
        }
        else
        {
            String message = String.format("No identity was extracted from the outbound keystore '%s'", config.server_encryption_options.outbound_keystore);
            logger.info(message);
            throw new ConfigurationException(message);
        }
    }

    @Override
    public boolean authenticate(InetAddress remoteAddress, int remotePort)
    {
        throw new UnsupportedOperationException("mTLS Authenticator only supports certificate based authenticate method");
    }

    @Override
    public boolean authenticate(InetAddress remoteAddress, int remotePort, Certificate[] certificates, InternodeConnectionDirection connectionType)
    {
        return authenticateInternodeWithMtls(remoteAddress, remotePort, certificates, connectionType);
    }


    @Override
    public void validateConfiguration() throws ConfigurationException
    {

    }

    protected boolean authenticateInternodeWithMtls(InetAddress remoteAddress, int remotePort, Certificate[] certificates,
                                                    IInternodeAuthenticator.InternodeConnectionDirection connectionType)
    {
        if (connectionType == IInternodeAuthenticator.InternodeConnectionDirection.INBOUND)
        {
            String identity = certificateValidator.identity(certificates);
            if (!certificateValidator.isValidCertificate(certificates))
            {
                noSpamLogger.error("Not a valid certificate from {}:{} with identity '{}'", remoteAddress, remotePort, identity);
                return false;
            }

            if(!trustedIdentities.contains(identity))
            {
                noSpamLogger.error("Unable to authenticate user {}", identity);
                return false;
            }
            return true;
        }
        // Outbound connections don't need to be authenticated again in certificate based connections. SSL handshake
        // makes sure that we are talking to valid server by checking root certificates of the server in the
        // truststore of the client.
        return true;
    }

    @VisibleForTesting
    List<String> getIdentitiesFromKeyStore(final String outboundKeyStorePath,
                                           final String outboundKeyStorePassword,
                                           final String storeType)
    {
        final List<String> allUsers = new ArrayList<>();
        try (InputStream ksf = Files.newInputStream(Paths.get(outboundKeyStorePath)))
        {
            final KeyStore ks = KeyStore.getInstance(storeType);
            ks.load(ksf, outboundKeyStorePassword.toCharArray());
            Enumeration<String> enumeration = ks.aliases();
            while (enumeration.hasMoreElements())
            {
                String alias = enumeration.nextElement();
                Certificate[] chain = ks.getCertificateChain(alias);
                if (chain == null)
                {
                    logger.warn("Full chain/private key is not present in the keystore for certificate {}", alias);
                    continue;
                }
                try
                {
                    allUsers.add(certificateValidator.identity(chain));
                }
                catch (AuthenticationException e)
                {
                    // When identity cannot be extracted, this exception is thrown
                    // Ignore it, since only few certificates might contain identity
                }
            }
        }
        catch (Exception e)
        {
            logger.error("Failed to get identities from outbound_keystore {}", outboundKeyStorePath, e);
        }
        return allUsers;
    }

    private void checkInternodeMtlsConfigurationIsValid(Config config)
    {
        if (config.server_encryption_options.internode_encryption == EncryptionOptions.ServerEncryptionOptions.InternodeEncryption.none
            || !config.server_encryption_options.require_client_auth)
        {
            String msg = "MutualTlsInternodeAuthenticator requires server_encryption_options.internode_encryption to be enabled" +
                         " & server_encryption_options.require_client_auth to be true";
            logger.error(msg);
            throw new ConfigurationException(msg);
        }
    }
}
