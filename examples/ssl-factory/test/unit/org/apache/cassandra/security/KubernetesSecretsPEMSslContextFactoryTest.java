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

package org.apache.cassandra.security;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLException;
import javax.net.ssl.TrustManagerFactory;

import org.apache.commons.lang3.StringUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.cassandra.security.KubernetesSecretsPEMSslContextFactory.DEFAULT_PRIVATE_KEY_ENV_VAR_NAME;
import static org.apache.cassandra.security.KubernetesSecretsPEMSslContextFactory.DEFAULT_PRIVATE_KEY_PASSWORD_ENV_VAR_NAME;
import static org.apache.cassandra.security.KubernetesSecretsPEMSslContextFactory.PEMConfigKey.PRIVATE_KEY_PASSWORD_ENV_VAR;
import static org.apache.cassandra.security.KubernetesSecretsPEMSslContextFactory.PEMConfigKey.TRUSTED_CERTIFICATE_ENV_VAR;
import static org.apache.cassandra.security.KubernetesSecretsSslContextFactory.ConfigKeys.KEYSTORE_UPDATED_TIMESTAMP_PATH;
import static org.apache.cassandra.security.KubernetesSecretsSslContextFactory.ConfigKeys.TRUSTSTORE_UPDATED_TIMESTAMP_PATH;

public class KubernetesSecretsPEMSslContextFactoryTest
{
    private static final Logger logger = LoggerFactory.getLogger(KubernetesSecretsPEMSslContextFactoryTest.class);

    private static final String ENCRYPTED_PRIVATE_KEY_FILEPATH = "build/test/conf/cassandra_encrypted_private_key.pem";
    private static final String ENCRYPTED_PRIVATE_KEY_WITH_MULTIPLE_CERTS_IN_CERTCHAIN_FILEPATH = "build/test/conf" +
                                                                                                  "/cassandra_encrypted_private_key_multiplecerts.pem";
    private static final String UNENCRYPTED_PRIVATE_KEY_FILEPATH = "build/test/conf/cassandra_unencrypted_private_key.pem";
    private static final String TRUSTED_CERTIFICATES_FILEPATH = "build/test/conf/cassandra_trusted_certificates.pem";
    private final static String TRUSTSTORE_UPDATED_TIMESTAMP_FILEPATH = "build/test/conf/cassandra_truststore_last_updatedtime";
    private final static String KEYSTORE_UPDATED_TIMESTAMP_FILEPATH = "build/test/conf/cassandra_keystore_last_updatedtime";

    private static String private_key;
    private static String unencrypted_private_key;
    private static String trusted_certificates;

    private final Map<String, Object> commonConfig = new HashMap<>();

    @BeforeClass
    public static void prepare()
    {
        deleteFileIfExists(TRUSTSTORE_UPDATED_TIMESTAMP_FILEPATH);
        deleteFileIfExists(KEYSTORE_UPDATED_TIMESTAMP_FILEPATH);
        private_key = readFile(ENCRYPTED_PRIVATE_KEY_FILEPATH);
        unencrypted_private_key = readFile(UNENCRYPTED_PRIVATE_KEY_FILEPATH);
        trusted_certificates = readFile(TRUSTED_CERTIFICATES_FILEPATH);
    }

    private static void deleteFileIfExists(String filePath)
    {
        try
        {
            logger.info("Deleting the file {} to prepare for the tests", new File(filePath).getAbsolutePath());
            File file = new File(filePath);
            if (file.exists())
            {
                file.delete();
            }
        }
        catch (Exception e)
        {
            logger.warn("File {} could not be deleted.", filePath, e);
        }
    }

    private static String readFile(String file)
    {
        try
        {
            return new String(Files.readAllBytes(Paths.get(file)));
        }
        catch (Exception e)
        {
            logger.error("Unable to read the file {}. Without this tests in this file would fail.", file);
        }
        return null;
    }

    @Before
    public void setup()
    {
        commonConfig.put(TRUSTED_CERTIFICATE_ENV_VAR,
                         "MY_TRUSTED_CERTIFICATES");
        commonConfig.put("MY_TRUSTED_CERTIFICATES", trusted_certificates);
        commonConfig.put(TRUSTSTORE_UPDATED_TIMESTAMP_PATH, TRUSTSTORE_UPDATED_TIMESTAMP_FILEPATH);
        /*
         * In order to test with real 'env' variables comment out this line and set appropriate env variable. This is
         * done to avoid having a dependency on env in the unit test.
         */
        commonConfig.put("require_client_auth", Boolean.FALSE);
        commonConfig.put("cipher_suites", Arrays.asList("TLS_RSA_WITH_AES_128_CBC_SHA"));
    }

    private void addKeystoreOptions(Map<String, Object> config)
    {
        config.put(DEFAULT_PRIVATE_KEY_ENV_VAR_NAME, private_key);
        config.put(PRIVATE_KEY_PASSWORD_ENV_VAR, "MY_KEY_PASSWORD");
        config.put(KEYSTORE_UPDATED_TIMESTAMP_PATH, KEYSTORE_UPDATED_TIMESTAMP_FILEPATH);
        /*
         * In order to test with real 'env' variables comment out this line and set appropriate env variable. This is
         *  done to avoid having a dependency on env in the unit test.
         */
        config.put("MY_KEY_PASSWORD", "cassandra");
    }

    private void addUnencryptedKeystoreOptions(Map<String, Object> config)
    {
        config.put(DEFAULT_PRIVATE_KEY_ENV_VAR_NAME, unencrypted_private_key);
        config.put(KEYSTORE_UPDATED_TIMESTAMP_PATH, KEYSTORE_UPDATED_TIMESTAMP_FILEPATH);
        config.remove(DEFAULT_PRIVATE_KEY_PASSWORD_ENV_VAR_NAME);
        config.remove(PRIVATE_KEY_PASSWORD_ENV_VAR);
    }

    @Test(expected = SSLException.class)
    public void buildTrustManagerFactoryWithInvalidTrustedCertificates() throws IOException
    {
        Map<String, Object> config = new HashMap<>();
        config.putAll(commonConfig);
        config.put("MY_TRUSTED_CERTIFICATES", trusted_certificates.replaceAll("\\s", String.valueOf(System.nanoTime())));

        KubernetesSecretsPEMSslContextFactory kubernetesSecretsSslContextFactory =
        new KubernetesSecretsPEMSslContextFactoryForTestOnly(config);
        kubernetesSecretsSslContextFactory.trustStoreContext.checkedExpiry = false;
        kubernetesSecretsSslContextFactory.buildTrustManagerFactory();
    }

    @Test
    public void buildTrustManagerFactoryHappyPath() throws IOException
    {
        Map<String, Object> config = new HashMap<>();
        config.putAll(commonConfig);

        KubernetesSecretsPEMSslContextFactory kubernetesSecretsSslContextFactory = new KubernetesSecretsPEMSslContextFactoryForTestOnly(config);
        kubernetesSecretsSslContextFactory.trustStoreContext.checkedExpiry = false;
        TrustManagerFactory trustManagerFactory = kubernetesSecretsSslContextFactory.buildTrustManagerFactory();
        Assert.assertNotNull(trustManagerFactory);
    }

    @Test(expected = SSLException.class)
    public void buildKeyManagerFactoryWithInvalidPrivateKey() throws IOException
    {
        Map<String, Object> config = new HashMap<>();
        config.putAll(commonConfig);
        config.put(DEFAULT_PRIVATE_KEY_ENV_VAR_NAME, private_key.replaceAll("\\s", String.valueOf(System.nanoTime())));

        KubernetesSecretsPEMSslContextFactory kubernetesSecretsSslContextFactory =
        new KubernetesSecretsPEMSslContextFactoryForTestOnly(config);
        kubernetesSecretsSslContextFactory.keystoreContext.checkedExpiry = false;
        kubernetesSecretsSslContextFactory.buildKeyManagerFactory();
    }

    @Test(expected = IOException.class)
    public void buildKeyManagerFactoryWithBadPassword() throws IOException
    {
        Map<String, Object> config = new HashMap<>();
        config.putAll(commonConfig);
        config.put(DEFAULT_PRIVATE_KEY_ENV_VAR_NAME, private_key);
        config.put(DEFAULT_PRIVATE_KEY_PASSWORD_ENV_VAR_NAME, "HomeOfBadPasswords");

        KubernetesSecretsPEMSslContextFactory kubernetesSecretsSslContextFactory =
        new KubernetesSecretsPEMSslContextFactoryForTestOnly(config);
        kubernetesSecretsSslContextFactory.buildKeyManagerFactory();
    }

    @Test
    public void buildKeyManagerFactoryHappyPath() throws IOException
    {
        Map<String, Object> config = new HashMap<>();
        config.putAll(commonConfig);

        KubernetesSecretsPEMSslContextFactory kubernetesSecretsSslContextFactory1 = new KubernetesSecretsPEMSslContextFactoryForTestOnly(config);
        // Make sure the exiry check didn't happen so far for the private key
        Assert.assertFalse(kubernetesSecretsSslContextFactory1.checkedExpiry);

        addKeystoreOptions(config);
        KubernetesSecretsPEMSslContextFactory kubernetesSecretsSslContextFactory2 = new KubernetesSecretsPEMSslContextFactoryForTestOnly(config);
        // Trigger the private key loading. That will also check for expired private key
        kubernetesSecretsSslContextFactory2.buildKeyManagerFactory();
        // Now we should have checked the private key's expiry
        Assert.assertTrue(kubernetesSecretsSslContextFactory2.checkedExpiry);

        // Make sure that new factory object preforms the fresh private key expiry check

        KubernetesSecretsPEMSslContextFactory kubernetesSecretsSslContextFactory3 =
        new KubernetesSecretsPEMSslContextFactoryForTestOnly(config);
        Assert.assertFalse(kubernetesSecretsSslContextFactory3.checkedExpiry);
        kubernetesSecretsSslContextFactory3.buildKeyManagerFactory();
        Assert.assertTrue(kubernetesSecretsSslContextFactory3.checkedExpiry);
    }

    @Test
    public void buildKeyManagerFactoryWithMultipleCertsInCertChain() throws IOException
    {
        Map<String, Object> config = new HashMap<>();
        config.putAll(commonConfig);
        addKeystoreOptions(config);
        config.put(DEFAULT_PRIVATE_KEY_ENV_VAR_NAME, readFile(ENCRYPTED_PRIVATE_KEY_WITH_MULTIPLE_CERTS_IN_CERTCHAIN_FILEPATH));

        KubernetesSecretsPEMSslContextFactory kubernetesSecretsSslContextFactory2 = new KubernetesSecretsPEMSslContextFactoryForTestOnly(config);
        // Trigger the private key loading. That will also check for expired private key
        kubernetesSecretsSslContextFactory2.buildKeyManagerFactory();
    }

    @Test
    public void buildKeyManagerFactoryHappyPathForUnencryptedKey() throws IOException
    {
        Map<String, Object> config = new HashMap<>();
        config.putAll(commonConfig);

        KubernetesSecretsPEMSslContextFactory kubernetesSecretsSslContextFactory1 = new KubernetesSecretsPEMSslContextFactoryForTestOnly(config);
        // Make sure the exiry check didn't happen so far for the private key
        Assert.assertFalse(kubernetesSecretsSslContextFactory1.checkedExpiry);

        addUnencryptedKeystoreOptions(config);
        KubernetesSecretsPEMSslContextFactory kubernetesSecretsSslContextFactory2 = new KubernetesSecretsPEMSslContextFactoryForTestOnly(config);
        // Trigger the private key loading. That will also check for expired private key
        kubernetesSecretsSslContextFactory2.buildKeyManagerFactory();
        // Now we should have checked the private key's expiry
        Assert.assertTrue(kubernetesSecretsSslContextFactory2.checkedExpiry);

        // Make sure that new factory object preforms the fresh private key expiry check

        KubernetesSecretsPEMSslContextFactory kubernetesSecretsSslContextFactory3 =
        new KubernetesSecretsPEMSslContextFactoryForTestOnly(config);
        Assert.assertFalse(kubernetesSecretsSslContextFactory3.checkedExpiry);
        kubernetesSecretsSslContextFactory3.buildKeyManagerFactory();
        Assert.assertTrue(kubernetesSecretsSslContextFactory3.checkedExpiry);
    }

    @Test
    public void checkTruststoreUpdateReloading() throws IOException
    {
        Map<String, Object> config = new HashMap<>();
        config.putAll(commonConfig);
        addKeystoreOptions(config);

        KubernetesSecretsPEMSslContextFactory kubernetesSecretsSslContextFactory = new KubernetesSecretsPEMSslContextFactoryForTestOnly(config);
        kubernetesSecretsSslContextFactory.trustStoreContext.checkedExpiry = false;
        TrustManagerFactory trustManagerFactory = kubernetesSecretsSslContextFactory.buildTrustManagerFactory();
        Assert.assertNotNull(trustManagerFactory);
        Assert.assertFalse(kubernetesSecretsSslContextFactory.shouldReload());

        updateTimestampFile(config, TRUSTSTORE_UPDATED_TIMESTAMP_PATH);
        Assert.assertTrue(kubernetesSecretsSslContextFactory.shouldReload());

        config.remove(TRUSTSTORE_UPDATED_TIMESTAMP_PATH);
        Assert.assertFalse(kubernetesSecretsSslContextFactory.shouldReload());
    }

    @Test
    public void checkKeystoreUpdateReloading() throws IOException
    {
        Map<String, Object> config = new HashMap<>();
        config.putAll(commonConfig);
        addKeystoreOptions(config);

        KubernetesSecretsPEMSslContextFactory kubernetesSecretsSslContextFactory = new KubernetesSecretsPEMSslContextFactoryForTestOnly(config);
        kubernetesSecretsSslContextFactory.keystoreContext.checkedExpiry = false;
        KeyManagerFactory keyManagerFactory = kubernetesSecretsSslContextFactory.buildKeyManagerFactory();
        Assert.assertNotNull(keyManagerFactory);
        Assert.assertFalse(kubernetesSecretsSslContextFactory.shouldReload());

        updateTimestampFile(config, KEYSTORE_UPDATED_TIMESTAMP_PATH);
        Assert.assertTrue(kubernetesSecretsSslContextFactory.shouldReload());

        config.remove(KEYSTORE_UPDATED_TIMESTAMP_PATH);
        Assert.assertFalse(kubernetesSecretsSslContextFactory.shouldReload());
    }

    private void updateTimestampFile(Map<String, Object> config, String filePathKey)
    {
        String filePath = config.containsKey(filePathKey) ? config.get(filePathKey).toString() : null;
        try (OutputStream os = Files.newOutputStream(Paths.get(filePath)))
        {
            String timestamp = String.valueOf(System.nanoTime());
            os.write(timestamp.getBytes());
            logger.info("Successfully wrote to file {}", filePath);
        }
        catch (IOException e)
        {
            logger.warn("Failed to write to filePath {} from the mounted volume", filePath, e);
        }
    }

    private static class KubernetesSecretsPEMSslContextFactoryForTestOnly extends KubernetesSecretsPEMSslContextFactory
    {

        public KubernetesSecretsPEMSslContextFactoryForTestOnly()
        {
        }

        public KubernetesSecretsPEMSslContextFactoryForTestOnly(Map<String, Object> config)
        {
            super(config);
        }

        /*
         * This is overriden to first give priority to the input map configuration since we should not be setting env
         * variables from the unit tests. However, if the input map configuration doesn't have the value for the
         * given key then fallback to loading from the real environment variables.
         */
        @Override
        String getValueFromEnv(String envVarName, String defaultValue)
        {
            String envVarValue = parameters.get(envVarName) != null ? parameters.get(envVarName).toString() : null;
            if (StringUtils.isEmpty(envVarValue))
            {
                logger.info("Configuration doesn't have env variable {}. Will use parent's implementation", envVarName);
                return super.getValueFromEnv(envVarName, defaultValue);
            }
            else
            {
                logger.info("Configuration has environment variable {} with value {}. Will use that.",
                            envVarName, envVarValue);
                return envVarValue;
            }
        }
    }
}
