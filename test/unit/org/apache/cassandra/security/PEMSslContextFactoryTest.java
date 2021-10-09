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

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import javax.net.ssl.TrustManagerFactory;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import io.netty.handler.ssl.OpenSsl;
import io.netty.handler.ssl.OpenSslContext;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslProvider;
import org.apache.cassandra.config.EncryptionOptions;

import static org.apache.cassandra.security.PEMBasedSslContextFactory.ConfigKey.ENCODED_CERTIFICATES;
import static org.apache.cassandra.security.PEMBasedSslContextFactory.ConfigKey.ENCODED_KEY;
import static org.apache.cassandra.security.PEMBasedSslContextFactory.ConfigKey.KEY_PASSWORD;
import static org.apache.cassandra.security.PEMBasedSslContextFactory.ConfigKey.TARGET_STORETYPE;

public class PEMSslContextFactoryTest
{
    private Map<String,Object> commonConfig = new HashMap<>();

    private static final String encoded_key =
    "-----BEGIN ENCRYPTED PRIVATE KEY-----\n" +
    "MIIE6jAcBgoqhkiG9w0BDAEDMA4ECOWqSzq5PBIdAgIFxQSCBMjXsCK30J0aT3J/\n" +
    "g5kcbmevTOY1pIhJGbf5QYYrMUPiuDK2ydxIbiPzoTE4/S+OkCeHhlqwn/YydpBl\n" +
    "xgjZZ1Z5rLJHO27d2biuESqanDiBVXYuVmHmaifRnFy0uUTFkStB5mjVZEiJgO29\n" +
    "L83hL60uWru71EVuVriC2WCfmZ/EXp6wyYszOqCFQ8Quk/rDO6XuaBl467MJbx5V\n" +
    "sucGT6E9XKNd9hB14/Izb2jtVM5kqKxoiHpz1na6yhEYJiE5D1uOonznWjBnjwB/\n" +
    "f0x+acpDfVDoJKTlRdz+DEcbOF7mb9lBVVjP6P/AAsmQzz6JKwHjvCrjYfQmyyN8\n" +
    "RI4KRQnWgm4L3dtByLqY8HFU4ogisCMCgI+hZQ+OKMz/hoRO540YGiPcTRY3EOUR\n" +
    "0bd5JxU6tCJDMTqKP9aSL2KmLoiLowdMkSPz7TCzLsZ2bGJemuCfpAs4XT1vXCHs\n" +
    "evrUbOnh8et1IA8mZ9auThfqsZtNagJLEXA6hWIKp1FfVL3Q49wvMKZt4eTn/zwU\n" +
    "tLL0m5yPo6/HAaOA3hbm/oghZS0dseshXl7PZrmZQtvYnIvjyoxEL7ducYDQCDP6\n" +
    "wZ7Nzyh1QZAauSS15hl3vLFRZCA9hWAVgwQAviTvhB342O0i9qI7TQkcHk+qcTPN\n" +
    "K+iGNbFZ8ma1izXNKSJ2PgI/QqFNIeJWvZrb9PhJRmaZVsTJ9fERm1ewpebZqkVv\n" +
    "zMqMhlKgx9ggAaSKgnGZkwXwB6GrSbbzUrwRCKm3FieD1QE4VVYevaadVUU75GG5\n" +
    "mrFKorJEH7kFZlic8OTjDksYnHbcgU36XZrGEXa2+ldVeGKL3CsXWciaQRcJg8yo\n" +
    "WQDjZpcutGI0eMJWCqUkv8pYZC2/wZU4htCve5nVJUU4t9uuo9ex7lnwlLWPvheQ\n" +
    "jUBMgzSRsZ+zwaIusvufAAxiKK/cJm4ubZSZPIjBbfd4U7VPxtirP4Accydu7EK6\n" +
    "eG/MZwtAMFNJxfxUR+/aYzJU/q1ePw7fWVHrpt58t/22CX2SJBEiUGmSmuyER4Ny\n" +
    "DPw6d6mhvPUS1jRhIZ9A81ht8MOX7VL5uVp307rt7o5vRpV1mo0iPiRHzGscMpJn\n" +
    "AP36klEAUNTf0uLTKZa7KHiwhn5iPmsCrENHkOKJjxhRrqHjD2wy3YHs3ow2voyY\n" +
    "Ua4Cids+c1hvRkNEDGNHm4+rKGFOGOsG/ZU7uj/6gflO4JXxNGiyTLflqMdWBvow\n" +
    "Zd7hk1zCaGAAn8nZ0hPweGxQ4Q30I9IBZrimGxB0vjiUqNio9+qMf33dCHFJEuut\n" +
    "ZGJMaUGVaPhXQcTy4uD5hzsPZV5xcsU4H3vBYyBcZgrusJ6OOgkuZQaU7p8rWQWr\n" +
    "bUEVbXuZdwEmxsCe7H/vEVv5+aA4sF4kWnMMFL7/LIYaiEzkTqdJlRv/KyJJgcAH\n" +
    "hg2BvR3XTAq8wiX0C98CdmTbsx2eyQdj5tCU606rEohFLKUxWkJYAKxCiUbxGGpI\n" +
    "RheVmxkef9ErxJiq7hsAsGrSJvMtJuDKIasnD14SOEwD/7jRAq6WdL9VLpxtzlOw\n" +
    "pWnIl8kUCO3WoaG9Jf+ZTIv2hnxJhaSzYrdXzGPNnaWKhBlwnXJRvQEdrIxZOimP\n" +
    "FujZhqbKUDbYAcqTkoQ=\n" +
    "-----END ENCRYPTED PRIVATE KEY-----\n" +
    "-----BEGIN CERTIFICATE-----\n" +
    "MIIDkTCCAnmgAwIBAgIETxH5JDANBgkqhkiG9w0BAQsFADB5MRAwDgYDVQQGEwdV\n" +
    "bmtub3duMRAwDgYDVQQIEwdVbmtub3duMRAwDgYDVQQHEwdVbmtub3duMRAwDgYD\n" +
    "VQQKEwdVbmtub3duMRQwEgYDVQQLDAtzc2xfdGVzdGluZzEZMBcGA1UEAxMQQXBh\n" +
    "Y2hlIENhc3NhbmRyYTAeFw0xNjAzMTgyMTI4MDJaFw0xNjA2MTYyMTI4MDJaMHkx\n" +
    "EDAOBgNVBAYTB1Vua25vd24xEDAOBgNVBAgTB1Vua25vd24xEDAOBgNVBAcTB1Vu\n" +
    "a25vd24xEDAOBgNVBAoTB1Vua25vd24xFDASBgNVBAsMC3NzbF90ZXN0aW5nMRkw\n" +
    "FwYDVQQDExBBcGFjaGUgQ2Fzc2FuZHJhMIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8A\n" +
    "MIIBCgKCAQEAjkmVX/HS49cS8Hn6o26IGwMIcEV3d7ZhH0GNcx8rnSRd10dU9F6d\n" +
    "ugSjbwGFMcWUQzYNejN6az0Wb8JIQyXRPTWjfgaWTyVGr0bGTnxg6vwhzfI/9jzy\n" +
    "q59xv29OuSY1dxmY31f0pZ9OOw3mabWksjoO2TexfKoxqsRHJ8PrM1f8E84Z4xo2\n" +
    "TJXGzpuIxRkAJ+sVDqKEAhrKAfRYMSgdJ7zRt8VXv9ngjX20uA2m092NcH0Kmeto\n" +
    "TmuWUtK8E/qcN7ULN8xRWNUn4hu6mG6mayk4XliGRqI1VZupqh+MgNqHznuTd0bA\n" +
    "YrQsFPw9HaZ2hvVnJffJ5l7njAekZNOL+wIDAQABoyEwHzAdBgNVHQ4EFgQUcdiD\n" +
    "N6aylI91kAd34Hl2AzWY51QwDQYJKoZIhvcNAQELBQADggEBAG9q29ilUgCWQP5v\n" +
    "iHkZHj10gXGEoMkdfrPBf8grC7dpUcaw1Qfku/DJ7kPvMALeEsmFDk/t78roeNbh\n" +
    "IYBLJlzI1HZN6VPtpWQGsqxltAy5XN9Xw9mQM/tu70ShgsodGmE1UoW6eE5+/GMv\n" +
    "6Fg+zLuICPvs2cFNmWUvukN5LW146tJSYCv0Q/rCPB3m9dNQ9pBxrzPUHXw4glwG\n" +
    "qGnGddXmOC+tSW5lDLLG1BRbKv4zxv3UlrtIjqlJtZb/sQMT6WtG2ihAz7SKOBHa\n" +
    "HOWUwuPTetWIuJCKP7P4mWWtmSmjLy+BFX5seNEngn3RzJ2L8uuTJQ/88OsqgGru\n" +
    "n3MVF9w=\n" +
    "-----END CERTIFICATE-----";

    private static final String encoded_certificates =
    "-----BEGIN CERTIFICATE-----\n" +
    "MIIDkTCCAnmgAwIBAgIETxH5JDANBgkqhkiG9w0BAQsFADB5MRAwDgYDVQQGEwdV\n" +
    "bmtub3duMRAwDgYDVQQIEwdVbmtub3duMRAwDgYDVQQHEwdVbmtub3duMRAwDgYD\n" +
    "VQQKEwdVbmtub3duMRQwEgYDVQQLDAtzc2xfdGVzdGluZzEZMBcGA1UEAxMQQXBh\n" +
    "Y2hlIENhc3NhbmRyYTAeFw0xNjAzMTgyMTI4MDJaFw0xNjA2MTYyMTI4MDJaMHkx\n" +
    "EDAOBgNVBAYTB1Vua25vd24xEDAOBgNVBAgTB1Vua25vd24xEDAOBgNVBAcTB1Vu\n" +
    "a25vd24xEDAOBgNVBAoTB1Vua25vd24xFDASBgNVBAsMC3NzbF90ZXN0aW5nMRkw\n" +
    "FwYDVQQDExBBcGFjaGUgQ2Fzc2FuZHJhMIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8A\n" +
    "MIIBCgKCAQEAjkmVX/HS49cS8Hn6o26IGwMIcEV3d7ZhH0GNcx8rnSRd10dU9F6d\n" +
    "ugSjbwGFMcWUQzYNejN6az0Wb8JIQyXRPTWjfgaWTyVGr0bGTnxg6vwhzfI/9jzy\n" +
    "q59xv29OuSY1dxmY31f0pZ9OOw3mabWksjoO2TexfKoxqsRHJ8PrM1f8E84Z4xo2\n" +
    "TJXGzpuIxRkAJ+sVDqKEAhrKAfRYMSgdJ7zRt8VXv9ngjX20uA2m092NcH0Kmeto\n" +
    "TmuWUtK8E/qcN7ULN8xRWNUn4hu6mG6mayk4XliGRqI1VZupqh+MgNqHznuTd0bA\n" +
    "YrQsFPw9HaZ2hvVnJffJ5l7njAekZNOL+wIDAQABoyEwHzAdBgNVHQ4EFgQUcdiD\n" +
    "N6aylI91kAd34Hl2AzWY51QwDQYJKoZIhvcNAQELBQADggEBAG9q29ilUgCWQP5v\n" +
    "iHkZHj10gXGEoMkdfrPBf8grC7dpUcaw1Qfku/DJ7kPvMALeEsmFDk/t78roeNbh\n" +
    "IYBLJlzI1HZN6VPtpWQGsqxltAy5XN9Xw9mQM/tu70ShgsodGmE1UoW6eE5+/GMv\n" +
    "6Fg+zLuICPvs2cFNmWUvukN5LW146tJSYCv0Q/rCPB3m9dNQ9pBxrzPUHXw4glwG\n" +
    "qGnGddXmOC+tSW5lDLLG1BRbKv4zxv3UlrtIjqlJtZb/sQMT6WtG2ihAz7SKOBHa\n" +
    "HOWUwuPTetWIuJCKP7P4mWWtmSmjLy+BFX5seNEngn3RzJ2L8uuTJQ/88OsqgGru\n" +
    "n3MVF9w=\n" +
    "-----END CERTIFICATE-----";

    @Before
    public void setup()
    {
        commonConfig.put(TARGET_STORETYPE.getKeyName(), "PKCS12");
        commonConfig.put(ENCODED_CERTIFICATES.getKeyName(), encoded_certificates);
        commonConfig.put("require_client_auth", Boolean.FALSE);
        commonConfig.put("cipher_suites", Arrays.asList("TLS_RSA_WITH_AES_128_CBC_SHA"));
    }

    private void addPEMKeyStoreOptions(Map<String,Object> config)
    {
        config.put(ENCODED_KEY.getKeyName(), encoded_key);
        config.put(KEY_PASSWORD.getKeyName(), "cassandra");
    }

    private void addFileBasePEMTrustStoreOptions(Map<String,Object> config)
    {
        config.put("truststore", "test/conf/cassandra_ssl_test.truststore.pem");
    }

    private void addFileBasePEMKeyStoreOptions(Map<String,Object> config)
    {
        config.put("keystore", "test/conf/cassandra_ssl_test.keystore.pem");
        config.put("keystore_password", "cassandra");
    }

    //TODO
    //@Test
    public void getSslContextOpenSSL() throws IOException
    {
        EncryptionOptions options = new EncryptionOptions().withTrustStore("test/conf/cassandra_ssl_test.truststore")
                                                           .withTrustStorePassword("cassandra")
                                                           .withKeyStore("test/conf/cassandra_ssl_test.keystore")
                                                           .withKeyStorePassword("cassandra")
                                                           .withRequireClientAuth(false)
                                                           .withCipherSuites("TLS_RSA_WITH_AES_128_CBC_SHA");
        SslContext sslContext = SSLFactory.getOrCreateSslContext(options, true, ISslContextFactory.SocketType.CLIENT);
        Assert.assertNotNull(sslContext);
        if (OpenSsl.isAvailable())
            Assert.assertTrue(sslContext instanceof OpenSslContext);
        else
            Assert.assertTrue(sslContext instanceof SslContext);
    }

    @Test(expected = IOException.class)
    public void buildTrustManagerFactoryWithInvalidTruststoreFile() throws IOException
    {
        Map<String,Object> config = new HashMap<>();
        config.putAll(commonConfig);
        config.remove("encoded_certificates");
        config.put("truststore", "/this/is/probably/not/a/file/on/your/test/machine");

        DefaultSslContextFactory defaultSslContextFactoryImpl = new DefaultSslContextFactory(config);
        defaultSslContextFactoryImpl.checkedExpiry = false;
        defaultSslContextFactoryImpl.buildTrustManagerFactory();
    }

    @Test
    public void buildTrustManagerFactoryWithBadPassword() throws IOException
    {
        Map<String,Object> config = new HashMap<>();
        config.putAll(commonConfig);
        config.put("truststore_password", "HomeOfBadPasswords");

        PEMBasedSslContextFactory sslContextFactory = new PEMBasedSslContextFactory(config);
        sslContextFactory.checkedExpiry = false;
        sslContextFactory.buildTrustManagerFactory();
    }

    @Test
    public void buildTrustManagerFactoryHappyPath() throws IOException
    {
        Map<String,Object> config = new HashMap<>();
        config.putAll(commonConfig);

        PEMBasedSslContextFactory sslContextFactory = new PEMBasedSslContextFactory(config);
        sslContextFactory.checkedExpiry = false;
        TrustManagerFactory trustManagerFactory = sslContextFactory.buildTrustManagerFactory();
        Assert.assertNotNull(trustManagerFactory);
    }

    @Test
    public void buildFileBasedTrustManagerFactoryHappyPath() throws IOException
    {
        Map<String,Object> config = new HashMap<>();
        config.putAll(commonConfig);
        config.remove("encoded_certificates");
        addFileBasePEMTrustStoreOptions(config);

        PEMBasedSslContextFactory sslContextFactory = new PEMBasedSslContextFactory(config);
        sslContextFactory.checkedExpiry = false;
        TrustManagerFactory trustManagerFactory = sslContextFactory.buildTrustManagerFactory();
        Assert.assertNotNull(trustManagerFactory);
    }

    @Test(expected = IOException.class)
    public void buildKeyManagerFactoryWithInvalidKeystoreFile() throws IOException
    {
        Map<String,Object> config = new HashMap<>();
        config.putAll(commonConfig);
        config.put("keystore", "/this/is/probably/not/a/file/on/your/test/machine");

        PEMBasedSslContextFactory sslContextFactory = new PEMBasedSslContextFactory(config);
        sslContextFactory.checkedExpiry = false;
        sslContextFactory.buildKeyManagerFactory();
    }

    @Test(expected = IOException.class)
    public void buildKeyManagerFactoryWithBadPassword() throws IOException
    {
        Map<String,Object> config = new HashMap<>();
        config.putAll(commonConfig);
        addPEMKeyStoreOptions(config);
        config.put("keystore_password", "HomeOfBadPasswords");

        DefaultSslContextFactory defaultSslContextFactoryImpl = new DefaultSslContextFactory(config);
        defaultSslContextFactoryImpl.buildKeyManagerFactory();
    }

    @Test
    public void buildKeyManagerFactoryHappyPath() throws IOException
    {
        Map<String,Object> config = new HashMap<>();
        config.putAll(commonConfig);

        PEMBasedSslContextFactory sslContextFactory1 = new PEMBasedSslContextFactory(config);
        // Make sure the exiry check didn't happen so far for the private key
        Assert.assertFalse(sslContextFactory1.checkedExpiry);

        addPEMKeyStoreOptions(config);
        PEMBasedSslContextFactory sslContextFactory2 = new PEMBasedSslContextFactory(config);
        // Trigger the private key loading. That will also check for expired private key
        sslContextFactory2.buildKeyManagerFactory();
        // Now we should have checked the private key's expiry
        Assert.assertTrue(sslContextFactory2.checkedExpiry);

        // Make sure that new factory object preforms the fresh private key expiry check
        PEMBasedSslContextFactory sslContextFactory3 = new PEMBasedSslContextFactory(config);
        Assert.assertFalse(sslContextFactory3.checkedExpiry);
        sslContextFactory3.buildKeyManagerFactory();
        Assert.assertTrue(sslContextFactory3.checkedExpiry);
    }

    @Test(expected = IllegalArgumentException.class)
    public void buildKeyManagerFactoryWithConflictingPasswordConfigs() throws IOException
    {
        Map<String,Object> config = new HashMap<>();
        config.putAll(commonConfig);
        addPEMKeyStoreOptions(config);
        config.put("keystore_password", config.get("keyPassword")+"-conflict");

        PEMBasedSslContextFactory sslContextFactory = new PEMBasedSslContextFactory(config);
        sslContextFactory.buildKeyManagerFactory();
    }

    @Test
    public void buildKeyManagerFactoryWithMatchingPasswordConfigs() throws IOException
    {
        Map<String,Object> config = new HashMap<>();
        config.putAll(commonConfig);
        addPEMKeyStoreOptions(config);
        config.put("keystore_password", config.get("keyPassword"));

        PEMBasedSslContextFactory sslContextFactory = new PEMBasedSslContextFactory(config);
        sslContextFactory.buildKeyManagerFactory();
    }

    @Test
    public void buildFileBasedKeyManagerFactoryHappyPath() throws IOException
    {
        Map<String,Object> config = new HashMap<>();
        config.putAll(commonConfig);

        PEMBasedSslContextFactory sslContextFactory1 = new PEMBasedSslContextFactory(config);
        // Make sure the exiry check didn't happen so far for the private key
        Assert.assertFalse(sslContextFactory1.checkedExpiry);

        addFileBasePEMKeyStoreOptions(config);
        PEMBasedSslContextFactory sslContextFactory2 = new PEMBasedSslContextFactory(config);
        // Trigger the private key loading. That will also check for expired private key
        sslContextFactory2.buildKeyManagerFactory();
        // Now we should have checked the private key's expiry
        Assert.assertTrue(sslContextFactory2.checkedExpiry);

        // Make sure that new factory object preforms the fresh private key expiry check
        PEMBasedSslContextFactory sslContextFactory3 = new PEMBasedSslContextFactory(config);
        Assert.assertFalse(sslContextFactory3.checkedExpiry);
        sslContextFactory3.buildKeyManagerFactory();
        Assert.assertTrue(sslContextFactory3.checkedExpiry);
    }

    //@Test
    public void testDisableOpenSslForInJvmDtests() {
        // The configuration name below is hard-coded intentionally to make sure we don't break the contract without
        // changing the documentation appropriately
        System.setProperty("cassandra.disable_tcactive_openssl","true");
        Map<String,Object> config = new HashMap<>();
        config.putAll(commonConfig);

        DefaultSslContextFactory defaultSslContextFactoryImpl = new DefaultSslContextFactory(config);
        Assert.assertEquals(SslProvider.JDK, defaultSslContextFactoryImpl.getSslProvider());
        System.clearProperty("cassandra.disable_tcactive_openssl");
    }
}
