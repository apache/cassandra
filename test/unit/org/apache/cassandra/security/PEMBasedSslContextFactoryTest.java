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

import org.apache.commons.lang3.StringUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import io.netty.handler.ssl.OpenSsl;
import io.netty.handler.ssl.OpenSslContext;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslProvider;
import org.apache.cassandra.config.EncryptionOptions;
import org.apache.cassandra.config.ParameterizedClass;
import org.apache.cassandra.distributed.shared.WithProperties;

import static org.apache.cassandra.config.CassandraRelevantProperties.DISABLE_TCACTIVE_OPENSSL;
import static org.apache.cassandra.security.PEMBasedSslContextFactory.ConfigKey.ENCODED_CERTIFICATES;
import static org.apache.cassandra.security.PEMBasedSslContextFactory.ConfigKey.ENCODED_KEY;
import static org.apache.cassandra.security.PEMBasedSslContextFactory.ConfigKey.KEY_PASSWORD;

public class PEMBasedSslContextFactoryTest
{
    private static final String private_key =
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
    private static final String unencrypted_private_key =
    "-----BEGIN PRIVATE KEY-----\n" +
    "MIIEvQIBADANBgkqhkiG9w0BAQEFAASCBKcwggSjAgEAAoIBAQCOSZVf8dLj1xLw\n" +
    "efqjbogbAwhwRXd3tmEfQY1zHyudJF3XR1T0Xp26BKNvAYUxxZRDNg16M3prPRZv\n" +
    "wkhDJdE9NaN+BpZPJUavRsZOfGDq/CHN8j/2PPKrn3G/b065JjV3GZjfV/Sln047\n" +
    "DeZptaSyOg7ZN7F8qjGqxEcnw+szV/wTzhnjGjZMlcbOm4jFGQAn6xUOooQCGsoB\n" +
    "9FgxKB0nvNG3xVe/2eCNfbS4DabT3Y1wfQqZ62hOa5ZS0rwT+pw3tQs3zFFY1Sfi\n" +
    "G7qYbqZrKTheWIZGojVVm6mqH4yA2ofOe5N3RsBitCwU/D0dpnaG9Wcl98nmXueM\n" +
    "B6Rk04v7AgMBAAECggEAYnxIKjrFz/JkJ5MmiszM5HV698r9YB0aqHnFIHPoykIL\n" +
    "uiCjiumantDrFsCkosixULwvI/BRwbxstTpyrheU9psT6P1CONICVPvV8ylgJAYU\n" +
    "l+ofn56cEXKxVuICSWFLDH7pM1479g+IJJQAchbKQpqxAGTuMu3SpvJolfuj5srt\n" +
    "bM7/RYhJFLwDuvHNA3ivlogMneItP03+C25aaxstM+lBuBf68+n78zMgSvt6J/6Y\n" +
    "G2TOMKnxveMlG2qu9l2lAw/2i8daG/qre08nTH7wpRx0gZLZqNpe45exkrzticzF\n" +
    "FgWYjG2K2brX21jqHroFgMhdXF7zhhRgLoIeC0BrIQKBgQDCfGfWrJESKBbVai5u\n" +
    "7wqD9nlzjv6N6FXfTDOPXO1vz5frdvtLVWbs0SMPy+NglkaZK0iqHvb9mf2of8eC\n" +
    "0D5cmewjn7WCDBQMypIMYgT912ak/BBVuGXcxb6UgD+xARfSARo2C8NG1hfprw1W\n" +
    "ad14CjS5xhFMs44HpVYhI7iPYwKBgQC7SqVG/b37vZ7CINemdvoMujLvvYXDJJM8\n" +
    "N21LqNJfVXdukdH3T0xuLnh9Z/wPHjJDMF/9+1foxSEPHijtyz5P19EilNEC/3qw\n" +
    "fI19+VZoY0mdhPtXSGzy+rbTE2v71QgwFLizSos14Gr+eNiIjF7FYccK05++K/zk\n" +
    "cd8ZA3bwiQKBgQCl+HTFBs9mpz+VMOAfW2+l3hkXPNiPUc62mNkHZ05ZNNd44jjh\n" +
    "uSf0wSUiveR08MmevQlt5K7zDQ8jVKh2QjB15gVXAVxsdtJFeDnax2trFP9LnLBz\n" +
    "9sE2/qn9INU5wK0LUlWD+dXUBbCyg+jl7cJKRqtoPldVFYYHkFlIPqup8QKBgHXv\n" +
    "hyuw1FUVDkdHzwOvn70r8q8sNHKxMVWVwWkHIZGOi+pAQGrusD4hXRX6yKnsZdIR\n" +
    "QCD6iFy25R5T64nxlYdJaxPPid3NakB/7ckJnPOWseBSwMIxhQlr/nvjmve1Kba9\n" +
    "FaEwq4B9lGIxToiNe4/nBiM3JzvlDxX67nUdzWOhAoGAdFvriyvjshSJ4JHgIY9K\n" +
    "37BVB0VKMcFV2P8fLVWO5oyRtE1bJhU4QVpQmauABU4RGSojJ3NPIVH1wxmJeYtj\n" +
    "Q3b7EZaqI6ovna2eK2qtUx4WwxhRaXTT8xueBI2lgL6sBSTGG+K69ZOzGQzG/Mfr\n" +
    "RXKInnLInFD9JD94VqmMozo=\n" +
    "-----END PRIVATE KEY-----\n" +
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
    private static final String trusted_certificates =
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
    private final Map<String, Object> commonConfig = new HashMap<>();

    @Before
    public void setup()
    {
        commonConfig.put(ENCODED_CERTIFICATES.getKeyName(), trusted_certificates);
        commonConfig.put("require_client_auth", Boolean.FALSE);
        commonConfig.put("cipher_suites", Arrays.asList("TLS_RSA_WITH_AES_128_CBC_SHA"));
    }

    private void addKeyStoreOptions(Map<String, Object> config)
    {
        config.put(ENCODED_KEY.getKeyName(), private_key);
        config.put(KEY_PASSWORD.getKeyName(), "cassandra");
    }

    private void addUnencryptedKeyStoreOptions(Map<String, Object> config)
    {
        config.put(ENCODED_KEY.getKeyName(), unencrypted_private_key);
    }

    private void addFileBaseTrustStoreOptions(Map<String, Object> config)
    {
        config.put("truststore", "test/conf/cassandra_ssl_test.truststore.pem");
    }

    private void addFileBaseKeyStoreOptions(Map<String, Object> config)
    {
        config.put("keystore", "test/conf/cassandra_ssl_test.keystore.pem");
        config.put("keystore_password", "cassandra");
    }

    private void addFileBaseUnencryptedKeyStoreOptions(Map<String, Object> config)
    {
        config.put("keystore", "test/conf/cassandra_ssl_test.unencrypted_keystore.pem");
    }

    @Test
    public void getSslContextOpenSSL() throws IOException
    {
        ParameterizedClass sslContextFactory = new ParameterizedClass(PEMBasedSslContextFactory.class.getSimpleName()
        , new HashMap<>());
        EncryptionOptions options = new EncryptionOptions().withTrustStore("test/conf/cassandra_ssl_test.truststore.pem")
                                                           .withKeyStore("test/conf/cassandra_ssl_test.keystore.pem")
                                                           .withKeyStorePassword("cassandra")
                                                           .withRequireClientAuth(false)
                                                           .withCipherSuites("TLS_RSA_WITH_AES_128_CBC_SHA")
                                                           .withSslContextFactory(sslContextFactory);
        SslContext sslContext = SSLFactory.getOrCreateSslContext(options, true, ISslContextFactory.SocketType.SERVER, "test");
        Assert.assertNotNull(sslContext);
        if (OpenSsl.isAvailable())
            Assert.assertTrue(sslContext instanceof OpenSslContext);
        else
            Assert.assertTrue(sslContext instanceof SslContext);
    }

    @Test
    public void getSslContextOpenSSLOutboundKeystore() throws IOException
    {
        ParameterizedClass sslContextFactory = new ParameterizedClass(PEMBasedSslContextFactory.class.getSimpleName()
        , new HashMap<>());
        EncryptionOptions.ServerEncryptionOptions options = new EncryptionOptions.ServerEncryptionOptions().withTrustStore("test/conf/cassandra_ssl_test.truststore.pem")
                                                                                                           .withKeyStore("test/conf/cassandra_ssl_test.keystore.pem")
                                                                                                           .withKeyStorePassword("cassandra")
                                                                                                           .withOutboundKeystore("test/conf/cassandra_ssl_test.keystore.pem")
                                                                                                           .withOutboundKeystorePassword("cassandra")
                                                                                                           .withRequireClientAuth(false)
                                                                                                           .withCipherSuites("TLS_RSA_WITH_AES_128_CBC_SHA")
                                                                                                           .withSslContextFactory(sslContextFactory);
        SslContext sslContext = SSLFactory.getOrCreateSslContext(options, true, ISslContextFactory.SocketType.CLIENT, "test");
        Assert.assertNotNull(sslContext);
        if (OpenSsl.isAvailable())
            Assert.assertTrue(sslContext instanceof OpenSslContext);
        else
            Assert.assertTrue(sslContext instanceof SslContext);
    }

    @Test(expected = IOException.class)
    public void buildTrustManagerFactoryWithInvalidTruststoreFile() throws IOException
    {
        Map<String, Object> config = new HashMap<>();
        config.putAll(commonConfig);
        config.remove("encoded_certificates");
        config.put("truststore", "/this/is/probably/not/a/file/on/your/test/machine");

        DefaultSslContextFactory defaultSslContextFactoryImpl = new DefaultSslContextFactory(config);
        defaultSslContextFactoryImpl.keystoreContext.checkedExpiry = false;
        defaultSslContextFactoryImpl.buildTrustManagerFactory();
    }

    @Test
    public void buildTrustManagerFactoryHappyPath() throws IOException
    {
        Map<String, Object> config = new HashMap<>();
        config.putAll(commonConfig);

        PEMBasedSslContextFactory sslContextFactory = new PEMBasedSslContextFactory(config);
        sslContextFactory.keystoreContext.checkedExpiry = false;
        TrustManagerFactory trustManagerFactory = sslContextFactory.buildTrustManagerFactory();
        Assert.assertNotNull(trustManagerFactory);
    }

    @Test
    public void buildFileBasedTrustManagerFactoryHappyPath() throws IOException
    {
        Map<String, Object> config = new HashMap<>();
        config.putAll(commonConfig);
        config.remove(ENCODED_CERTIFICATES.getKeyName());
        addFileBaseTrustStoreOptions(config);

        PEMBasedSslContextFactory sslContextFactory = new PEMBasedSslContextFactory(config);
        sslContextFactory.keystoreContext.checkedExpiry = false;
        TrustManagerFactory trustManagerFactory = sslContextFactory.buildTrustManagerFactory();
        Assert.assertNotNull(trustManagerFactory);
    }

    @Test(expected = IOException.class)
    public void buildKeyManagerFactoryWithInvalidKeystoreFile() throws IOException
    {
        Map<String, Object> config = new HashMap<>();
        config.putAll(commonConfig);
        config.put("keystore", "/this/is/probably/not/a/file/on/your/test/machine");

        PEMBasedSslContextFactory sslContextFactory = new PEMBasedSslContextFactory(config);
        sslContextFactory.keystoreContext.checkedExpiry = false;
        sslContextFactory.buildKeyManagerFactory();
    }

    @Test(expected = IOException.class)
    public void buildKeyManagerFactoryWithBadPassword() throws IOException
    {
        Map<String, Object> config = new HashMap<>();
        config.putAll(commonConfig);
        addKeyStoreOptions(config);
        config.put("keystore_password", "HomeOfBadPasswords");

        DefaultSslContextFactory defaultSslContextFactoryImpl = new DefaultSslContextFactory(config);
        defaultSslContextFactoryImpl.buildKeyManagerFactory();
    }

    @Test
    public void buildKeyManagerFactoryHappyPath() throws IOException
    {
        Map<String, Object> config = new HashMap<>();
        config.putAll(commonConfig);

        PEMBasedSslContextFactory sslContextFactory1 = new PEMBasedSslContextFactory(config);
        // Make sure the exiry check didn't happen so far for the private key
        Assert.assertFalse(sslContextFactory1.keystoreContext.checkedExpiry);

        addKeyStoreOptions(config);
        PEMBasedSslContextFactory sslContextFactory2 = new PEMBasedSslContextFactory(config);
        // Trigger the private key loading. That will also check for expired private key
        sslContextFactory2.buildKeyManagerFactory();
        // Now we should have checked the private key's expiry
        Assert.assertTrue(sslContextFactory2.keystoreContext.checkedExpiry);

        // Make sure that new factory object preforms the fresh private key expiry check
        PEMBasedSslContextFactory sslContextFactory3 = new PEMBasedSslContextFactory(config);
        Assert.assertFalse(sslContextFactory3.keystoreContext.checkedExpiry);
        sslContextFactory3.buildKeyManagerFactory();
        Assert.assertTrue(sslContextFactory3.keystoreContext.checkedExpiry);
    }

    @Test(expected = IllegalArgumentException.class)
    public void buildKeyManagerFactoryWithConflictingPasswordConfigs() throws IOException
    {
        Map<String, Object> config = new HashMap<>();
        config.putAll(commonConfig);
        addKeyStoreOptions(config);
        config.put("keystore_password", config.get("keyPassword") + "-conflict");

        PEMBasedSslContextFactory sslContextFactory = new PEMBasedSslContextFactory(config);
        sslContextFactory.buildKeyManagerFactory();
    }

    @Test
    public void buildKeyManagerFactoryWithMatchingPasswordConfigs() throws IOException
    {
        Map<String, Object> config = new HashMap<>();
        config.putAll(commonConfig);
        addKeyStoreOptions(config);
        config.put("keystore_password", config.get("keyPassword"));

        PEMBasedSslContextFactory sslContextFactory = new PEMBasedSslContextFactory(config);
        sslContextFactory.buildKeyManagerFactory();
    }

    @Test
    public void buildFileBasedKeyManagerFactoryHappyPath() throws IOException
    {
        Map<String, Object> config = new HashMap<>();
        config.putAll(commonConfig);

        PEMBasedSslContextFactory sslContextFactory1 = new PEMBasedSslContextFactory(config);
        // Make sure the expiry check didn't happen so far for the private key
        Assert.assertFalse(sslContextFactory1.keystoreContext.checkedExpiry);

        addFileBaseKeyStoreOptions(config);
        PEMBasedSslContextFactory sslContextFactory2 = new PEMBasedSslContextFactory(config);
        // Trigger the private key loading. That will also check for expired private key
        sslContextFactory2.buildKeyManagerFactory();
        // Now we should have checked the private key's expiry
        Assert.assertTrue(sslContextFactory2.keystoreContext.checkedExpiry);

        // Make sure that new factory object preforms the fresh private key expiry check
        PEMBasedSslContextFactory sslContextFactory3 = new PEMBasedSslContextFactory(config);
        Assert.assertFalse(sslContextFactory3.keystoreContext.checkedExpiry);
        sslContextFactory3.buildKeyManagerFactory();
        Assert.assertTrue(sslContextFactory3.keystoreContext.checkedExpiry);
    }

    @Test
    public void buildKeyManagerFactoryWithUnencryptedKey() throws IOException
    {
        Map<String, Object> config = new HashMap<>();
        config.putAll(commonConfig);
        addUnencryptedKeyStoreOptions(config);

        Assert.assertTrue("Unencrypted Key test must not specify a key password",
                          StringUtils.isEmpty((String) config.get(KEY_PASSWORD.getKeyName())));

        PEMBasedSslContextFactory sslContextFactory = new PEMBasedSslContextFactory(config);
        sslContextFactory.buildKeyManagerFactory();
    }

    @Test
    public void buildKeyManagerFactoryWithFileBasedUnencryptedKey() throws IOException
    {
        Map<String, Object> config = new HashMap<>();
        config.putAll(commonConfig);
        addFileBaseUnencryptedKeyStoreOptions(config);

        Assert.assertTrue("Unencrypted Key test must not specify a key password",
                          StringUtils.isEmpty((String) config.get(KEY_PASSWORD.getKeyName())));

        PEMBasedSslContextFactory sslContextFactory = new PEMBasedSslContextFactory(config);
        sslContextFactory.buildKeyManagerFactory();
    }

    @Test
    public void testDisableOpenSslForInJvmDtests()
    {
        // The configuration name below is hard-coded intentionally to make sure we don't break the contract without
        // changing the documentation appropriately
        try (WithProperties properties = new WithProperties().set(DISABLE_TCACTIVE_OPENSSL, true))
        {
            Map<String, Object> config = new HashMap<>();
            config.putAll(commonConfig);

            PEMBasedSslContextFactory sslContextFactory = new PEMBasedSslContextFactory(config);
            Assert.assertEquals(SslProvider.JDK, sslContextFactory.getSslProvider());
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void testMultiplePrivateKeySources()
    {
        Map<String, Object> config = new HashMap<>();
        config.putAll(commonConfig);
        addUnencryptedKeyStoreOptions(config);

        // Check with a valid file path for the keystore
        addFileBaseUnencryptedKeyStoreOptions(config);
        new PEMBasedSslContextFactory(config);
    }

    @Test
    public void testMultiplePrivateKeySourcesWithInvalidKeystorePath()
    {
        Map<String, Object> config = new HashMap<>();
        config.putAll(commonConfig);
        addUnencryptedKeyStoreOptions(config);

        // Check with an invalid file path for the keystore
        config.put("keystore", "/path/to/nowhere");
        new PEMBasedSslContextFactory(config);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testMultipleTrustedCertificatesSources()
    {
        Map<String, Object> config = new HashMap<>();
        config.putAll(commonConfig);

        // Check with a valid file path for the truststore
        addFileBaseTrustStoreOptions(config);
        new PEMBasedSslContextFactory(config);
    }

    @Test
    public void testMultipleTrustedCertificatesSourcesWithInvalidTruststorePath()
    {
        Map<String, Object> config = new HashMap<>();
        config.putAll(commonConfig);

        // Check with an invalid file path for the truststore
        config.put("truststore", "/path/to/nowhere");
        new PEMBasedSslContextFactory(config);
    }
}
