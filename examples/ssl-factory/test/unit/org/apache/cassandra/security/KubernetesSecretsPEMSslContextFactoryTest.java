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
    private final static String truststoreUpdatedTimestampFilepath = "build/test/conf/cassandra_truststore_last_updatedtime";
    private final static String keystoreUpdatedTimestampFilepath = "build/test/conf/cassandra_keystore_last_updatedtime";
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
    private Map<String, Object> commonConfig = new HashMap<>();

    @BeforeClass
    public static void prepare()
    {
        deleteFileIfExists(truststoreUpdatedTimestampFilepath);
        deleteFileIfExists(keystoreUpdatedTimestampFilepath);
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

    @Before
    public void setup()
    {
        commonConfig.put(TRUSTED_CERTIFICATE_ENV_VAR,
                         "MY_TRUSTED_CERTIFICATES");
        commonConfig.put("MY_TRUSTED_CERTIFICATES", trusted_certificates);
        commonConfig.put(TRUSTSTORE_UPDATED_TIMESTAMP_PATH, truststoreUpdatedTimestampFilepath);
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
        config.put(KEYSTORE_UPDATED_TIMESTAMP_PATH, keystoreUpdatedTimestampFilepath);
        /*
         * In order to test with real 'env' variables comment out this line and set appropriate env variable. This is
         *  done to avoid having a dependency on env in the unit test.
         */
        config.put("MY_KEY_PASSWORD", "cassandra");
    }

    private void addUnencryptedKeystoreOptions(Map<String, Object> config)
    {
        config.put(DEFAULT_PRIVATE_KEY_ENV_VAR_NAME, unencrypted_private_key);
        config.put(KEYSTORE_UPDATED_TIMESTAMP_PATH, keystoreUpdatedTimestampFilepath);
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
        kubernetesSecretsSslContextFactory.checkedExpiry = false;
        kubernetesSecretsSslContextFactory.buildTrustManagerFactory();
    }

    @Test
    public void buildTrustManagerFactoryHappyPath() throws IOException
    {
        Map<String, Object> config = new HashMap<>();
        config.putAll(commonConfig);

        KubernetesSecretsPEMSslContextFactory kubernetesSecretsSslContextFactory = new KubernetesSecretsPEMSslContextFactoryForTestOnly(config);
        kubernetesSecretsSslContextFactory.checkedExpiry = false;
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
        kubernetesSecretsSslContextFactory.checkedExpiry = false;
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
        kubernetesSecretsSslContextFactory.checkedExpiry = false;
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
        kubernetesSecretsSslContextFactory.checkedExpiry = false;
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
