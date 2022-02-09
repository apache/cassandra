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
import java.security.GeneralSecurityException;
import java.security.PrivateKey;
import java.security.cert.Certificate;
import java.util.Set;

import org.junit.Assert;
import org.junit.Test;

public class PEMReaderTest
{
    private static final String encoded_encrypted_key =
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

    private static final String encoded_key =
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
    private static final String encoded_unencrypted_ec_private_key =
    "-----BEGIN PRIVATE KEY-----\n" +
    "MIGHAgEAMBMGByqGSM49AgEGCCqGSM49AwEHBG0wawIBAQQgMLP6H2Wdl28J5PHU\n" +
    "gMLApCsjONhbyMd5br0byJaQpXShRANCAASmX26IPehdE1wdLW2fVndT9QbjURro\n" +
    "h74aMnzlmq8GIBWnRzpd+JVJlHgeWLZIDwapthGCYUGivtH27wiO3g7d\n" +
    "-----END PRIVATE KEY-----\n" +
    "-----BEGIN CERTIFICATE-----\n" +
    "MIIBizCCATACCQCtgEKhNta70DAKBggqhkjOPQQDAjBNMQswCQYDVQQGEwJVUzEL\n" +
    "MAkGA1UECAwCQ0ExETAPBgNVBAcMCFNhbiBKb3NlMREwDwYDVQQKDAhQZXJzb25h\n" +
    "bDELMAkGA1UECwwCSVQwHhcNMjExMDE5MDAzMDU4WhcNMjIxMDE0MDAzMDU4WjBN\n" +
    "MQswCQYDVQQGEwJVUzELMAkGA1UECAwCQ0ExETAPBgNVBAcMCFNhbiBKb3NlMREw\n" +
    "DwYDVQQKDAhQZXJzb25hbDELMAkGA1UECwwCSVQwWTATBgcqhkjOPQIBBggqhkjO\n" +
    "PQMBBwNCAASmX26IPehdE1wdLW2fVndT9QbjURroh74aMnzlmq8GIBWnRzpd+JVJ\n" +
    "lHgeWLZIDwapthGCYUGivtH27wiO3g7dMAoGCCqGSM49BAMCA0kAMEYCIQDsNMGL\n" +
    "b4+BEhgNXaXyHWkezUO/3hCmLDw2gUdwMXG+JQIhAIAm8wALKbb9jJDgFQTHyqGJ\n" +
    "AVAkzYOwmRMYC9BHKjNs\n" +
    "-----END CERTIFICATE-----";
    private static final String encoded_encrypted_key_with_multiplecerts_in_chain =
    "-----BEGIN ENCRYPTED PRIVATE KEY-----\n" +
    "MIIE6TAbBgkqhkiG9w0BBQMwDgQI4QuRiKYzf88CAggABIIEyPRVmPp38SIFr8H3\n" +
    "wi+oc6b+HJH7SPflXO6XZe4Tignw/aSyBTsLm2dWrzojRAYMIRd1xC7yQ2ffYrvx\n" +
    "uoYbtOQeAminNqvwXdRTnwu6oC0rxdBT8RQ9NK7xL2tQyD/shmOeTJG/glXxaeqS\n" +
    "rT0CZ5P5GJh6xdIWLEu3lEa3NSWVFE2YacUphmxBoaWjBjsJfWTgkF665SgP+2lh\n" +
    "8R2WTcHrHjD8jR4jHB03wlup0LOmOwzplUmqHB9TyuA4wF6tlJajwBcPa0PNI6ny\n" +
    "e9YcdcRr7Y0IxnPQr7PhQNV5AQb9TivwX4WaZxR+BXtwMglp+mz0ohjwLS3z6pqr\n" +
    "tLrFhv2qcacSl+CKukFb9umV/QBkUk/iu+jwLcNJKPC965GWdieNbO0akBQpQsUN\n" +
    "mqaF9DYHogW5lRnybl8WWPIR8tXmSCbSUIgzw4lRK+o15I4vaMI0NfkwFD/2y1sn\n" +
    "t3m9LnVBukkpx3g/CPKd9PbZZeWpOTrnRJQfOu9Fj2lmkpGp0peCBqLJpO0pieVl\n" +
    "87EQ0ZCErtAGLGIAhWnDUqRK0MaWZ+DMQNKYn5klF4YTVBkfRc9tQbIgBaa77wvz\n" +
    "gvVWBuJtTFpCt9c8jByTH1gLbchC4bhLsy1nO7moevypMmNW4rqw9x5f0EIR3zCU\n" +
    "L5/buoIh91TG5JB7BaIbVHtbB/Y2siARRXJibuw3ChBjqPOfzQ66j//NCMqhfTwT\n" +
    "x2wn7L1BB4xyLJgVW9803FUTzaaL3EvJjzpdvrGC7vzcB6Jd0La9cjHhWSAPOKro\n" +
    "nD9XPCbgLs10vW9g1Tc8drnZklhw6f7xrIQhWFg6VlwmVpvCQhEpX48rCCo2PH9X\n" +
    "uzeJA+oqFEH3zfDp0r+q6jbAl+5TkkbBBgC2GCoI1vTYSKaWjeKKHkgzGG0QQLAz\n" +
    "YXWMXvWKOME4wVPkeVxJv80RqDv0JsoOrnVoaFAzAHJMWa7UZQnjkrbVz/y8ZbV4\n" +
    "oLJjQjvuOnU2PoXXs6SXbzOs1xx5zbX1UUH3Wc7/CCaUec2hemQJ5m6b1XJciyrY\n" +
    "GHpMKWtXky9Mo1ruTP7ZH1nk03b4PTObKSx2gQD5AZ/ASuTeahMqMb/2PJkDkpHA\n" +
    "sy8b1zOn2YTbf4K6NWVNIOkiaApmKhhX0Af6Lg8Wr2ymRTXdp/Um8f+SQLADpB/F\n" +
    "xOydEN622wmihKDge9DrUFqPG/bdIiRGLXLg8efNboC6/cn/i/sheO7/YlrvcUNo\n" +
    "qxDa/Bb1N/DgmtgAQ1ZP+AKjk6FKkwZRF1X/VZkZ6auscDlaPetF7razPeIJUrKN\n" +
    "z/x4AD2txGYKmeYztYR577hPXBw+PPKdggRhIugb6z5Tk89C2pDEwfnByA/wcGJr\n" +
    "w5avxrubosPrp0QtJpZMzouOvcD52VUiZzDfu9dqI/hpinyt5rETj5E19qxBjIZt\n" +
    "X3Nq5lY2ktbyqVIo8Z8w4EUU+3XHZKqDwjyYvjxCxv5lVVnqvQrH9h3kENBMrPYQ\n" +
    "4XonQHpUGG7g7pA3ylmHi+nEedr0H5qKHzyFZlRdI7CLVNoAtBSdwvmtGd2cVVXU\n" +
    "EaToKNbHPXXYYPX/oVAWZYwD7PHXIRJkiEZnrFARNhLypicU7yjvejUPXcVy5HMh\n" +
    "XqEbrODPp4VXfbYhVg==\n" +
    "-----END ENCRYPTED PRIVATE KEY-----\n" +
    "-----BEGIN CERTIFICATE-----\n" +
    "MIIDXjCCAkYCAhI0MA0GCSqGSIb3DQEBBAUAMHcxCzAJBgNVBAYTAlVTMRMwEQYD\n" +
    "VQQIDApDYWxpZm9ybmlhMREwDwYDVQQHDAhTYW4gSm9zZTEXMBUGA1UECgwOUGVy\n" +
    "c29uYWwsIEluYy4xEDAOBgNVBAsMB1Jvb3QgQ0ExFTATBgNVBAMMDG15ZG9tYWlu\n" +
    "LmNvbTAeFw0yMTExMjIyMjQ5MzlaFw0yMjExMjIyMjQ5MzlaMHIxCzAJBgNVBAYT\n" +
    "AlVTMRMwEQYDVQQIDApDYWxpZm9ybmlhMREwDwYDVQQHDAhTYW4gSm9zZTEXMBUG\n" +
    "A1UECgwOUGVyc29uYWwsIEluYy4xCzAJBgNVBAsMAklUMRUwEwYDVQQDDAxteWRv\n" +
    "bWFpbi5jb20wggEiMA0GCSqGSIb3DQEBAQUAA4IBDwAwggEKAoIBAQC5fdA7wwD9\n" +
    "9e5RcdLscvGB+hqJUEHuNC53SYKg5X4Sf0H4ExQUbsy8UaoWzWHhgGbCtTvUVavl\n" +
    "72xsO74ei0EblopW7QknF0kaTO8Vi3mxhUAdtZFLG/o0NS9J16HdGDGojJwuqU9+\n" +
    "sMQt1w0HCTMlriELnxaUFKP7M9b0uK5VODEKJ38QKNGXUDt66D7BVYeT/6hz2cXK\n" +
    "QWDoHk/JadALSzW5ES8KIHfxCLnl2TcKxQhJ4CnL8qeGvc8N3VyTh2AXajaJW5RB\n" +
    "8Oy4CVoYxcdmP1IapxCD+yNcmNt9XpUTD+6eM5gnvtbye+MSfwPz2MW+fWEDZXOv\n" +
    "3VxhJyTRFNVTAgMBAAEwDQYJKoZIhvcNAQEEBQADggEBADYK/pn6QG7bvUL0Xnnw\n" +
    "1KPf1nx36gfJE2V0bNk4uyNNeYufMKS8gPLzC+a3RigCEDc+hIZFE5BJexHd7DXA\n" +
    "CWgHZJtdjM/Xlgoxbf1yfGV3DWeIZlNFSFZujBIpbm1Ug2BAeV31YRWODPZlUSEZ\n" +
    "0jv8NEs8/oEz9bM4jwRdn09lo4D9hE6o8qDnrzmN2LBZP5dDIJ6g/M+mq/SJFnho\n" +
    "qBrfUABZhbgk2+tkZ89OI2xpASCLo6X/vqua2iho6km3x+cz6EI9BbvVr6xOOdVK\n" +
    "m6gs/Bi4MGTh35jdmvyXoyBUOd1w3yBBj86qbEt2ZHYqreRTxntQYx06598Q9Dsi\n" +
    "xdg=\n" +
    "-----END CERTIFICATE-----\n" +
    "-----BEGIN CERTIFICATE-----\n" +
    "MIIDajCCAlICCQD/7mxPcMTPIDANBgkqhkiG9w0BAQsFADB3MQswCQYDVQQGEwJV\n" +
    "UzETMBEGA1UECAwKQ2FsaWZvcm5pYTERMA8GA1UEBwwIU2FuIEpvc2UxFzAVBgNV\n" +
    "BAoMDlBlcnNvbmFsLCBJbmMuMRAwDgYDVQQLDAdSb290IENBMRUwEwYDVQQDDAxt\n" +
    "eWRvbWFpbi5jb20wHhcNMjExMTIyMjExODAwWhcNNDkwNDA5MjExODAwWjB3MQsw\n" +
    "CQYDVQQGEwJVUzETMBEGA1UECAwKQ2FsaWZvcm5pYTERMA8GA1UEBwwIU2FuIEpv\n" +
    "c2UxFzAVBgNVBAoMDlBlcnNvbmFsLCBJbmMuMRAwDgYDVQQLDAdSb290IENBMRUw\n" +
    "EwYDVQQDDAxteWRvbWFpbi5jb20wggEiMA0GCSqGSIb3DQEBAQUAA4IBDwAwggEK\n" +
    "AoIBAQCkIwuNGv3ckew/o2UwaDlYgXH9bh1jap4ZCb6qpjvR3tq9nCerY6XMli0Z\n" +
    "Xxg0wMHDNUr/jmVYIdQjbz0DVNz/l6ZBJHzHCEgqR40pNM3NgC5sDyuNhF3WLNvj\n" +
    "WgHEwYosfb/9kFRjKUPqqtJ0ccj87OP3XrE/4epCTdJdmugroAQSpXt1ZZfwwPO4\n" +
    "K27DzMD9W01EmeLcUhMfrpUnKGCfL22c0sZZm/6Khk4BExC3pSILP/NREKeUEAHw\n" +
    "+rxhNqbUyD/e4/DutdtJ5zONA+GVVGYCpu1Iy0W78Jve4MD2/TFPcEzf5omiWpPz\n" +
    "WjpOWayD43ur0SZnYJ5haUlZ+bSLAgMBAAEwDQYJKoZIhvcNAQELBQADggEBABqN\n" +
    "/eb+mKEw2zklPZuzy5uhOc7ImG2LP/ha3oc2ntwkqZQ2LmVA/2ZNVfcNrRRngIfn\n" +
    "Ir9Kual7djmKmIQvanpnSyXOKOlIiuN0VOrewgzlBZZwlFJL/AH1l7K9uZfBbV5h\n" +
    "oFfaR9wc+vRGsg3nqO9+hEuk6xbp0jk8QCt26EVhEPlijxzbxTQYiNPNSLuf/kPW\n" +
    "C9xtIKSWIDT4N6DtH7BtHGQKQdRJ2b4SSUF4joEmBe6jcrLBeDybmuFtKqlVJKUk\n" +
    "tzBd9CPseqMML1c518KzxlSkXNxTCa7PWEnuN5atLZ+pGGjxtGcDKkrZ9Cgi09G8\n" +
    "MzB8b4C/goypyhBNlyI=\n" +
    "-----END CERTIFICATE-----";
    private static final String encoded_encrypted_dsa_key =
    "-----BEGIN ENCRYPTED PRIVATE KEY-----\n" +
    "MIICkTAbBgkqhkiG9w0BBQMwDgQIL1GiUDca2x0CAggABIICcCrSiNBy5kZNC7RK\n" +
    "fVF3IZ9Ecl00OYIjvBhlWGkaiNt9ZAeWPpYx57HSQAygzJ8ba+3jtz1dV8Bhz5D5\n" +
    "4kggzolC820I/QLPxClH5R6ZPzDHGu/JFuNNZWASq2JHZfolEP+itdnz0F6VvQx9\n" +
    "imngOGIJMkRWIzvwuCnq8xpNSEJHJcs8kyLBP/3qT+kUiEwJ5KrmJ8DQsHluwpyo\n" +
    "GoiGEtiA+MNUSzc/DkuLxhC45k7K4afe4QzpWl9eR/z91F9Q06jdc5mnUWke8qCg\n" +
    "ZopBVWbSI6fMZVlqLxKeSFljDZ+moW+2/Lh8cjEvNMccVTwWQE7JH9RLnVogqixV\n" +
    "HIpN58wrlHd4uMVH47wD18A11AGEbghO1MEShX4SCEJIdZsr67bNx8mIkhSWjqAx\n" +
    "BT9OmITzdbnR9sHi4CyEWhGMAMDp7YBySwpt+U7Y6DvRwWJbXUaF8zYJUlrp2IbH\n" +
    "qdSE+oJPKxGB1s2B5KGrJUA0JkElkBUYm6ghXZlTI32w8HoV9fDOhjx1ATu7SdZY\n" +
    "8DX0mwVVdc88Msr5RPxdeB3V4yN2iFJs4i3usicPkB0N+29LJ/lKQlm/pia0yl9j\n" +
    "yDNN3R0RiCJdYHme4t1PqRqeTfjMauz05ObennQmkzMxD8mlBZ0zhaKL5I4TuQod\n" +
    "PITFgYihTR9OYfa8lryvHQNCIi5iZ6M9myLUxbjPoeVBdp8pSlMAjmekEHo47vn3\n" +
    "7IGF7AfnKVNymc/1Kim3WQx8D7nryCb8EUyb7BuNA7izGKq+NW51l39J8RGVcFSx\n" +
    "sVkpFUbqGXusUgLWWwi21EJHCJceFnOWJRgsoeqNeCt5VCUSag==\n" +
    "-----END ENCRYPTED PRIVATE KEY-----\n" +
    "-----BEGIN CERTIFICATE-----\n" +
    "MIIEeDCCBB4CCQD9UojL1A7cmzAJBgcqhkjOOAQDME0xCzAJBgNVBAYTAlVTMQsw\n" +
    "CQYDVQQIDAJDQTERMA8GA1UEBwwIU2FuIEpvc2UxETAPBgNVBAoMCFBlcnNvbmFs\n" +
    "MQswCQYDVQQLDAJJVDAeFw0yMTEwMTgyMzU0MjRaFw0yMjEwMTgyMzU0MjRaME0x\n" +
    "CzAJBgNVBAYTAlVTMQswCQYDVQQIDAJDQTERMA8GA1UEBwwIU2FuIEpvc2UxETAP\n" +
    "BgNVBAoMCFBlcnNvbmFsMQswCQYDVQQLDAJJVDCCA0YwggI5BgcqhkjOOAQBMIIC\n" +
    "LAKCAQEAzxMsktQYQ06q71pwqWAa9YvDIkF34RJgQT5tqCZdzwtP7HXyrwaZBgLK\n" +
    "oC4grIPKyUlPmIbm+oucucbTzDCYCx2d09VMVR471vqJi7vRCYnBIqMTTOSvbTGw\n" +
    "8VuwA2prSt4TiqFYeqYM0Afv/KrI2MLNTP+z8RFfiLkijZIxZ9CzyHmTOEqHxOft\n" +
    "Ln145NNcuPy3U7Bmh5k7i+RR+5jYweuLzOfTK4bTUnG111mjAhAeo+ST14ydfiuQ\n" +
    "2e85UwLucVheh4REqm+n7g0k0B/+nG+9Os1nxfQVRVKHP4/iTrNLpf35EoPeh9XT\n" +
    "D0BtV9lqQuttCB1aPLH66TKl9v4FowIhAMXhJ3LooBT7ypUWMtN07FvpPpoLERBr\n" +
    "Pk9MeGwQnBPFAoIBAAcVYJ+RH1i8JFyD8MUwwOKgkmVzbRvc28B5F17oek76R6fi\n" +
    "yyqyhHrkxmjwxktXuwRlWQBJqCTqpmMstLplqsVjcETHo7KRaHgDpT9tHf14PZvP\n" +
    "qpoxYNpa5z4wPpOFhZ3K6VVaFUSlqSSHhaS1HqVzZC0FefFFd3TDKu0EWHDDkLM8\n" +
    "luOxDUMydKpRtrVba+iF4kx9NcCyXjSMhFAOw3cWhGvoq6R3h4UkRJO67pzYtqK4\n" +
    "yAETPsQKT0c8NUM+VUiPAHL/+f1EqaVtzatk9G0OHWZXgDxuhx2CAM6QfELew+ys\n" +
    "D/QUUT0tXHQRxRCKmYE+uacwyQx5G0DeCHN0c9gDggEFAAKCAQBFGXF7flzfRH/8\n" +
    "r/qDuNC+9y3fRtDWXaados+XQkzujizBBK9dkkAd/j/pgP5p7/bRvoeR507Oyrge\n" +
    "6xBbfE7SlRu0cx08Ihlw7a6mCowMnP0psFBwW7npKJRYPXxRDpu9oUuuJHi1WYp7\n" +
    "8/Ekg6hG21zAYO0JLGyU6aH5Rvuls754R+rxqveVqLiig5NTnfw7ymLb2kF1z8g7\n" +
    "fDvX6OS242ceQIQHvq97cGnysEVSlavuHfbh3PKXklh91ip/BYuzanQjiEb97YIU\n" +
    "DhMQyPGvk+iDdhD0PwgwOZB0P0mL7Xszw6p+lfzUc+g8jETjrnzksnVEg8On6q3Q\n" +
    "RDJG9VnuMAkGByqGSM44BAMDSQAwRgIhAIay5hyZrwHslGXmTAx498903l7CQsdi\n" +
    "fUrmGkxbPiZXAiEAitS8+gyG64c0jV7V/u8Z1NEL8MI47K2P9Jbn3cqzz3k=\n" +
    "-----END CERTIFICATE-----";
    private static final String invalid_encoded_private_key =
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
    "RXKInnLInFD9JD94VqmMozoInvalidData=\n" +
    "-----END PRIVATE KEY-----\n";
    private static final String invalid_encoded_certificate =
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
    "n3MVF9wInvalidData=\n" +
    "-----END CERTIFICATE-----";

    @Test
    public void readEncryptedKey() throws IOException, GeneralSecurityException
    {
        PrivateKey privateKey = PEMReader.extractPrivateKey(encoded_encrypted_key, "cassandra");
        Assert.assertNotNull(privateKey);
    }

    @Test
    public void readEncryptedDSAKey() throws IOException, GeneralSecurityException
    {
        PrivateKey privateKey = PEMReader.extractPrivateKey(encoded_encrypted_dsa_key, "mytest");
        Assert.assertNotNull(privateKey);
    }

    @Test(expected = GeneralSecurityException.class)
    public void readEncryptedDSAKeyWithBadPassword() throws IOException, GeneralSecurityException
    {
        try
        {
            PEMReader.extractPrivateKey(encoded_encrypted_dsa_key, "bad-password");
        } catch(GeneralSecurityException e) {
            Assert.assertTrue(e.getMessage().startsWith("Failed to decrypt the private key data. Either the password " +
                                                        "provided for the key is wrong or the private key data is " +
                                                        "corrupted. msg="));
            throw e;
        }
    }

    @Test(expected = GeneralSecurityException.class)
    public void readInvalidEncryptedKey() throws IOException, GeneralSecurityException
    {
        // Test by injecting junk data in the given key and making it invalid
        PrivateKey privateKey = PEMReader.extractPrivateKey(encoded_encrypted_key.replaceAll("\\s",
                                                                                             String.valueOf(System.nanoTime())),
                                                            "cassandra");
        Assert.assertNotNull(privateKey);
    }

    @Test(expected = GeneralSecurityException.class)
    public void readInvalidBase64PrivateKey() throws IOException, GeneralSecurityException
    {
        PEMReader.extractPrivateKey(invalid_encoded_private_key);
    }

    @Test
    public void readUnencryptedKey() throws IOException, GeneralSecurityException
    {
        PrivateKey privateKey = PEMReader.extractPrivateKey(encoded_key);
        Assert.assertNotNull(privateKey);
    }

    @Test
    public void readUnencryptedECKey() throws IOException, GeneralSecurityException
    {
        PrivateKey privateKey = PEMReader.extractPrivateKey(encoded_unencrypted_ec_private_key);
        Assert.assertNotNull(privateKey);
    }

    @Test
    public void readCertChain() throws GeneralSecurityException
    {
        Certificate[] certificates = PEMReader.extractCertificates(encoded_encrypted_key);
        Assert.assertNotNull("CertChain must not be null", certificates);
        Assert.assertTrue("CertChain must have only one certificate", certificates.length == 1);
    }

    @Test
    public void readCertChainWithMoreThanOneCerts() throws GeneralSecurityException
    {
        Certificate[] certificates = PEMReader.extractCertificates(encoded_encrypted_key_with_multiplecerts_in_chain);
        Assert.assertNotNull("CertChain must not be null", certificates);
    }

    @Test(expected = GeneralSecurityException.class)
    public void readInvalidCertificate() throws GeneralSecurityException
    {
        // Test by injecting junk data in the given key and making it invalid
        Certificate[] certificates = PEMReader.extractCertificates(encoded_encrypted_key.replaceAll("\\s",
                                                                                                    String.valueOf(System.nanoTime())));
        Assert.assertNotNull("CertChain must not be null", certificates);
        Assert.assertTrue("CertChain must have only one certificate", certificates.length == 1);
    }

    @Test(expected = GeneralSecurityException.class)
    public void readInvalidBase64Certificate() throws GeneralSecurityException
    {
        PEMReader.extractCertificates(invalid_encoded_certificate);
    }

    @Test
    public void readTrustedCertificates() throws GeneralSecurityException
    {
        Certificate[] certificates = PEMReader.extractCertificates(encoded_certificates);
        Assert.assertNotNull("Trusted certificate list must not be null", certificates);
        Assert.assertTrue("Trusted certificate list must have only one certificate", certificates.length == 1);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void tamperSupportedAlgorithms() throws GeneralSecurityException
    {
        Set<String> original = PEMReader.SUPPORTED_PRIVATE_KEY_ALGORITHMS;
        for (int i = 0; i < original.size(); i++)
        {
            original.remove("RSA");
            original.remove("DSA");
            original.remove("EC");
            original.add("TAMPERED");
        }
    }
}
