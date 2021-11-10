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

    private static String encoded_encrypted_dsa_key =
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
    public void readInvalidEncryptedKey() throws IOException, GeneralSecurityException
    {
        // Test by injecting junk data in the given key and making it invalid
        PrivateKey privateKey = PEMReader.extractPrivateKey(encoded_encrypted_key.replaceAll("\\s",
                                                                                             String.valueOf(System.nanoTime())),
                                                            "cassandra");
        Assert.assertNotNull(privateKey);
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
        Assert.assertTrue("CertChain must have only one certificate", certificates.length==1);
    }

    @Test(expected = GeneralSecurityException.class)
    public void readInvalidCertificate() throws GeneralSecurityException
    {
        // Test by injecting junk data in the given key and making it invalid
        Certificate[] certificates = PEMReader.extractCertificates(encoded_encrypted_key.replaceAll("\\s",
                                                                                                    String.valueOf(System.nanoTime())));
        Assert.assertNotNull("CertChain must not be null", certificates);
        Assert.assertTrue("CertChain must have only one certificate", certificates.length==1);
    }

    @Test
    public void readTrustedCertificates() throws GeneralSecurityException
    {
        Certificate[] certificates = PEMReader.extractCertificates(encoded_certificates);
        Assert.assertNotNull("Trusted certificate list must not be null", certificates);
        Assert.assertTrue("Trusted certificate list must have only one certificate", certificates.length==1);
    }
}
