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

package org.apache.cassandra.vault;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import com.google.common.collect.ImmutableMap;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.security.X509Credentials;
import org.apache.cassandra.vault.VaultIO.VaultResponse;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class VaultCertificateIssuerTest
{
    @BeforeClass
    public static void setup() throws ConfigurationException
    {
        SchemaLoader.prepareServer();
        VaultIO.instance.setup(true);
    }

    @Test
    public void testParseResult() throws IOException, ExecutionException, InterruptedException
    {
        try(LocalHttpTestServer ignored = new LocalHttpTestServer())
        {
            VaultCertificateIssuer issuer = new VaultCertificateIssuer(ImmutableMap.of("pki_path", "test"));
            CompletableFuture<VaultResponse> result = VaultIO.instance.post("vault/issue.json", issuer.createPayload(), null);
            X509Credentials credentials = issuer.parseCredentials(result.get());
            assertNotNull(credentials);
            assertNotNull(credentials.privateKey);
            assertNotNull(credentials.chain);
            assertEquals(2, credentials.chain.length);
        }
    }

    @Test
    public void testDecodeKey()
    {
        VaultCertificateIssuer issuer = new VaultCertificateIssuer(ImmutableMap.of("pki_path", "test"));
        String key = "MIIEvQIBADANBgkqhkiG9w0BAQEFAASCBKcwggSjAgEAAoIBAQCkzXgRYdkTY1ZSTpTZ2mkJb0IpMKY/V52qwKXLM7e3Bfa06lm+tTOe3LKza6bETT17ZmvR5ffYxuOQNVATDsPsPWh780/AAk74nOejKWlwEdsa3Um5hAqLK4uaPU3oF25jW6tcyYslf706aZrz4uuuNGt//EkNkoAKV868qVoUEl6PqbawbMwPFMEaHRmy49mAsDAzYtxz5ejOLBE3jyfmDa95zmwMXYzvwP1Q4OSc9gYOy2jPe0xe7is90YV0VR9mrWuZaAjHgfrbXuhIQ80ZssovsfMwnoWzGI9wi583eEAXtE/1Tyv3GoPRoHnc4oS4a/7hiTiL5PZhHdyi4RHxAgMBAAECggEAF4RaheB4oZOjVctw4kWQh5Ag2M/gaVmPXjZvcjfHF476TYbDl7szyo+j0IakHY8IHvvWlvVCEOUQxBtXeeC5hJSevFFUKAosSr3ZMCdQrHVf1s2NH7P/7Swhl7j3zQ7K6dLC+VKpUIiR8CvxuKvBBfLlBDZ0FkO5kcBF+BYSjJp31DGvem6l6yg1/Y2IOfFBpLwNSXnVO4CK2wR1v1XBNgp0jaRoKWcdk75FDo8A/EsB8oigmprk26wIfrLgL4Ee4R2VdYaVHC0Ggg+0Vub66/LTvlLgQG3dEHIzGcVcnU01/H4BcJr9Is4Bkx5MuzUtLPwC8a24DpKAoJl1qG/BIQKBgQDLcxa8ne9KRXzLre78og0QapBXBRbdVV27zP9jHP1W3V3P2Z/JyxI2J82Y3Stg+uh58hGM93v5wioV0krJfS67UATOmbdY2VE32e+W0piSp9QBQQBR5s6/STf9GNJkOGe1uv5yqbVGw/er11hWW8VRXZGhPgy+2wupAjmpIqdpvwKBgQDPXuMG4UTHYIeAwwXhS7T2ePPGomLOk4ioowb+JoSpgwCcLIdilrhsmhOP5iFFIWVpJBno5QqBg/7gyZmZFK2NaqD81OBa4van4DJ7i0IQGXWthu4cRUOwj5BS8hlpWXUvp0bkj130JslgKLYTeg3cW0IrVYHnEZKXFekREUSQTwKBgEi4JVtb1Ekm1zlyPSb9wU+p11fTUN3iAnP7DRnfJcpjq4F8lvmo5SSIS5ulCjlK1ceot488corOVP6hwOuOHCMFsgIqvXc7jiU5d8LgGXrqFAQyuKuNpT6ILEQCGEmMQ72YThSsBkh6CU/Z1BBiEwBHQqNwq1uYre1GB1gmM9K/AoGAHPST4L8NuoU5Bnq01HucvhmveFnuUAf7ughhjpVUStMW/7ecA+ElyUxOVPZ+SMfcAC1hTMrGh8Uljr+3qc9gWHG+Hu+ekDJG3LQKeIO+ar8TVnKTxvDI/dtd/Kb/c11hZpEF7h4ysUfFMJ8epWOSkeVQPPrIk7o5bM1LaO2vVDMCgYEAofnNZf2u22doQLrVyBVfMJHfTOxeEjpg4Dri6aQNpzR4WNCiDBe5fI4tERQpWfnfy3WMjZebEWONBu0E1AVcV9LtOflBLhimRZ75pVAZyXXxhYhVTRimfR+MKkfayBCaHP7mtdCRK8cD6X1PuZkhGU55eDfhzF/PSygd3RlNfnU=";

        assertNotNull(issuer.decodeKey(key));
    }

    @Test
    public void testDecodeCert()
    {
        VaultCertificateIssuer issuer = new VaultCertificateIssuer(ImmutableMap.of("pki_path", "test"));
        String cert = "MIID7jCCAtagAwIBAgIUK7hWybcivxQ7fOrAfGQ3qq17LagwDQYJKoZIhvcNAQELBQAwFjEUMBIGA1UEAxMLbXl2YXVsdC5jb20wHhcNMTcxMTAyMTQzNTU0WhcNMTcxMTA1MTQzNjI0WjAaMRgwFgYDVQQDEw9mb28uZXhhbXBsZS5jb20wggEiMA0GCSqGSIb3DQEBAQUAA4IBDwAwggEKAoIBAQCkzXgRYdkTY1ZSTpTZ2mkJb0IpMKY/V52qwKXLM7e3Bfa06lm+tTOe3LKza6bETT17ZmvR5ffYxuOQNVATDsPsPWh780/AAk74nOejKWlwEdsa3Um5hAqLK4uaPU3oF25jW6tcyYslf706aZrz4uuuNGt//EkNkoAKV868qVoUEl6PqbawbMwPFMEaHRmy49mAsDAzYtxz5ejOLBE3jyfmDa95zmwMXYzvwP1Q4OSc9gYOy2jPe0xe7is90YV0VR9mrWuZaAjHgfrbXuhIQ80ZssovsfMwnoWzGI9wi583eEAXtE/1Tyv3GoPRoHnc4oS4a/7hiTiL5PZhHdyi4RHxAgMBAAGjggEuMIIBKjAOBgNVHQ8BAf8EBAMCA6gwHQYDVR0lBBYwFAYIKwYBBQUHAwEGCCsGAQUFBwMCMB0GA1UdDgQWBBQcNkWL82UAAg8hnd13pERzPgKoXjAfBgNVHSMEGDAWgBSS7XY8gXBIaBOYNcOULWCARbH29DBHBggrBgEFBQcBAQQ7MDkwNwYIKwYBBQUHMAKGK2h0dHA6Ly8xMjcuMC4wLjE6ODIwMC92MS9wa2kvY2FzLWNsdXN0ZXIvY2EwMQYDVR0RBCowKIIPZm9vLmV4YW1wbGUuY29tgg9mb28uZXhhbXBsZS5jb22HBH8AAAEwPQYDVR0fBDYwNDAyoDCgLoYsaHR0cDovLzEyNy4wLjAuMTo4MjAwL3YxL3BraS9jYXMtY2x1c3Rlci9jcmwwDQYJKoZIhvcNAQELBQADggEBACR7tWQkYe+1JtwV2Q5omAxWhxot9KMMVfC1Qye8a38Hi+uFFYeOFW/0S9f59T1x7TzxroPNDe5u45KF5K37oLzGTpG/JZbnY9lyfiUqct53Vhh0ZrC6CQ02Wyx1NhBj2Yxd8Fs198A7pX6s8vSzhmQTC4aVCzuHoKQm4LBKQ8wsms8nqlO8mvYg5lIhohe9tNn+RggGmaTlvzDHbY5CWU692kxMJE3J8B/H2ybVMrZ1el64E3l1h2MqCyOfBpl0RXuAkL8zDPgin1hvdxrCpJpf2hZnjAAy68BVOpThW/OxJ6daisgx0lHNjMzL3tk03iUH9l8i1xh/s3M3uDALm9c=";

        assertNotNull(issuer.decodeCertificate(cert));
    }

}
