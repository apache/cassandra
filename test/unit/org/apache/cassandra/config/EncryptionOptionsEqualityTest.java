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

package org.apache.cassandra.config;

import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

import org.apache.cassandra.security.DefaultSslContextFactory;
import org.apache.cassandra.security.DummySslContextFactoryImpl;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

/**
 * This class tests the equals and hashCode method of {@link EncryptionOptions} in order to make sure that the
 * caching done in the {@link org.apache.cassandra.security.SSLFactory} doesn't break.
 */
public class EncryptionOptionsEqualityTest
{
    private EncryptionOptions.ServerEncryptionOptions createServerEncryptionOptions()
    {
        return new EncryptionOptions.ServerEncryptionOptions()
               .withStoreType("JKS")
               .withKeyStore("test/conf/cassandra.keystore")
               .withKeyStorePassword("cassandra")
               .withTrustStore("test/conf/cassandra_ssl_test.truststore")
               .withTrustStorePassword("cassandra")
               .withOutboundKeystore("test/conf/cassandra_outbound.keystore")
               .withOutboundKeystorePassword("cassandra")
               .withProtocol("TLSv1.1")
               .withRequireClientAuth(true)
               .withRequireEndpointVerification(false);
    }

    @Test
    public void testKeystoreOptions() {
        EncryptionOptions encryptionOptions1 =
        new EncryptionOptions()
        .withStoreType("JKS")
        .withKeyStore("test/conf/cassandra.keystore")
        .withKeyStorePassword("cassandra")
        .withTrustStore("test/conf/cassandra_ssl_test.truststore")
        .withTrustStorePassword("cassandra")
        .withProtocol("TLSv1.1")
        .withRequireClientAuth(true)
        .withRequireEndpointVerification(false);

        EncryptionOptions encryptionOptions2 =
        new EncryptionOptions()
        .withStoreType("JKS")
        .withKeyStore("test/conf/cassandra.keystore")
        .withKeyStorePassword("cassandra")
        .withTrustStore("test/conf/cassandra_ssl_test.truststore")
        .withTrustStorePassword("cassandra")
        .withProtocol("TLSv1.1")
        .withRequireClientAuth(true)
        .withRequireEndpointVerification(false);

        assertEquals(encryptionOptions1, encryptionOptions2);
        assertEquals(encryptionOptions1.hashCode(), encryptionOptions2.hashCode());
    }

    @Test
    public void testSameCustomSslContextFactoryImplementation() {

        Map<String,String> parameters1 = new HashMap<>();
        parameters1.put("key1", "value1");
        parameters1.put("key2", "value2");
        EncryptionOptions encryptionOptions1 =
        new EncryptionOptions()
        .withSslContextFactory(new ParameterizedClass(DummySslContextFactoryImpl.class.getName(), parameters1))
        .withProtocol("TLSv1.1")
        .withRequireClientAuth(true)
        .withRequireEndpointVerification(false);

        Map<String,String> parameters2 = new HashMap<>();
        parameters2.put("key1", "value1");
        parameters2.put("key2", "value2");
        EncryptionOptions encryptionOptions2 =
        new EncryptionOptions()
        .withSslContextFactory(new ParameterizedClass(DummySslContextFactoryImpl.class.getName(), parameters2))
        .withProtocol("TLSv1.1")
        .withRequireClientAuth(true)
        .withRequireEndpointVerification(false);

        assertEquals(encryptionOptions1, encryptionOptions2);
        assertEquals(encryptionOptions1.hashCode(), encryptionOptions2.hashCode());
    }

    @Test
    public void testDifferentCustomSslContextFactoryImplementations() {

        Map<String,String> parameters1 = new HashMap<>();
        parameters1.put("key1", "value1");
        parameters1.put("key2", "value2");
        EncryptionOptions encryptionOptions1 =
        new EncryptionOptions()
        .withSslContextFactory(new ParameterizedClass(DummySslContextFactoryImpl.class.getName(), parameters1))
        .withProtocol("TLSv1.1")
        .withRequireClientAuth(false)
        .withRequireEndpointVerification(true);

        Map<String,String> parameters2 = new HashMap<>();
        parameters2.put("key1", "value1");
        parameters2.put("key2", "value2");
        EncryptionOptions encryptionOptions2 =
        new EncryptionOptions()
        .withSslContextFactory(new ParameterizedClass(DefaultSslContextFactory.class.getName(), parameters2))
        .withProtocol("TLSv1.1")
        .withRequireClientAuth(false)
        .withRequireEndpointVerification(true);

        assertNotEquals(encryptionOptions1, encryptionOptions2);
        assertNotEquals(encryptionOptions1.hashCode(), encryptionOptions2.hashCode());
    }

    @Test
    public void testDifferentCustomSslContextFactoryParameters() {

        Map<String,String> parameters1 = new HashMap<>();
        parameters1.put("key1", "value11");
        parameters1.put("key2", "value12");
        EncryptionOptions encryptionOptions1 =
        new EncryptionOptions()
        .withSslContextFactory(new ParameterizedClass(DummySslContextFactoryImpl.class.getName(), parameters1))
        .withProtocol("TLSv1.1");

        Map<String,String> parameters2 = new HashMap<>();
        parameters2.put("key1", "value21");
        parameters2.put("key2", "value22");
        EncryptionOptions encryptionOptions2 =
        new EncryptionOptions()
        .withSslContextFactory(new ParameterizedClass(DummySslContextFactoryImpl.class.getName(), parameters2))
        .withProtocol("TLSv1.1");

        assertNotEquals(encryptionOptions1, encryptionOptions2);
        assertNotEquals(encryptionOptions1.hashCode(), encryptionOptions2.hashCode());
    }

    @Test
    public void testServerEncryptionOptions()
    {
        EncryptionOptions.ServerEncryptionOptions encryptionOptions1 = createServerEncryptionOptions();
        EncryptionOptions.ServerEncryptionOptions encryptionOptions2 = createServerEncryptionOptions();

        assertEquals(encryptionOptions1, encryptionOptions2);
        assertEquals(encryptionOptions1.hashCode(), encryptionOptions2.hashCode());
    }

    @Test
    public void testServerEncryptionOptionsMismatchForOutboundKeystore()
    {
        EncryptionOptions.ServerEncryptionOptions encryptionOptions1 = createServerEncryptionOptions();
        EncryptionOptions.ServerEncryptionOptions encryptionOptions2 = createServerEncryptionOptions();

        encryptionOptions1 = encryptionOptions1
                             .withOutboundKeystore("test/conf/cassandra_outbound1.keystore")
                             .withOutboundKeystorePassword("cassandra1");

        encryptionOptions2 = encryptionOptions2
                             .withOutboundKeystore("test/conf/cassandra_outbound2.keystore")
                             .withOutboundKeystorePassword("cassandra2");

        assertNotEquals(encryptionOptions1, encryptionOptions2);
        assertNotEquals(encryptionOptions1.hashCode(), encryptionOptions2.hashCode());
    }

    @Test
    public void testServerEncryptionOptionsMismatchForInboundKeystore()
    {
        EncryptionOptions.ServerEncryptionOptions encryptionOptions1 = createServerEncryptionOptions();
        EncryptionOptions.ServerEncryptionOptions encryptionOptions2 = createServerEncryptionOptions();

        encryptionOptions1 = encryptionOptions1
                             .withKeyStore("test/conf/cassandra1.keystore")
                             .withKeyStorePassword("cassandra1");

        encryptionOptions2 = encryptionOptions2
                             .withKeyStore("test/conf/cassandra2.keystore")
                             .withKeyStorePassword("cassandra2");

        assertNotEquals(encryptionOptions1, encryptionOptions2);
        assertNotEquals(encryptionOptions1.hashCode(), encryptionOptions2.hashCode());
    }
}
