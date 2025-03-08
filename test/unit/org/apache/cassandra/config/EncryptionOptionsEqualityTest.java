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
import org.apache.cassandra.transport.TlsTestUtils;

import static org.apache.cassandra.config.EncryptionOptions.ClientEncryptionOptions.ClientAuth.NOT_REQUIRED;
import static org.apache.cassandra.config.EncryptionOptions.ClientEncryptionOptions.ClientAuth.REQUIRED;
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
        EncryptionOptions.ServerEncryptionOptions.Builder serverEncryptionOptionsBuilder = new EncryptionOptions.ServerEncryptionOptions.Builder();
        return serverEncryptionOptionsBuilder
               .withOutboundKeystore(TlsTestUtils.SERVER_OUTBOUND_KEYSTORE_PATH)
               .withOutboundKeystorePassword(TlsTestUtils.SERVER_OUTBOUND_KEYSTORE_PASSWORD)
               .withStoreType("JKS")
               .withKeyStore(TlsTestUtils.SERVER_KEYSTORE_PATH)
               .withKeyStorePassword(TlsTestUtils.SERVER_KEYSTORE_PASSWORD)
               .withTrustStore(TlsTestUtils.SERVER_TRUSTSTORE_PATH)
               .withTrustStorePassword(TlsTestUtils.SERVER_TRUSTSTORE_PASSWORD)
               .withProtocol("TLSv1.1")
               .withRequireClientAuth(REQUIRED)
               .withRequireEndpointVerification(false)
               .build();
    }

    @Test
    public void testKeystoreOptions() {
        EncryptionOptions.ServerEncryptionOptions encryptionOptions1 =
        new EncryptionOptions.ServerEncryptionOptions.Builder()
        .withStoreType("JKS")
        .withKeyStore(TlsTestUtils.SERVER_KEYSTORE_PATH)
        .withKeyStorePassword(TlsTestUtils.SERVER_KEYSTORE_PASSWORD)
        .withTrustStore(TlsTestUtils.SERVER_TRUSTSTORE_PATH)
        .withTrustStorePassword(TlsTestUtils.SERVER_TRUSTSTORE_PASSWORD)
        .withProtocol("TLSv1.1")
        .withRequireClientAuth(REQUIRED)
        .withRequireEndpointVerification(false)
        .build();

        EncryptionOptions.ServerEncryptionOptions encryptionOptions2 =
        new EncryptionOptions.ServerEncryptionOptions.Builder()
        .withStoreType("JKS")
        .withKeyStore(TlsTestUtils.SERVER_KEYSTORE_PATH)
        .withKeyStorePassword(TlsTestUtils.SERVER_KEYSTORE_PASSWORD)
        .withTrustStore(TlsTestUtils.SERVER_TRUSTSTORE_PATH)
        .withTrustStorePassword(TlsTestUtils.SERVER_TRUSTSTORE_PASSWORD)
        .withProtocol("TLSv1.1")
        .withRequireClientAuth(REQUIRED)
        .withRequireEndpointVerification(false)
        .build();

        assertEquals(encryptionOptions1, encryptionOptions2);
        assertEquals(encryptionOptions1.hashCode(), encryptionOptions2.hashCode());
    }

    @Test
    public void testKeystoreOptionsWithPasswordFile() {
        EncryptionOptions.ServerEncryptionOptions encryptionOptions1 =
        new EncryptionOptions.ServerEncryptionOptions.Builder()
        .withStoreType("JKS")
        .withKeyStore(TlsTestUtils.SERVER_KEYSTORE_PATH)
        .withKeyStorePasswordFile(TlsTestUtils.SERVER_KEYSTORE_PASSWORD_FILE)
        .withTrustStore(TlsTestUtils.SERVER_TRUSTSTORE_PATH)
        .withTrustStorePasswordFile(TlsTestUtils.SERVER_TRUSTSTORE_PASSWORD_FILE)
        .withProtocol("TLSv1.1")
        .withRequireClientAuth(REQUIRED)
        .withRequireEndpointVerification(false)
        .build();

        EncryptionOptions.ServerEncryptionOptions encryptionOptions2 =
        new EncryptionOptions.ServerEncryptionOptions.Builder()
        .withStoreType("JKS")
        .withKeyStore(TlsTestUtils.SERVER_KEYSTORE_PATH)
        .withKeyStorePasswordFile(TlsTestUtils.SERVER_KEYSTORE_PASSWORD_FILE)
        .withTrustStore(TlsTestUtils.SERVER_TRUSTSTORE_PATH)
        .withTrustStorePasswordFile(TlsTestUtils.SERVER_TRUSTSTORE_PASSWORD_FILE)
        .withProtocol("TLSv1.1")
        .withRequireClientAuth(REQUIRED)
        .withRequireEndpointVerification(false)
        .build();

        assertEquals(encryptionOptions1, encryptionOptions2);
        assertEquals(encryptionOptions1.hashCode(), encryptionOptions2.hashCode());
    }

    @Test
    public void testMismatchForKeystoreOptionsWithPasswordFile()
    {
        EncryptionOptions.ServerEncryptionOptions encryptionOptions1 = createServerEncryptionOptions();
        EncryptionOptions.ServerEncryptionOptions encryptionOptions2 = createServerEncryptionOptions();

        encryptionOptions1 = new EncryptionOptions.ServerEncryptionOptions.Builder(encryptionOptions1)
                             .withKeyStore(TlsTestUtils.SERVER_KEYSTORE_PATH)
                             .withKeyStorePassword(null)
                             .withKeyStorePasswordFile(TlsTestUtils.SERVER_KEYSTORE_PASSWORD_FILE)
                             .build();

        encryptionOptions2 = new EncryptionOptions.ServerEncryptionOptions.Builder(encryptionOptions2)
                             .withKeyStore(TlsTestUtils.SERVER_KEYSTORE_PATH)
                             .withKeyStorePassword(null)
                             .withKeyStorePasswordFile(TlsTestUtils.SERVER_TRUSTSTORE_PASSWORD_FILE)
                             .build();

        assertNotEquals(encryptionOptions1, encryptionOptions2);
        assertNotEquals(encryptionOptions1.hashCode(), encryptionOptions2.hashCode());
    }

    @Test
    public void testSameCustomSslContextFactoryImplementation() {

        Map<String,String> parameters1 = new HashMap<>();
        parameters1.put("key1", "value1");
        parameters1.put("key2", "value2");
        EncryptionOptions.ClientEncryptionOptions encryptionOptions1 =
        new EncryptionOptions.ClientEncryptionOptions.Builder()
        .withSslContextFactory(new ParameterizedClass(DummySslContextFactoryImpl.class.getName(), parameters1))
        .withProtocol("TLSv1.1")
        .withRequireClientAuth(REQUIRED)
        .withRequireEndpointVerification(false)
        .build();

        Map<String,String> parameters2 = new HashMap<>();
        parameters2.put("key1", "value1");
        parameters2.put("key2", "value2");
        EncryptionOptions.ClientEncryptionOptions encryptionOptions2 =
        new EncryptionOptions.ClientEncryptionOptions.Builder()
        .withSslContextFactory(new ParameterizedClass(DummySslContextFactoryImpl.class.getName(), parameters2))
        .withProtocol("TLSv1.1")
        .withRequireClientAuth(REQUIRED)
        .withRequireEndpointVerification(false)
        .build();

        assertEquals(encryptionOptions1, encryptionOptions2);
        assertEquals(encryptionOptions1.hashCode(), encryptionOptions2.hashCode());
    }

    @Test
    public void testDifferentCustomSslContextFactoryImplementations() {

        Map<String,String> parameters1 = new HashMap<>();
        parameters1.put("key1", "value1");
        parameters1.put("key2", "value2");
        EncryptionOptions.ClientEncryptionOptions encryptionOptions1 =
        new EncryptionOptions.ClientEncryptionOptions.Builder()
        .withSslContextFactory(new ParameterizedClass(DummySslContextFactoryImpl.class.getName(), parameters1))
        .withProtocol("TLSv1.1")
        .withRequireClientAuth(NOT_REQUIRED)
        .withRequireEndpointVerification(true)
        .build();

        Map<String,String> parameters2 = new HashMap<>();
        parameters2.put("key1", "value1");
        parameters2.put("key2", "value2");
        EncryptionOptions.ClientEncryptionOptions encryptionOptions2 =
        new EncryptionOptions.ClientEncryptionOptions.Builder()
        .withSslContextFactory(new ParameterizedClass(DefaultSslContextFactory.class.getName(), parameters2))
        .withProtocol("TLSv1.1")
        .withRequireClientAuth(NOT_REQUIRED)
        .withRequireEndpointVerification(true)
        .build();

        assertNotEquals(encryptionOptions1, encryptionOptions2);
        assertNotEquals(encryptionOptions1.hashCode(), encryptionOptions2.hashCode());
    }

    @Test
    public void testDifferentCustomSslContextFactoryParameters() {

        Map<String,String> parameters1 = new HashMap<>();
        parameters1.put("key1", "value11");
        parameters1.put("key2", "value12");
        EncryptionOptions.ClientEncryptionOptions encryptionOptions1 =
        new EncryptionOptions.ClientEncryptionOptions.Builder()
        .withSslContextFactory(new ParameterizedClass(DummySslContextFactoryImpl.class.getName(), parameters1))
        .withProtocol("TLSv1.1")
        .build();

        Map<String,String> parameters2 = new HashMap<>();
        parameters2.put("key1", "value21");
        parameters2.put("key2", "value22");
        EncryptionOptions.ClientEncryptionOptions encryptionOptions2 =
        new EncryptionOptions.ClientEncryptionOptions.Builder()
        .withSslContextFactory(new ParameterizedClass(DummySslContextFactoryImpl.class.getName(), parameters2))
        .withProtocol("TLSv1.1")
        .build();

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

        encryptionOptions1 = new EncryptionOptions.ServerEncryptionOptions.Builder(encryptionOptions1)
                             .withOutboundKeystore("test/conf/cassandra_outbound1.keystore")
                             .withOutboundKeystorePassword("cassandra1")
                             .build();

        encryptionOptions2 = new EncryptionOptions.ServerEncryptionOptions.Builder(encryptionOptions2)
                             .withOutboundKeystore("test/conf/cassandra_outbound2.keystore")
                             .withOutboundKeystorePassword("cassandra2")
                             .build();

        assertNotEquals(encryptionOptions1, encryptionOptions2);
        assertNotEquals(encryptionOptions1.hashCode(), encryptionOptions2.hashCode());
    }

    @Test
    public void testServerEncryptionOptionsMismatchForInboundKeystore()
    {
        EncryptionOptions.ServerEncryptionOptions encryptionOptions1 = createServerEncryptionOptions();
        EncryptionOptions.ServerEncryptionOptions encryptionOptions2 = createServerEncryptionOptions();

        encryptionOptions1 = new EncryptionOptions.ServerEncryptionOptions.Builder(encryptionOptions1)
                             .withKeyStore("test/conf/cassandra1.keystore")
                             .withKeyStorePassword("cassandra1")
                             .build();

        encryptionOptions2 = new EncryptionOptions.ServerEncryptionOptions.Builder(encryptionOptions2)
                             .withKeyStore("test/conf/cassandra2.keystore")
                             .withKeyStorePassword("cassandra2")
                             .build();

        assertNotEquals(encryptionOptions1, encryptionOptions2);
        assertNotEquals(encryptionOptions1.hashCode(), encryptionOptions2.hashCode());
    }
}
