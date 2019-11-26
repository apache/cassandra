/*
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing,
* software distributed under the License is distributed on an
* "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
* KIND, either express or implied.  See the License for the
* specific language governing permissions and limitations
* under the License.
*/
package org.apache.cassandra.security;

import java.io.File;
import java.io.IOException;
import java.security.cert.CertificateException;
import javax.net.ssl.TrustManagerFactory;

import org.apache.commons.io.FileUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.handler.ssl.JdkSslContext;
import io.netty.handler.ssl.OpenSsl;
import io.netty.handler.ssl.OpenSslContext;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.util.SelfSignedCertificate;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.EncryptionOptions;
import org.apache.cassandra.config.EncryptionOptions.ServerEncryptionOptions;

import static org.junit.Assert.assertArrayEquals;

public class SSLFactoryTest
{
    private static final Logger logger = LoggerFactory.getLogger(SSLFactoryTest.class);

    static final SelfSignedCertificate ssc;
    static
    {
        DatabaseDescriptor.daemonInitialization();
        try
        {
            ssc = new SelfSignedCertificate();
        }
        catch (CertificateException e)
        {
            throw new RuntimeException("fialed to create test certs");
        }
    }

    private ServerEncryptionOptions encryptionOptions;

    @Before
    public void setup()
    {
        encryptionOptions = new ServerEncryptionOptions()
                            .withTrustStore("test/conf/cassandra_ssl_test.truststore")
                            .withTrustStorePassword("cassandra")
                            .withRequireClientAuth(false)
                            .withCipherSuites("TLS_RSA_WITH_AES_128_CBC_SHA");

        SSLFactory.checkedExpiry = false;
    }

    @Test
    public void testFilterCipherSuites()
    {
        String[] supported = new String[] {"x", "b", "c", "f"};
        String[] desired = new String[] { "k", "a", "b", "c" };
        assertArrayEquals(new String[] { "b", "c" }, SSLFactory.filterCipherSuites(supported, desired));

        desired = new String[] { "c", "b", "x" };
        assertArrayEquals(desired, SSLFactory.filterCipherSuites(supported, desired));
    }

    @Test
    public void getSslContext_OpenSSL() throws IOException
    {
        // only try this test if OpenSsl is available
        if (!OpenSsl.isAvailable())
        {
            logger.warn("OpenSSL not available in this application, so not testing the netty-openssl code paths");
            return;
        }

        EncryptionOptions options = addKeystoreOptions(encryptionOptions);
        SslContext sslContext = SSLFactory.getOrCreateSslContext(options, true, SSLFactory.SocketType.CLIENT, true);
        Assert.assertNotNull(sslContext);
        Assert.assertTrue(sslContext instanceof OpenSslContext);
    }

    @Test
    public void getSslContext_JdkSsl() throws IOException
    {
        EncryptionOptions options = addKeystoreOptions(encryptionOptions);
        SslContext sslContext = SSLFactory.getOrCreateSslContext(options, true, SSLFactory.SocketType.CLIENT, false);
        Assert.assertNotNull(sslContext);
        Assert.assertTrue(sslContext instanceof JdkSslContext);
        Assert.assertEquals(encryptionOptions.cipher_suites, sslContext.cipherSuites());
    }

    private ServerEncryptionOptions addKeystoreOptions(ServerEncryptionOptions options)
    {
        return options.withKeyStore("test/conf/cassandra_ssl_test.keystore")
                      .withKeyStorePassword("cassandra");
    }

    @Test(expected = IOException.class)
    public void buildTrustManagerFactory_NoFile() throws IOException
    {
        SSLFactory.buildTrustManagerFactory(encryptionOptions.withTrustStore("/this/is/probably/not/a/file/on/your/test/machine"));
    }

    @Test(expected = IOException.class)
    public void buildTrustManagerFactory_BadPassword() throws IOException
    {
        SSLFactory.buildTrustManagerFactory(encryptionOptions.withTrustStorePassword("HomeOfBadPasswords"));
    }

    @Test
    public void buildTrustManagerFactory_HappyPath() throws IOException
    {
        TrustManagerFactory trustManagerFactory = SSLFactory.buildTrustManagerFactory(encryptionOptions);
        Assert.assertNotNull(trustManagerFactory);
    }

    @Test(expected = IOException.class)
    public void buildKeyManagerFactory_NoFile() throws IOException
    {
        EncryptionOptions options = addKeystoreOptions(encryptionOptions)
                                    .withKeyStore("/this/is/probably/not/a/file/on/your/test/machine");
        SSLFactory.buildKeyManagerFactory(options);
    }

    @Test(expected = IOException.class)
    public void buildKeyManagerFactory_BadPassword() throws IOException
    {
        EncryptionOptions options = addKeystoreOptions(encryptionOptions)
                                    .withKeyStorePassword("HomeOfBadPasswords");
        SSLFactory.buildKeyManagerFactory(options);
    }

    @Test
    public void buildKeyManagerFactory_HappyPath() throws IOException
    {
        Assert.assertFalse(SSLFactory.checkedExpiry);
        EncryptionOptions options = addKeystoreOptions(encryptionOptions);
        SSLFactory.buildKeyManagerFactory(options);
        Assert.assertTrue(SSLFactory.checkedExpiry);
    }

    @Test
    public void testSslContextReload_HappyPath() throws IOException, InterruptedException
    {
        try
        {
            EncryptionOptions options = addKeystoreOptions(encryptionOptions)
                                        .withEnabled(true);

            SSLFactory.initHotReloading((ServerEncryptionOptions) options, options, true);

            SslContext oldCtx = SSLFactory.getOrCreateSslContext(options, true, SSLFactory.SocketType.CLIENT, OpenSsl
                                                                                                           .isAvailable());
            File keystoreFile = new File(options.keystore);

            SSLFactory.checkCertFilesForHotReloading((ServerEncryptionOptions) options, options);

            keystoreFile.setLastModified(System.currentTimeMillis() + 15000);

            SSLFactory.checkCertFilesForHotReloading((ServerEncryptionOptions) options, options);;
            SslContext newCtx = SSLFactory.getOrCreateSslContext(options, true, SSLFactory.SocketType.CLIENT, OpenSsl
                                                                                                          .isAvailable());

            Assert.assertNotSame(oldCtx, newCtx);
        }
        catch (Exception e)
        {
            throw e;
        }
        finally
        {
            DatabaseDescriptor.loadConfig();
        }
    }

    @Test(expected = IOException.class)
    public void testSslFactorySslInit_BadPassword_ThrowsException() throws IOException
    {
        EncryptionOptions options = addKeystoreOptions(encryptionOptions)
                                    .withKeyStorePassword("bad password")
                                    .withEnabled(true);

        SSLFactory.initHotReloading((ServerEncryptionOptions) options, options, true);
    }

    @Test
    public void testSslFactoryHotReload_BadPassword_DoesNotClearExistingSslContext() throws IOException
    {
        try
        {
            ServerEncryptionOptions options = addKeystoreOptions(encryptionOptions)
                                              .withEnabled(true);

            SSLFactory.initHotReloading(options, options, true);
            SslContext oldCtx = SSLFactory.getOrCreateSslContext(options, true, SSLFactory.SocketType.CLIENT, OpenSsl
                                                                                                          .isAvailable());
            File keystoreFile = new File(options.keystore);

            SSLFactory.checkCertFilesForHotReloading(options, options);
            keystoreFile.setLastModified(System.currentTimeMillis() + 5000);

            ServerEncryptionOptions modOptions = new ServerEncryptionOptions(options)
                                                 .withKeyStorePassword("bad password");
            SSLFactory.checkCertFilesForHotReloading(modOptions, modOptions);
            SslContext newCtx = SSLFactory.getOrCreateSslContext(options, true, SSLFactory.SocketType.CLIENT, OpenSsl
                                                                                                          .isAvailable());

            Assert.assertSame(oldCtx, newCtx);
        }
        finally
        {
            DatabaseDescriptor.loadConfig();
        }
    }

    @Test
    public void testSslFactoryHotReload_CorruptOrNonExistentFile_DoesNotClearExistingSslContext() throws IOException
    {
        try
        {
            ServerEncryptionOptions options = addKeystoreOptions(encryptionOptions);

            File testKeystoreFile = new File(options.keystore + ".test");
            FileUtils.copyFile(new File(options.keystore),testKeystoreFile);
            options = options
                      .withKeyStore(testKeystoreFile.getPath())
                      .withEnabled(true);


            SSLFactory.initHotReloading(options, options, true);
            SslContext oldCtx = SSLFactory.getOrCreateSslContext(options, true, SSLFactory.SocketType.CLIENT, OpenSsl
                                                                                                          .isAvailable());
            SSLFactory.checkCertFilesForHotReloading(options, options);

            testKeystoreFile.setLastModified(System.currentTimeMillis() + 15000);
            FileUtils.forceDelete(testKeystoreFile);

            SSLFactory.checkCertFilesForHotReloading(options, options);;
            SslContext newCtx = SSLFactory.getOrCreateSslContext(options, true, SSLFactory.SocketType.CLIENT, OpenSsl
                                                                                                          .isAvailable());

            Assert.assertSame(oldCtx, newCtx);
        }
        catch (Exception e)
        {
            throw e;
        }
        finally
        {
            DatabaseDescriptor.loadConfig();
            FileUtils.deleteQuietly(new File(encryptionOptions.keystore + ".test"));
        }
    }

    @Test
    public void getSslContext_ParamChanges() throws IOException
    {
        EncryptionOptions options = addKeystoreOptions(encryptionOptions)
                                    .withEnabled(true)
                                    .withCipherSuites("TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256");

        SslContext ctx1 = SSLFactory.getOrCreateSslContext(options, true,
                                                           SSLFactory.SocketType.SERVER, OpenSsl.isAvailable());

        Assert.assertTrue(ctx1.isServer());
        Assert.assertEquals(ctx1.cipherSuites(), options.cipher_suites);

        options = options.withCipherSuites("TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256");

        SslContext ctx2 = SSLFactory.getOrCreateSslContext(options, true,
                                                           SSLFactory.SocketType.CLIENT, OpenSsl.isAvailable());

        Assert.assertTrue(ctx2.isClient());
        Assert.assertEquals(ctx2.cipherSuites(), options.cipher_suites);
    }
}
