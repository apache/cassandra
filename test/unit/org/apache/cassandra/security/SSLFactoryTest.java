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
import java.nio.file.Files;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.security.cert.CertificateException;
import java.util.Collections;
import java.util.Set;
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
import static org.junit.Assume.assumeFalse;
import static org.junit.Assume.assumeNoException;

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
            throw new RuntimeException("failed to create test certs");
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
            ServerEncryptionOptions options = addKeystoreOptions(encryptionOptions)
                                              .withInternodeEncryption(ServerEncryptionOptions.InternodeEncryption.all);

            SSLFactory.initHotReloading(options, options, true);

            SslContext oldCtx = SSLFactory.getOrCreateSslContext(options, true, SSLFactory.SocketType.CLIENT, OpenSsl
                                                                                                           .isAvailable());
            File keystoreFile = new File(options.keystore);

            SSLFactory.checkCertFilesForHotReloading(options, options);

            keystoreFile.setLastModified(System.currentTimeMillis() + 15000);

            SSLFactory.checkCertFilesForHotReloading(options, options);
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
        ServerEncryptionOptions options = addKeystoreOptions(encryptionOptions)
                                    .withKeyStorePassword("bad password")
                                    .withInternodeEncryption(ServerEncryptionOptions.InternodeEncryption.all);

        SSLFactory.validateSslContext("testSslFactorySslInit_BadPassword_ThrowsException", options, false, true);
    }

    @Test
    public void testSslFactoryHotReload_BadPassword_DoesNotClearExistingSslContext() throws IOException
    {
        try
        {
            ServerEncryptionOptions options = addKeystoreOptions(encryptionOptions);

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
            options = options.withKeyStore(testKeystoreFile.getPath());


            SSLFactory.initHotReloading(options, options, true);
            SslContext oldCtx = SSLFactory.getOrCreateSslContext(options, true, SSLFactory.SocketType.CLIENT, OpenSsl
                                                                                                          .isAvailable());
            SSLFactory.checkCertFilesForHotReloading(options, options);

            testKeystoreFile.setLastModified(System.currentTimeMillis() + 15000);
            FileUtils.forceDelete(testKeystoreFile);

            SSLFactory.checkCertFilesForHotReloading(options, options);
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
    public void testHotReloadRetriesAndRecoversAfterFailedReload() throws IOException
    {
        ServerEncryptionOptions options = addKeystoreOptions(encryptionOptions);

        File testKeystoreFile = new File(options.keystore + ".test");
        FileUtils.copyFile(new File(options.keystore), testKeystoreFile);
        options = options.withKeyStore(testKeystoreFile.getPath());

        Set<PosixFilePermission> readable = PosixFilePermissions.fromString("rw-------");

        // Skip where POSIX perms are not enforced (non-POSIX FS, or running as root).
        try
        {
            Files.setPosixFilePermissions(testKeystoreFile.toPath(), Collections.emptySet());
        }
        catch (UnsupportedOperationException e)
        {
            assumeNoException("POSIX permissions unsupported on this filesystem", e);
        }
        assumeFalse("running as root; POSIX permissions not enforced", testKeystoreFile.canRead());
        Files.setPosixFilePermissions(testKeystoreFile.toPath(), readable);

        try
        {
            SSLFactory.initHotReloading(options, options, true);
            SslContext oldCtx = SSLFactory.getOrCreateSslContext(options, true, SSLFactory.SocketType.CLIENT, OpenSsl.isAvailable());
            SSLFactory.checkCertFilesForHotReloading(options, options);

            // New keystore written (fresh mtime) but unreadable by the process.
            testKeystoreFile.setLastModified(System.currentTimeMillis() + 15000);
            Files.setPosixFilePermissions(testKeystoreFile.toPath(), Collections.emptySet());

            SSLFactory.checkCertFilesForHotReloading(options, options); // AccessDeniedException -> logged, context retained
            SslContext afterFailCtx = SSLFactory.getOrCreateSslContext(options, true, SSLFactory.SocketType.CLIENT, OpenSsl.isAvailable());
            Assert.assertSame("failed reload must not replace the live context", oldCtx, afterFailCtx);

            // Fix permissions WITHOUT touching mtime (chmod changes ctime only).
            Files.setPosixFilePermissions(testKeystoreFile.toPath(), readable);

            // Periodic check must retry even though mtime is unchanged since the failed attempt.
            SSLFactory.checkCertFilesForHotReloading(options, options);
            SslContext recoveredCtx = SSLFactory.getOrCreateSslContext(options, true, SSLFactory.SocketType.CLIENT, OpenSsl.isAvailable());
            Assert.assertNotSame("reload must retry and recover once the file is readable", oldCtx, recoveredCtx);
        }
        finally
        {
            FileUtils.deleteQuietly(new File(encryptionOptions.keystore + ".test"));
        }
    }

    @Test
    public void testHotReloadResetsExpiryCheck() throws IOException
    {
        ServerEncryptionOptions options = addKeystoreOptions(encryptionOptions);

        File testKeystoreFile = new File(options.keystore + ".test");
        FileUtils.copyFile(new File(options.keystore), testKeystoreFile);
        options = options.withKeyStore(testKeystoreFile.getPath());

        try
        {
            SSLFactory.initHotReloading(options, options, true);
            SSLFactory.buildKeyManagerFactory(options);
            Assert.assertTrue("the initial load scans certificate expiry", SSLFactory.checkedExpiry);

            // A changed keystore that fails to load clears checkedExpiry, and it stays clear until a load succeeds.
            testKeystoreFile.setLastModified(System.currentTimeMillis() + 15000);
            ServerEncryptionOptions badPassword = options.withKeyStorePassword("bad password");
            SSLFactory.checkCertFilesForHotReloading(badPassword, badPassword);
            Assert.assertFalse("a reload attempt clears the expiry check flag", SSLFactory.checkedExpiry);

            // The retry loads the keystore and scans expiry again.
            SSLFactory.checkCertFilesForHotReloading(options, options);
            Assert.assertTrue("the successful retry scans expiry again", SSLFactory.checkedExpiry);
        }
        finally
        {
            FileUtils.deleteQuietly(new File(encryptionOptions.keystore + ".test"));
        }
    }

    @Test
    public void testHotReloadableFileCommitsOnlyChangesThatPredateTheCheck()
    {
        // commit() advances lastModTime only to the mtime that the most recent shouldReload() observed, and only
        // if the file has not changed again since. This protects against a new certificate landing while a reload
        // is in flight. That later write must NOT be masked, so shouldReload() stays true and the change is retried
        // next period. The comparison is mtime-vs-mtime (no wall clock), so even a future-dated mtime settles after
        // one successful commit rather than reloading forever.
        File tmp = org.apache.cassandra.io.util.FileUtils.createTempFile("ssl-hot-reload", ".test");
        try
        {
            SSLFactory.HotReloadableFile file = new SSLFactory.HotReloadableFile(tmp.getPath());
            Assert.assertFalse("no change since construction", file.shouldReload());

            // A new cert lands and mtime advances, so a reload is due.
            tmp.setLastModified(tmp.lastModified() + 60_000);
            long mtime = tmp.lastModified();
            Assert.assertTrue("mtime change is detected", file.shouldReload());

            // A second write lands after this check observed the file but before commit (reload still in flight).
            // commit() sees the file changed since shouldReload() observed it, so it must not commit. The reload
            // stays due so the newer contents are picked up next period.
            tmp.setLastModified(mtime + 60_000);
            file.commit();
            Assert.assertTrue("a change after the check is not committed", file.shouldReload());

            // The shouldReload() above re-observed the now-settled mtime; with no further change, commit() records
            // it and the reload is no longer due.
            file.commit();
            Assert.assertFalse("a settled change is committed", file.shouldReload());

            // A future-dated mtime (clock skew, or an mtime-preserving copy) must also settle after one commit
            // rather than reloading on every check.
            tmp.setLastModified(System.currentTimeMillis() + 3_600_000);
            Assert.assertTrue("future-dated mtime is detected", file.shouldReload());
            file.commit();
            Assert.assertFalse("future-dated change settles after one commit", file.shouldReload());
        }
        finally
        {
            FileUtils.deleteQuietly(tmp);
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
