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

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.Certificate;
import java.security.cert.CertificateException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import javax.net.ssl.SSLException;
import javax.net.ssl.X509KeyManager;

import org.apache.commons.io.FileUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.EncryptionOptions;
import org.apache.cassandra.config.EncryptionOptions.ServerEncryptionOptions;
import org.apache.cassandra.config.EncryptionOptions.ServerEncryptionOptions.Builder;
import org.apache.cassandra.config.ParameterizedClass;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.transport.TlsTestUtils;
import org.apache.cassandra.utils.Clock;

import io.netty.handler.ssl.CipherSuiteFilter;
import io.netty.handler.ssl.OpenSslClientContext;
import io.netty.handler.ssl.OpenSslServerContext;
import io.netty.handler.ssl.OpenSslSessionContext;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.util.SelfSignedCertificate;

import static org.apache.cassandra.config.EncryptionOptions.ClientEncryptionOptions.ClientAuth.NOT_REQUIRED;
import static org.apache.cassandra.config.EncryptionOptions.ClientEncryptionOptions.ClientAuth.REQUIRED;
import static org.apache.cassandra.io.util.FileUtils.createDeletableTempFile;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assume.assumeFalse;
import static org.junit.Assume.assumeNoException;

public class SSLFactoryTest
{
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
        SSLFactory.clearSslContextCache();
        encryptionOptions = new Builder().withTrustStore(TlsTestUtils.SERVER_TRUSTSTORE_PATH)
                                         .withTrustStorePassword(TlsTestUtils.SERVER_TRUSTSTORE_PASSWORD)
                                         .withRequireClientAuth(NOT_REQUIRED)
                                         .withCipherSuites("TLS_RSA_WITH_AES_128_CBC_SHA")
                                         .withSslContextFactory(new ParameterizedClass(TestFileBasedSSLContextFactory.class.getName(),
                                                                                       new HashMap<>()))
                                         .build();
    }

    private Builder addKeystoreOptions(ServerEncryptionOptions options)
    {
        EncryptionOptions.ServerEncryptionOptions.Builder builder = new EncryptionOptions.ServerEncryptionOptions.Builder(options);

        builder.withOutboundKeystorePassword(TlsTestUtils.SERVER_OUTBOUND_KEYSTORE_PASSWORD)
               .withOutboundKeystore(TlsTestUtils.SERVER_OUTBOUND_KEYSTORE_PATH)
               .withKeyStorePassword(TlsTestUtils.SERVER_KEYSTORE_PASSWORD)
               .withKeyStore(TlsTestUtils.SERVER_KEYSTORE_PATH);

        return builder;
    }

    private Builder addPEMKeystoreOptions(ServerEncryptionOptions options)
    {
        ParameterizedClass sslContextFactoryClass = new ParameterizedClass("org.apache.cassandra.security.PEMBasedSslContextFactory",
                                                                           new HashMap<>());
        EncryptionOptions.ServerEncryptionOptions.Builder builder = new EncryptionOptions.ServerEncryptionOptions.Builder(options);

        builder.withOutboundKeystore(TlsTestUtils.SERVER_KEYSTORE_PATH_PEM)
               .withOutboundKeystorePassword(TlsTestUtils.SERVER_KEYSTORE_PASSWORD)
               .withSslContextFactory(sslContextFactoryClass)
               .withKeyStore(TlsTestUtils.SERVER_KEYSTORE_PATH_PEM)
               .withKeyStorePassword(TlsTestUtils.SERVER_KEYSTORE_PASSWORD)
               .withTrustStore(TlsTestUtils.SERVER_TRUSTSTORE_PEM_PATH);

        return builder;
    }

    @Test
    public void testSslContextReload_HappyPath() throws IOException, InterruptedException
    {
        try
        {
            Builder optionsBuilder = addKeystoreOptions(encryptionOptions)
                                     .withInternodeEncryption(ServerEncryptionOptions.InternodeEncryption.all);
            ServerEncryptionOptions options = optionsBuilder.build();
            ServerEncryptionOptions legacyOptions = optionsBuilder
                                                    .withInternodeEncryption(ServerEncryptionOptions.InternodeEncryption.all)
                                                    .withOptional(false)
                                                    .build();
            options.sslContextFactoryInstance.initHotReloading();
            legacyOptions.sslContextFactoryInstance.initHotReloading();

            SslContext oldCtx = SSLFactory.getOrCreateSslContext(options, REQUIRED, ISslContextFactory.SocketType.CLIENT, "test");
            SslContext oldLegacyCtx = SSLFactory.getOrCreateSslContext(legacyOptions, REQUIRED, ISslContextFactory.SocketType.CLIENT, "test legacy");
            File keystoreFile = new File(options.keystore);

            SSLFactory.checkCertFilesForHotReloading();

            keystoreFile.trySetLastModified(System.currentTimeMillis() + 15000);

            SSLFactory.checkCertFilesForHotReloading();
            SslContext newCtx = SSLFactory.getOrCreateSslContext(options, REQUIRED, ISslContextFactory.SocketType.CLIENT, "test");
            SslContext newLegacyCtx = SSLFactory.getOrCreateSslContext(legacyOptions, REQUIRED, ISslContextFactory.SocketType.CLIENT, "test legacy");

            Assert.assertNotSame(oldCtx, newCtx);
            Assert.assertNotSame(oldLegacyCtx, newLegacyCtx);
        }
        catch (Exception e)
        {
            throw e;
        }
    }

    @Test
    public void testServerSocketShouldUseKeystore() throws IOException, CertificateException, KeyStoreException, NoSuchAlgorithmException, NoSuchFieldException, IllegalAccessException, ClassNotFoundException, NoSuchMethodException, InvocationTargetException
    {
        ServerEncryptionOptions options = addKeystoreOptions(encryptionOptions)
                                          .withOutboundKeystore("dummyKeystore")
                                          .withOutboundKeystorePassword("dummyPassword")
                                          .build();

        // Server socket type should create a keystore with keystore & keystore password
        final OpenSslServerContext context = (OpenSslServerContext) SSLFactory.createNettySslContext(options, REQUIRED, ISslContextFactory.SocketType.SERVER);
        assertNotNull(context);

        // Verify if right certificate is loaded into SslContext
        final Certificate loadedCertificate = getCertificateLoadedInSslContext(context.sessionContext());
        final Certificate certificate = getCertificates(TlsTestUtils.SERVER_KEYSTORE_PATH, TlsTestUtils.SERVER_KEYSTORE_PASSWORD);
        assertEquals(loadedCertificate, certificate);
    }

    @Test
    public void testClientSocketShouldUseOutboundKeystore() throws IOException, CertificateException, KeyStoreException, NoSuchAlgorithmException, NoSuchFieldException, ClassNotFoundException, InvocationTargetException, IllegalAccessException, NoSuchMethodException
    {
        ServerEncryptionOptions options = addKeystoreOptions(encryptionOptions)
                                          .withKeyStore("dummyKeystore")
                                          .withKeyStorePassword("dummyPassword")
                                          .build();

        // Client socket type should create a keystore with outbound Keystore & outbound password
        final OpenSslClientContext context = (OpenSslClientContext) SSLFactory.createNettySslContext(options, REQUIRED, ISslContextFactory.SocketType.CLIENT);
        assertNotNull(context);

        // Verify if right certificate is loaded into SslContext
        final Certificate loadedCertificate = getCertificateLoadedInSslContext(context.sessionContext());
        final Certificate certificate = getCertificates(TlsTestUtils.SERVER_OUTBOUND_KEYSTORE_PATH, TlsTestUtils.SERVER_OUTBOUND_KEYSTORE_PASSWORD);
        assertEquals(loadedCertificate, certificate);
    }

    @Test
    public void testPEMSslContextReload_HappyPath() throws IOException
    {
        try
        {
            Builder optionsBuilder = addPEMKeystoreOptions(encryptionOptions);
            ServerEncryptionOptions options = optionsBuilder.withInternodeEncryption(ServerEncryptionOptions.InternodeEncryption.dc)
                                                            .build();
            // emulate InboundSockets and share the cert but with different options, no extra hot reloading init
            ServerEncryptionOptions legacyOptions = optionsBuilder
                                                    .withInternodeEncryption(ServerEncryptionOptions.InternodeEncryption.all)
                                                    .withOptional(false)
                                                    .build();
            options.sslContextFactoryInstance.initHotReloading();
            legacyOptions.sslContextFactoryInstance.initHotReloading();

            SslContext oldCtx = SSLFactory.getOrCreateSslContext(options, REQUIRED, ISslContextFactory.SocketType.CLIENT, "test");
            SslContext oldLegacyCtx = SSLFactory.getOrCreateSslContext(legacyOptions, REQUIRED, ISslContextFactory.SocketType.CLIENT, "test legacy");
            File keystoreFile = new File(options.keystore);

            SSLFactory.checkCertFilesForHotReloading();

            keystoreFile.trySetLastModified(System.currentTimeMillis() + 15000);

            SSLFactory.checkCertFilesForHotReloading();
            SslContext newCtx = SSLFactory.getOrCreateSslContext(options, REQUIRED, ISslContextFactory.SocketType.CLIENT, "test");
            SslContext newLegacyCtx = SSLFactory.getOrCreateSslContext(legacyOptions, REQUIRED, ISslContextFactory.SocketType.CLIENT, "test legacy");

            Assert.assertNotSame(oldCtx, newCtx);
            Assert.assertNotSame(oldLegacyCtx, newLegacyCtx);
        }
        catch (Exception e)
        {
            throw e;
        }
    }

    @Test(expected = IOException.class)
    public void testSslFactorySslInit_BadPassword_ThrowsException() throws IOException
    {
        ServerEncryptionOptions options = addKeystoreOptions(encryptionOptions)
                                          .withInternodeEncryption(ServerEncryptionOptions.InternodeEncryption.all)
                                          .withKeyStorePassword("bad password")
                                          .build();

        SSLFactory.validateSslContext("testSslFactorySslInit_BadPassword_ThrowsException", options, NOT_REQUIRED, true);
    }

    @Test
    public void testSslFactoryHotReload_BadPassword_DoesNotClearExistingSslContext() throws IOException
    {
        Builder optionsBuilder = addKeystoreOptions(encryptionOptions);
        ServerEncryptionOptions options = optionsBuilder.build();
        // emulate InboundSockets and share the cert but with different options, no extra hot reloading init
        ServerEncryptionOptions legacyOptions = optionsBuilder
                                                .withInternodeEncryption(ServerEncryptionOptions.InternodeEncryption.all)
                                                .withOptional(false)
                                                .build();

        File testKeystoreFile = new File(options.keystore + ".test");
        FileUtils.copyFile(new File(options.keystore).toJavaIOFile(), testKeystoreFile.toJavaIOFile());
        options = new Builder(options).withKeyStore(testKeystoreFile.path()).build();

        SSLFactory.initHotReloading(options, options, true);  // deliberately not initializing with legacyOptions to match InboundSockets.addBindings

        SslContext oldCtx = SSLFactory.getOrCreateSslContext(options, REQUIRED, ISslContextFactory.SocketType.CLIENT, "test");
        SslContext oldLegacyCtx = SSLFactory.getOrCreateSslContext(legacyOptions, REQUIRED, ISslContextFactory.SocketType.CLIENT, "test legacy");

        changeKeystorePassword(options.keystore, options.keystore_password, "bad password");

        SSLFactory.checkCertFilesForHotReloading();
        SslContext newCtx = SSLFactory.getOrCreateSslContext(options, REQUIRED, ISslContextFactory.SocketType.CLIENT, "test");
        SslContext newLegacyCtx = SSLFactory.getOrCreateSslContext(legacyOptions, REQUIRED, ISslContextFactory.SocketType.CLIENT, "test legacy");

        Assert.assertSame(oldCtx, newCtx);
        Assert.assertSame(oldLegacyCtx, newLegacyCtx);
    }

    @Test
    public void testSslFactoryHotReload_CorruptOrNonExistentFile_DoesNotClearExistingSslContext() throws IOException
    {
        try
        {
            Builder optionsBuilder = addKeystoreOptions(encryptionOptions);
            ServerEncryptionOptions options = optionsBuilder.build();

            File testKeystoreFile = new File(options.keystore + ".test");
            FileUtils.copyFile(new File(options.keystore).toJavaIOFile(), testKeystoreFile.toJavaIOFile());
            options = optionsBuilder.withKeyStore(testKeystoreFile.path()).build();


            SSLFactory.initHotReloading(options, options, true);
            SslContext oldCtx = SSLFactory.getOrCreateSslContext(options, REQUIRED, ISslContextFactory.SocketType.CLIENT, "test");
            SSLFactory.checkCertFilesForHotReloading();

            testKeystoreFile.trySetLastModified(System.currentTimeMillis() + 15000);
            FileUtils.forceDelete(testKeystoreFile.toJavaIOFile());

            SSLFactory.checkCertFilesForHotReloading();
            SslContext newCtx = SSLFactory.getOrCreateSslContext(options, REQUIRED, ISslContextFactory.SocketType.CLIENT, "test");

            Assert.assertSame(oldCtx, newCtx);
        }
        catch (Exception e)
        {
            throw e;
        }
        finally
        {
            FileUtils.deleteQuietly(new File(encryptionOptions.keystore + ".test").toJavaIOFile());
        }
    }

    @Test
    public void testHotReloadRetriesAndRecoversAfterFailedReload() throws IOException
    {
        ServerEncryptionOptions options = addKeystoreOptions(encryptionOptions).build();

        File testKeystoreFile = new File(options.keystore + ".test");
        FileUtils.copyFile(new File(options.keystore).toJavaIOFile(), testKeystoreFile.toJavaIOFile());
        options = new Builder(options).withKeyStore(testKeystoreFile.path()).build();

        java.io.File jio = testKeystoreFile.toJavaIOFile();
        Set<PosixFilePermission> readable =
            PosixFilePermissions.fromString("rw-------");

        // Skip where POSIX perms are not enforced (non-POSIX FS, or running as root).
        try
        {
            java.nio.file.Files.setPosixFilePermissions(jio.toPath(), Collections.emptySet());
        }
        catch (UnsupportedOperationException e)
        {
            assumeNoException("POSIX permissions unsupported on this filesystem", e);
        }
        assumeFalse("running as root; POSIX permissions not enforced", jio.canRead());
        java.nio.file.Files.setPosixFilePermissions(jio.toPath(), readable);

        try
        {
            SSLFactory.initHotReloading(options, options, true);
            SslContext oldCtx = SSLFactory.getOrCreateSslContext(options, REQUIRED, ISslContextFactory.SocketType.CLIENT, "test");
            SSLFactory.checkCertFilesForHotReloading();

            // New keystore written (fresh mtime) but unreadable by the process.
            testKeystoreFile.trySetLastModified(System.currentTimeMillis() + 15000);
            java.nio.file.Files.setPosixFilePermissions(jio.toPath(), Collections.emptySet());

            SSLFactory.checkCertFilesForHotReloading(); // AccessDeniedException -> logged, context retained
            SslContext afterFailCtx = SSLFactory.getOrCreateSslContext(options, REQUIRED, ISslContextFactory.SocketType.CLIENT, "test");
            Assert.assertSame("failed reload must not replace the live context", oldCtx, afterFailCtx);

            // Fix permissions WITHOUT touching mtime (chmod changes ctime only).
            java.nio.file.Files.setPosixFilePermissions(jio.toPath(), readable);

            // Periodic check must retry even though mtime is unchanged since the failed attempt.
            SSLFactory.checkCertFilesForHotReloading();
            SslContext recoveredCtx = SSLFactory.getOrCreateSslContext(options, REQUIRED, ISslContextFactory.SocketType.CLIENT, "test");
            Assert.assertNotSame("reload must retry and recover once the file is readable", oldCtx, recoveredCtx);
        }
        finally
        {
            FileUtils.deleteQuietly(new File(encryptionOptions.keystore + ".test").toJavaIOFile());
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
        File tmp = createDeletableTempFile("ssl-hot-reload", ".test");
        try
        {
            FileBasedSslContextFactory.HotReloadableFile file = new FileBasedSslContextFactory.HotReloadableFile(tmp.path());
            Assert.assertFalse("no change since construction", file.shouldReload());

            // A new cert lands and mtime advances, so a reload is due.
            tmp.trySetLastModified(tmp.lastModified() + 60_000);
            long mtime = tmp.lastModified();
            Assert.assertTrue("mtime change is detected", file.shouldReload());

            // A second write lands after this check observed the file but before commit (reload still in flight).
            // commit() sees the file changed since shouldReload() observed it, so it must not commit. The reload
            // stays due so the newer contents are picked up next period.
            tmp.trySetLastModified(mtime + 60_000);
            file.commit();
            Assert.assertTrue("a change after the check is not committed", file.shouldReload());

            // The shouldReload() above re-observed the now-settled mtime; with no further change, commit() records
            // it and the reload is no longer due.
            file.commit();
            Assert.assertFalse("a settled change is committed", file.shouldReload());

            // A future-dated mtime (clock skew, or an mtime-preserving copy) must also settle after one commit
            // rather than reloading on every check.
            tmp.trySetLastModified(System.currentTimeMillis() + 3_600_000);
            Assert.assertTrue("future-dated mtime is detected", file.shouldReload());
            file.commit();
            Assert.assertFalse("future-dated change settles after one commit", file.shouldReload());
        }
        finally
        {
            FileUtils.deleteQuietly(tmp.toJavaIOFile());
        }
    }

    @Test
    public void getSslContext_ParamChanges() throws IOException
    {
        Builder optionsBuilder = addKeystoreOptions(encryptionOptions);
        ServerEncryptionOptions options = optionsBuilder
                                          .withCipherSuites("TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256")
                                          .build();

        SslContext ctx1 = SSLFactory.getOrCreateSslContext(options, REQUIRED,
                                                           ISslContextFactory.SocketType.SERVER, "test");

        Assert.assertTrue(ctx1.isServer());
        Assert.assertEquals(ctx1.cipherSuites(), options.cipher_suites);

        options = optionsBuilder.withCipherSuites("TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256").build();

        SslContext ctx2 = SSLFactory.getOrCreateSslContext(options, REQUIRED,
                                                           ISslContextFactory.SocketType.CLIENT, "test");

        Assert.assertTrue(ctx2.isClient());
        Assert.assertEquals(ctx2.cipherSuites(), options.cipher_suites);
    }

    @Test
    public void testCacheKeyEqualityForCustomSslContextFactory()
    {

        Map<String, String> parameters1 = new HashMap<>();
        parameters1.put("key1", "value1");
        parameters1.put("key2", "value2");
        EncryptionOptions.ClientEncryptionOptions encryptionOptions1 =
        new EncryptionOptions.ClientEncryptionOptions.Builder()
        .withSslContextFactory(new ParameterizedClass(DummySslContextFactoryImpl.class.getName(), parameters1))
        .withProtocol("TLSv1.1")
        .withRequireClientAuth(REQUIRED)
        .withRequireEndpointVerification(false)
        .build();

        SSLFactory.CacheKey cacheKey1 = new SSLFactory.CacheKey(encryptionOptions1, ISslContextFactory.SocketType.SERVER, "test"
        );

        Map<String, String> parameters2 = new HashMap<>();
        parameters2.put("key1", "value1");
        parameters2.put("key2", "value2");
        EncryptionOptions.ClientEncryptionOptions encryptionOptions2 =
        new EncryptionOptions.ClientEncryptionOptions.Builder()
        .withSslContextFactory(new ParameterizedClass(DummySslContextFactoryImpl.class.getName(), parameters2))
        .withProtocol("TLSv1.1")
        .withRequireClientAuth(REQUIRED)
        .withRequireEndpointVerification(false)
        .build();

        SSLFactory.CacheKey cacheKey2 = new SSLFactory.CacheKey(encryptionOptions2, ISslContextFactory.SocketType.SERVER, "test"
        );

        Assert.assertEquals(cacheKey1, cacheKey2);
    }

    @Test
    public void testCacheKeyInequalityForCustomSslContextFactory()
    {

        Map<String, String> parameters1 = new HashMap<>();
        parameters1.put("key1", "value11");
        parameters1.put("key2", "value12");
        EncryptionOptions.ClientEncryptionOptions encryptionOptions1 =
        new EncryptionOptions.ClientEncryptionOptions.Builder()
        .withSslContextFactory(new ParameterizedClass(DummySslContextFactoryImpl.class.getName(), parameters1))
        .withProtocol("TLSv1.1")
        .build();

        SSLFactory.CacheKey cacheKey1 = new SSLFactory.CacheKey(encryptionOptions1, ISslContextFactory.SocketType.SERVER, "test"
        );

        Map<String, String> parameters2 = new HashMap<>();
        parameters2.put("key1", "value21");
        parameters2.put("key2", "value22");
        EncryptionOptions.ClientEncryptionOptions encryptionOptions2 =
        new EncryptionOptions.ClientEncryptionOptions.Builder()
        .withSslContextFactory(new ParameterizedClass(DummySslContextFactoryImpl.class.getName(), parameters2))
        .withProtocol("TLSv1.1")
        .build();

        SSLFactory.CacheKey cacheKey2 = new SSLFactory.CacheKey(encryptionOptions2, ISslContextFactory.SocketType.SERVER, "test"
        );

        Assert.assertNotEquals(cacheKey1, cacheKey2);
    }

    @Test
    public void testHotReloadChecksExpiryOnEachReload() throws IOException
    {
        ServerEncryptionOptions options = addKeystoreOptions(encryptionOptions).build();
        File testKeystoreFile = new File(options.keystore + ".test");
        FileUtils.copyFile(new File(options.keystore).toJavaIOFile(), testKeystoreFile.toJavaIOFile());
        options = new Builder(options).withKeyStore(testKeystoreFile.path()).build();

        try
        {
            TestFileBasedSSLContextFactory.expiryCheckCount = 0;

            // Init hot reloading and load BOTH inbound (SERVER) and outbound (CLIENT) contexts
            // so that both keystoreContext.checkedExpiry and outboundKeystoreContext.checkedExpiry
            // are latched to true before any reload.
            SSLFactory.initHotReloading(options, options, true);
            SSLFactory.getOrCreateSslContext(options, REQUIRED, ISslContextFactory.SocketType.CLIENT, "test");
            SSLFactory.getOrCreateSslContext(options, REQUIRED, ISslContextFactory.SocketType.SERVER, "test");
            int afterInitialLoad = TestFileBasedSSLContextFactory.expiryCheckCount;

            // First reload — both contexts were already loaded above so checkedExpiry is
            // already true for both.  After this reload the cache is cleared; re-populate it so
            // the second reload also has entries to process.
            testKeystoreFile.trySetLastModified(System.currentTimeMillis() + 15000);
            SSLFactory.checkCertFilesForHotReloading();
            // Re-populate the cache so the second reload has something to process.
            SSLFactory.getOrCreateSslContext(options, REQUIRED, ISslContextFactory.SocketType.CLIENT, "test");
            SSLFactory.getOrCreateSslContext(options, REQUIRED, ISslContextFactory.SocketType.SERVER, "test");
            int afterFirstReload = TestFileBasedSSLContextFactory.expiryCheckCount;
            Assert.assertTrue("expiry scanned at least once across init + first reload", afterFirstReload >= 1);

            // Second reload. Both contexts are already loaded and both
            // checkedExpiry flags are latched true. onReloadRequired() resets
            // both flags so expiry IS re-scanned and expiryCheckCount increases.
            testKeystoreFile.trySetLastModified(System.currentTimeMillis() + 30000);
            SSLFactory.checkCertFilesForHotReloading();
            int afterSecondReload = TestFileBasedSSLContextFactory.expiryCheckCount;
            Assert.assertTrue("expiry re-scanned after second reload (both contexts already latched); afterInitialLoad=" + afterInitialLoad + " afterFirstReload=" + afterFirstReload + " afterSecondReload=" + afterSecondReload,
                               afterSecondReload > afterFirstReload);

            // no file change - no additional scan.
            SSLFactory.checkCertFilesForHotReloading();
            Assert.assertEquals("no re-scan when the file is unchanged", afterSecondReload,
                                 TestFileBasedSSLContextFactory.expiryCheckCount);
        }
        finally
        {
            FileUtils.deleteQuietly(new File(encryptionOptions.keystore + ".test").toJavaIOFile());
        }
    }

    @Test
    public void testExpiryWarningScheduledOnLoadAndRescheduledOnReload() throws Exception
    {
        Map<String, Object> params = new HashMap<>();
        params.put("keystore", TlsTestUtils.SERVER_KEYSTORE_PATH);
        params.put("keystore_password", TlsTestUtils.SERVER_KEYSTORE_PASSWORD);
        TestFileBasedSSLContextFactory factory = new TestFileBasedSSLContextFactory(params);

        // Build a keystore holding a future-dated cert (ssc validity is ~1 year).
        KeyStore ks = KeyStore.getInstance("PKCS12");
        ks.load(null, null);
        ks.setCertificateEntry("test", ssc.cert());
        Map<String, Date> expiries = factory.certificateExpiries(ks);

        factory.scheduleExpiryWarning(factory.keystoreContext, expiries, Clock.Global.currentTimeMillis());
        ScheduledFuture<?> first = factory.keystoreContext.expiryWarningFuture;
        Assert.assertNotNull("a future-dated cert schedules an expiry warning", first);
        Assert.assertFalse(first.isCancelled());

        // A reload reschedules: the prior future is cancelled and replaced.
        factory.scheduleExpiryWarning(factory.keystoreContext, expiries, Clock.Global.currentTimeMillis());
        ScheduledFuture<?> second = factory.keystoreContext.expiryWarningFuture;
        Assert.assertTrue("previous warning cancelled on reschedule", first.isCancelled());
        Assert.assertNotSame(first, second);

        // Client (outbound) and server (inbound) contexts track independent futures.
        factory.scheduleExpiryWarning(factory.outboundKeystoreContext, expiries, Clock.Global.currentTimeMillis());
        Assert.assertNotNull(factory.outboundKeystoreContext.expiryWarningFuture);
        Assert.assertNotSame(factory.keystoreContext.expiryWarningFuture, factory.outboundKeystoreContext.expiryWarningFuture);

        // Clean up scheduled tasks.
        factory.keystoreContext.expiryWarningFuture.cancel(false);
        factory.outboundKeystoreContext.expiryWarningFuture.cancel(false);
    }

    @Test
    public void testCertificateExpiriesCollectsNotAfterPerAlias() throws Exception
    {
        Map<String, Object> params = new HashMap<>();
        params.put("keystore", TlsTestUtils.SERVER_KEYSTORE_PATH);
        params.put("keystore_password", TlsTestUtils.SERVER_KEYSTORE_PASSWORD);
        TestFileBasedSSLContextFactory factory = new TestFileBasedSSLContextFactory(params);

        Date now = new Date();
        SelfSignedCertificate soon = new SelfSignedCertificate(new Date(now.getTime() - 1000),
                                                               new Date(now.getTime() + 60_000));
        SelfSignedCertificate later = new SelfSignedCertificate(new Date(now.getTime() - 1000),
                                                                new Date(now.getTime() + 3_600_000));

        KeyStore ks = KeyStore.getInstance("PKCS12");
        ks.load(null, null);
        ks.setCertificateEntry("soon", soon.cert());
        ks.setCertificateEntry("later", later.cert());

        Map<String, Date> expiries = factory.certificateExpiries(ks);
        assertEquals("every X.509 alias is collected", 2, expiries.size());
        assertEquals(soon.cert().getNotAfter(), expiries.get("soon"));
        assertEquals(later.cert().getNotAfter(), expiries.get("later"));

        // checkExpiredCerts and earliestExpiry share this scan, so they agree on the entries seen.
        assertEquals("earliestExpiry returns the minimum notAfter",
                     soon.cert().getNotAfter(), factory.earliestExpiry(expiries));
    }

    @Test
    public void testEarliestExpirySkewedByExtraEarlierCertificate() throws Exception
    {
        // Documents the known limitation noted on certificateExpiries: the scan is store-wide, so an unnecessary
        // extra entry that expires earlier than the certificate actually in use pulls earliestExpiry earlier. This
        // asserts current behavior (warning may be misattributed); it is not a handshake correctness issue.
        Map<String, Object> params = new HashMap<>();
        params.put("keystore", TlsTestUtils.SERVER_KEYSTORE_PATH);
        params.put("keystore_password", TlsTestUtils.SERVER_KEYSTORE_PASSWORD);
        TestFileBasedSSLContextFactory factory = new TestFileBasedSSLContextFactory(params);

        Date now = new Date();
        SelfSignedCertificate required = new SelfSignedCertificate(new Date(now.getTime() - 1000),
                                                                   new Date(now.getTime() + 3_600_000));
        SelfSignedCertificate straggler = new SelfSignedCertificate(new Date(now.getTime() - 1000),
                                                                    new Date(now.getTime() + 60_000));

        KeyStore ks = KeyStore.getInstance("PKCS12");
        ks.load(null, null);
        ks.setCertificateEntry("required", required.cert());
        ks.setCertificateEntry("straggler", straggler.cert());

        assertEquals("earliestExpiry is skewed to the earlier, unrelated certificate",
                     straggler.cert().getNotAfter(),
                     factory.earliestExpiry(factory.certificateExpiries(ks)));
    }

    @Test
    public void testCheckExpiredCertsOverrideIsDetected()
    {
        Map<String, Object> params = new HashMap<>();
        params.put("keystore", TlsTestUtils.SERVER_KEYSTORE_PATH);
        params.put("keystore_password", TlsTestUtils.SERVER_KEYSTORE_PASSWORD);

        // A subclass overriding checkExpiredCerts(KeyStore) is detected, so its override is honored on reload.
        Assert.assertTrue("overriding subclass is detected", new TestFileBasedSSLContextFactory(params).checkExpiredCertsOverridden);
        // A subclass that does not override it takes the fast map-based path (no redundant keystore scan).
        Assert.assertFalse("non-overriding subclass takes the map path", new PlainFileBasedSSLContextFactory(params).checkExpiredCertsOverridden);
    }

    @Test
    public void testConcurrentCallersShareOneBuild() throws Exception
    {
        SlowCountingSslContextFactory.builds.set(0);
        SlowCountingSslContextFactory.clearCacheWhileBuilding = false;
        EncryptionOptions options = slowFactoryOptions();

        int callers = 8;
        ExecutorService executor = Executors.newFixedThreadPool(callers);
        try
        {
            List<Future<SslContext>> results = new ArrayList<>();
            for (int i = 0; i < callers; i++)
                results.add(executor.submit(() -> SSLFactory.getOrCreateSslContext(options, REQUIRED, ISslContextFactory.SocketType.CLIENT, "shared")));

            SslContext first = results.get(0).get(30, TimeUnit.SECONDS);
            for (Future<SslContext> result : results)
                Assert.assertSame("every caller gets the one cached context", first, result.get(30, TimeUnit.SECONDS));
            assertEquals("concurrent callers share one build", 1, SlowCountingSslContextFactory.builds.get());
        }
        finally
        {
            executor.shutdownNow();
        }
    }

    @Test(timeout = 60_000)
    public void testClearingTheCacheWhileBuildingIsNotCached() throws Exception
    {
        SlowCountingSslContextFactory.builds.set(0);
        SlowCountingSslContextFactory.clearCacheWhileBuilding = true;
        try
        {
            EncryptionOptions options = slowFactoryOptions();
            SslContext built = SSLFactory.getOrCreateSslContext(options, REQUIRED, ISslContextFactory.SocketType.CLIENT, "clearing");
            assertNotNull(built);

            // The clear happened during the build, so the finished context must not be left in the cache.
            SslContext next = SSLFactory.getOrCreateSslContext(options, REQUIRED, ISslContextFactory.SocketType.CLIENT, "clearing");
            Assert.assertNotSame("a context built across a cache clear is not cached", built, next);
            assertEquals(2, SlowCountingSslContextFactory.builds.get());
        }
        finally
        {
            SlowCountingSslContextFactory.clearCacheWhileBuilding = false;
        }
    }

    @Test
    public void testFailedBuildIsNotCached() throws Exception
    {
        SlowCountingSslContextFactory.builds.set(0);
        SlowCountingSslContextFactory.clearCacheWhileBuilding = false;
        SlowCountingSslContextFactory.failBuilds = true;
        try
        {
            EncryptionOptions options = slowFactoryOptions();
            try
            {
                SSLFactory.getOrCreateSslContext(options, REQUIRED, ISslContextFactory.SocketType.CLIENT, "failing");
                Assert.fail("the build should have failed");
            }
            catch (SSLException expected)
            {
                // the original exception reaches the caller unwrapped
            }

            SlowCountingSslContextFactory.failBuilds = false;
            assertNotNull("a failed build must not be cached",
                          SSLFactory.getOrCreateSslContext(options, REQUIRED, ISslContextFactory.SocketType.CLIENT, "failing"));
            assertEquals(2, SlowCountingSslContextFactory.builds.get());
        }
        finally
        {
            SlowCountingSslContextFactory.failBuilds = false;
        }
    }

    private EncryptionOptions slowFactoryOptions()
    {
        Builder builder = addKeystoreOptions(encryptionOptions);
        builder.withSslContextFactory(new ParameterizedClass(SlowCountingSslContextFactory.class.getName(), new HashMap<>()));
        return builder.build();
    }

    /** Counts builds, takes 200ms per build, and can clear the context cache from inside a build. */
    public static class SlowCountingSslContextFactory extends FileBasedSslContextFactory
    {
        static final AtomicInteger builds = new AtomicInteger();
        static volatile boolean clearCacheWhileBuilding = false;
        static volatile boolean failBuilds = false;

        public SlowCountingSslContextFactory(Map<String, Object> parameters)
        {
            super(parameters);
        }

        @Override
        public SslContext createNettySslContext(EncryptionOptions.ClientEncryptionOptions.ClientAuth clientAuth,
                                                SocketType socketType, CipherSuiteFilter cipherFilter) throws SSLException
        {
            builds.incrementAndGet();
            if (failBuilds)
                throw new SSLException("simulated build failure");
            if (clearCacheWhileBuilding)
                SSLFactory.clearSslContextCache();
            try
            {
                TimeUnit.MILLISECONDS.sleep(200);
            }
            catch (InterruptedException e)
            {
                Thread.currentThread().interrupt();
                throw new SSLException(e);
            }
            return super.createNettySslContext(clientAuth, socketType, cipherFilter);
        }
    }

    public static class TestFileBasedSSLContextFactory extends FileBasedSslContextFactory
    {
        static volatile int expiryCheckCount = 0;

        public TestFileBasedSSLContextFactory(Map<String, Object> parameters)
        {
            super(parameters);
        }

        @Override
        protected boolean checkExpiredCerts(KeyStore ks) throws KeyStoreException
        {
            expiryCheckCount++;
            return super.checkExpiredCerts(ks);
        }
    }

    /** Does not override checkExpiredCerts(KeyStore); used to assert the override-detection returns false. */
    public static class PlainFileBasedSSLContextFactory extends FileBasedSslContextFactory
    {
        public PlainFileBasedSSLContextFactory(Map<String, Object> parameters)
        {
            super(parameters);
        }
    }

    private static Certificate getCertificates(final String filename, final String password) throws KeyStoreException, IOException, CertificateException, NoSuchAlgorithmException
    {
        FileInputStream is = new FileInputStream(filename);
        KeyStore keystore = KeyStore.getInstance(KeyStore.getDefaultType());
        char[] passwd = password.toCharArray();
        keystore.load(is, passwd);
        return keystore.getCertificate("cassandra_ssl_test");
    }

    private static Certificate getCertificateLoadedInSslContext(final OpenSslSessionContext session)
    throws ClassNotFoundException, InvocationTargetException, IllegalAccessException, NoSuchMethodException, NoSuchFieldException
    {
        Field providerField = OpenSslSessionContext.class.getDeclaredField("provider");
        providerField.setAccessible(true);

        Class<?> keyMaterialProvider = Class.forName("io.netty.handler.ssl.OpenSslKeyMaterialProvider");
        Object provider = keyMaterialProvider.cast(providerField.get(session));

        Method keyManager = provider.getClass().getDeclaredMethod("keyManager");
        keyManager.setAccessible(true);
        X509KeyManager keyManager1 = (X509KeyManager) keyManager.invoke(provider);
        final Certificate[] certificates = keyManager1.getCertificateChain("cassandra_ssl_test");
        return certificates[0];
    }

    void changeKeystorePassword(String filename, String currentPassword, String newPassword)
    {
        try
        {
            KeyStore keystore = KeyStore.getInstance(KeyStore.getDefaultType());
            char[] loadPasswd = currentPassword.toCharArray();
            char[] storePasswd = newPassword.toCharArray();
            try (FileInputStream is = new FileInputStream(filename))
            {
                keystore.load(is, loadPasswd);
            }
            try (FileOutputStream os = new FileOutputStream(filename))
            {
                keystore.store(os, storePasswd);
            }
        }
        catch (Throwable tr)
        {
            throw new RuntimeException(tr);
        }
    }
}
