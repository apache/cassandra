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

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.AccessDeniedException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.attribute.FileAttribute;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.UnrecoverableKeyException;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import com.google.common.base.Throwables;
import com.google.common.hash.HashCode;
import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.config.EncryptionOptions;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.utils.FBUtilities;

import static junit.framework.TestCase.assertNotNull;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class KeyStoreManagerTest
{
    private static final String PKI_KEYSTORE_PATH = "test/resources/pki/keystore.p12";
    private static final String PKI_KEYSTORE_EXPIRED_PATH = "test/resources/pki/keystore-expired.p12";
    private final char[] KS_PASS = "pass123".toCharArray();

    @BeforeClass
    public static void setup() throws ConfigurationException
    {
        SchemaLoader.prepareServer();
    }

    @After
    public void tearDown()
    {
        KeyStoreManager.shutdown();
    }

    @Test
    public void testStart()
    {
        KeyStoreManager.instantiate(null, null);
    }

    @Test (expected = Exception.class)
    public void testStartOnlyOnce()
    {
        KeyStoreManager.instantiate(null, null);
        KeyStoreManager.instantiate(null, null);
    }

    @Test (expected = UnrecoverableKeyException.class)
    public void testWrongPassword() throws Throwable
    {
        throwRoot(() -> KeyStoreManager.loadKeystore(PKI_KEYSTORE_PATH, new char[]{ 'X' }, "PKCS12"),
                  "password", null);
    }

    @Test (expected = UnrecoverableKeyException.class)
    public void testWrongKeystoreFormat() throws Throwable
    {
        throwRoot(() -> KeyStoreManager.loadKeystore(PKI_KEYSTORE_PATH, new char[]{ 'X' }, "JKS"),
                  "not properly padded", null);
    }

    @Test (expected = IOException.class)
    public void testSaveAsDirPath() throws Throwable
    {
        KeyStore ks = KeyStore.getInstance("PKCS12");
        Path tmpDir = Files.createTempDirectory("testSaveAsDirPath_");
        throwRoot(() -> KeyStoreManager.saveKeystore(ks, tmpDir.toString(), KS_PASS), null, tmpDir.toFile()::delete);
    }

    @Test
    public void testReplaceExistingTmp() throws Throwable
    {
        KeyStore ks = KeyStore.getInstance("PKCS12");
        ks.load(null, null);
        Path tmpDir = Files.createTempDirectory("testSaveAsDirPath_");
        Path tmpKs = tmpDir.resolve("keystore.p12");
        Path tmpTmp = tmpDir.resolve("keystore.p12.tmp");
        Files.createFile(tmpKs);
        Files.createFile(tmpTmp);
        KeyStoreManager.saveKeystore(ks, tmpKs.toString(), KS_PASS);

        assertFalse(tmpTmp.toFile().exists());
        assertTrue(tmpKs.toFile().exists());
        assertTrue(tmpKs.toFile().length() > 0);
        assertTrue(tmpKs.toFile().delete());
        assertTrue(tmpDir.toFile().delete());
    }

    @Test (expected = AccessDeniedException.class)
    public void testDirNotWritable() throws Throwable
    {
        if (FBUtilities.isWindows) return;
        KeyStore ks = KeyStore.getInstance("PKCS12");
        Set<PosixFilePermission> perm = PosixFilePermissions.fromString("r-x------");
        FileAttribute<Set<PosixFilePermission>> attr = PosixFilePermissions.asFileAttribute(perm);
        Path tmpDir = Files.createTempDirectory("testSaveAsDirPath_", attr);
        throwRoot(() -> KeyStoreManager.saveKeystore(ks, tmpDir.resolve("keystore.p12").toString(), KS_PASS), null,
            tmpDir.toFile()::delete);
    }

    @Test (expected = NoSuchFileException.class)
    public void testNonExistingDirectory() throws Throwable
    {
        KeyStore ks = KeyStore.getInstance("PKCS12");
        throwRoot(() -> KeyStoreManager.saveKeystore(ks, "non_existing_conf/keystore.p12", KS_PASS), null, null);
    }

    @Test
    public void testLoadWritebackDelete() throws KeyStoreException, IOException
    {
        KeyStore ks = KeyStoreManager.loadKeystore(PKI_KEYSTORE_PATH, KS_PASS, "PKCS12");
        assertNotNull(ks);
        assertEquals(1, ks.size());
        X509Credentials cred = KeyStoreManager.extractCredentials(ks, KS_PASS);
        assertNotNull(cred);
        assertEquals("RSA", cred.privateKey.getAlgorithm());
        assertEquals(2, cred.chain.length);

        KeyStore ks2 = KeyStoreManager.instantiateKeystore(cred, "PKCS12", KS_PASS);
        assertNotNull(ks2);
        assertEquals(1, ks2.size());

        Path tmpDir = Files.createTempDirectory("testLoadWritebackDelete_");
        Path path = tmpDir.resolve("test_keystore.p12");
        KeyStoreManager.saveKeystore(ks2, path.toString(), KS_PASS);
        KeyStoreManager.saveKeystore(ks2, path.toString(), KS_PASS);
        KeyStoreManager.saveKeystore(ks2, path.toString(), KS_PASS);

        KeyStore ks3 = KeyStoreManager.loadKeystore(path.toString(), KS_PASS, "PKCS12");
        assertNotNull(ks3);
        Files.delete(path);
        Files.delete(tmpDir);
    }

    @Test
    public void testExpirationCheckValid()
    {
        KeyStore ks = KeyStoreManager.loadKeystore(PKI_KEYSTORE_PATH, KS_PASS, "PKCS12");
        X509Credentials cred = KeyStoreManager.extractCredentials(ks, KS_PASS);
        assertFalse(KeyStoreManager.expired(cred));
        assertFalse(KeyStoreManager.needsRenewal(cred, 0));
    }

    @Test
    public void testExpirationCheckExpired()
    {
        KeyStore ks = KeyStoreManager.loadKeystore(PKI_KEYSTORE_EXPIRED_PATH, KS_PASS, "PKCS12");
        X509Credentials cred = KeyStoreManager.extractCredentials(ks, KS_PASS);
        assertTrue(KeyStoreManager.expired(cred));
        assertTrue(KeyStoreManager.needsRenewal(cred, 0));
    }

    @Test
    public void testGenerate() throws IOException
    {
        Path tmpDir = Files.createTempDirectory("testGenerate_");
        Path ksPath = tmpDir.resolve("test_keystore.jks");
        MockIssuer issuer = new MockIssuer(this::generateValidCredentials, true,
                                           7, false);
        KeyStoreManager ksm = new KeyStoreManager(serverOptions(ksPath.toString()), issuer);
        AtomicInteger notified = new AtomicInteger(0);
        ksm.addListener(notified::incrementAndGet);
        ksm.initKeystore();
        assertEquals(1, issuer.called);
        assertTrue(ksPath.toFile().exists());
        assertTrue(ksPath.toFile().length() > 0);
        assertEquals(1, notified.get());
        Files.delete(ksPath);
        Files.delete(tmpDir);
    }

    @Test
    public void testDoNotGenerate() throws Exception
    {
        Path tmpDir = Files.createTempDirectory("testDoNotGenerate_");
        Path ksPath = tmpDir.resolve("test_keystore.jks");

        // copy fixture ks to tmp path
        KeyStore ks = KeyStoreManager.loadKeystore(PKI_KEYSTORE_PATH, KS_PASS, "PKCS12");
        try (FileOutputStream fos = new FileOutputStream(ksPath.toFile()))
        {
            ks.store(fos, KS_PASS);
        }

        // init KSM while expecting that existing ks is used and no new cert would be generated
        MockIssuer issuer = new MockIssuer(this::generateValidCredentials, true,
                                           7, false);
        EncryptionOptions.ServerEncryptionOptions conf = serverOptions(ksPath.toString());
        conf.store_type = "PKCS12";
        KeyStoreManager ksm = new KeyStoreManager(conf, issuer);
        AtomicInteger notified = new AtomicInteger(0);
        ksm.addListener(notified::incrementAndGet);
        ksm.initKeystore();
        assertEquals(0, issuer.called);
        assertTrue(ksPath.toFile().exists());
        assertTrue(ksPath.toFile().length() > 0);
        assertEquals(1, notified.get());
        Files.delete(ksPath);
        Files.delete(tmpDir);
    }

    @Test
    public void testGenerateExpired() throws Exception
    {
        Path tmpDir = Files.createTempDirectory("testGenerateExpired_");
        Path ksPath = tmpDir.resolve("test_keystore.jks");

        // copy fixture ks to tmp path
        KeyStore ks = KeyStoreManager.loadKeystore(PKI_KEYSTORE_EXPIRED_PATH, KS_PASS, "PKCS12");
        try (FileOutputStream fos = new FileOutputStream(ksPath.toFile()))
        {
            ks.store(fos, KS_PASS);
        }
        Hasher hf = Hashing.md5().newHasher();
        hf.putBytes(Files.readAllBytes(ksPath));
        HashCode hashExpired = hf.hash();

        // init KSM while expecting that existing ks is used, the expired certificate detected
        // and a new certificate generated
        MockIssuer issuer = new MockIssuer(this::generateValidCredentials, true,
                                           7, false);
        EncryptionOptions.ServerEncryptionOptions conf = serverOptions(ksPath.toString());
        conf.store_type = "PKCS12";
        KeyStoreManager ksm = new KeyStoreManager(conf, issuer);
        AtomicInteger notified = new AtomicInteger(0);
        ksm.addListener(notified::incrementAndGet);
        ksm.initKeystore();
        assertEquals(1, issuer.called);
        assertTrue(ksPath.toFile().exists());
        assertTrue(ksPath.toFile().length() > 0);
        assertEquals(1, notified.get());

        hf = Hashing.md5().newHasher();
        hf.putBytes(Files.readAllBytes(ksPath));
        HashCode hashReplaced = hf.hash();
        assertNotEquals(hashReplaced, hashExpired);

        Files.delete(ksPath);
        Files.delete(tmpDir);
    }

    @Test
    public void testGenerateNoStoreLocally() throws IOException
    {
        Path tmpDir = Files.createTempDirectory("testGenerateExpired_");
        Path ksPath = tmpDir.resolve("test_keystore.jks");

        // setting useKeyStore to false should prevent saving the credentials to a local keystore
        MockIssuer issuer = new MockIssuer(this::generateValidCredentials, false,
                                           7, false);
        EncryptionOptions.ServerEncryptionOptions conf = serverOptions(ksPath.toString());
        KeyStoreManager ksm = new KeyStoreManager(conf, issuer);
        AtomicInteger notified = new AtomicInteger(0);
        ksm.addListener(notified::incrementAndGet);
        ksm.initKeystore();
        assertEquals(1, issuer.called);
        assertFalse(ksPath.toFile().exists());
        assertEquals(1, notified.get());

        Files.delete(tmpDir);
    }

    @Test (expected = SecurityException.class)
    public void testGenerateWithException() throws Throwable
    {
        Path tmpDir = Files.createTempDirectory("testGenerateWithException_");
        Path ksPath = tmpDir.resolve("test_keystore.jks");
        MockIssuer issuer = new MockIssuer(this::generateWithException, true,
                                           7, false);
        KeyStoreManager ksm = new KeyStoreManager(serverOptions(ksPath.toString()), issuer);
        AtomicBoolean notified = new AtomicBoolean(false);
        ksm.addListener(() -> notified.set(true));
        try
        {
            ksm.initKeystore();
        }
        catch (Exception e)
        {
            assertEquals(1, issuer.called);
            assertFalse(ksPath.toFile().exists());
            assertFalse(notified.get());
            Files.delete(tmpDir);
            throw Throwables.getRootCause(e);
        }
    }

    @Test (expected = RuntimeException.class)
    public void testFailInstatiateWithEncryptionDisabled() throws Throwable
    {
        Path tmpDir = Files.createTempDirectory("testFailInstatiateWithEncryptionDisabled_");
        Path ksPath = tmpDir.resolve("test_keystore.jks");
        EncryptionOptions.ServerEncryptionOptions conf = serverOptions(ksPath.toString());
        conf.enabled = false;
        KeyStoreManager ksm = new KeyStoreManager(conf, null);
        throwRoot(ksm::initKeystore, null, tmpDir.toFile()::delete);
    }

    @Test (expected = RuntimeException.class)
    public void testFailInstatiateWithoutLocalStoreAndIssuer() throws Throwable
    {
        Path tmpDir = Files.createTempDirectory("testFailInstatiateWithoutLocalStoreAndIssuer_");
        Path ksPath = tmpDir.resolve("test_keystore.jks");
        KeyStoreManager ksm = new KeyStoreManager(serverOptions(ksPath.toString()), null);
        throwRoot(ksm::initKeystore, null, tmpDir.toFile()::delete);
    }

    @Test
    public void testInstantiate() throws IOException
    {
        EncryptionOptions.ServerEncryptionOptions conf = serverOptions(PKI_KEYSTORE_PATH);
        KeyStoreManager.instantiate(conf, null);
        assertNotNull(KeyStoreManager.internodeInstance);
        assertNull(KeyStoreManager.clientInstance);
        KeyStoreManager.shutdown();

        KeyStoreManager.instantiate(null, conf);
        assertNull(KeyStoreManager.internodeInstance);
        assertNotNull(KeyStoreManager.clientInstance);
        KeyStoreManager.shutdown();

        KeyStoreManager.instantiate(conf, conf);
        assertNotNull(KeyStoreManager.internodeInstance);
        assertNotNull(KeyStoreManager.clientInstance);
        assertTrue(KeyStoreManager.internodeInstance == KeyStoreManager.clientInstance);
        KeyStoreManager.shutdown();

        EncryptionOptions.ServerEncryptionOptions conf2 = serverOptions(PKI_KEYSTORE_EXPIRED_PATH);
        KeyStoreManager.instantiate(conf, conf2);
        assertNotNull(KeyStoreManager.internodeInstance);
        assertNotNull(KeyStoreManager.clientInstance);
        assertTrue(KeyStoreManager.internodeInstance != KeyStoreManager.clientInstance);
        KeyStoreManager.shutdown();
    }

    private EncryptionOptions.ServerEncryptionOptions serverOptions(String ksPath)
    {
        EncryptionOptions.ServerEncryptionOptions conf = new EncryptionOptions.ServerEncryptionOptions();
        conf.enabled = true;
        conf.internode_encryption = EncryptionOptions.ServerEncryptionOptions.InternodeEncryption.all;
        conf.keystore = ksPath;
        conf.keystore_password = String.valueOf(KS_PASS);
        return conf;
    }

    private EncryptionOptions clientOptions(String ksPath)
    {
        EncryptionOptions conf = new EncryptionOptions();
        conf.enabled = true;
        conf.keystore = ksPath;
        conf.keystore_password = String.valueOf(KS_PASS);
        return conf;
    }

    private void throwRoot(Runnable f, String msg, Callable<Boolean> cleanup) throws Throwable
    {
        try
        {
            f.run();
        }
        catch (Exception e)
        {
            Throwable root = Throwables.getRootCause(e);
            if (msg != null) assertTrue(e.getMessage().contains(msg) || root.getMessage().contains(msg));
            throw root;
        }
        finally
        {
            if (cleanup != null) assertTrue(cleanup.call());
        }
    }

    private X509Credentials generateValidCredentials()
    {
        KeyStore ks = KeyStoreManager.loadKeystore(PKI_KEYSTORE_PATH, KS_PASS, "PKCS12");
        return KeyStoreManager.extractCredentials(ks, KS_PASS);
    }

    private X509Credentials generateWithException()
    {
        throw new SecurityException();
    }


    private static class MockIssuer implements CertificateIssuer
    {
        private final Supplier<X509Credentials> generator;
        private final boolean useKeyStore;
        private final int renewDaysBeforeExpire;
        private final boolean shareClientAndServer;
        int called = 0;

        private MockIssuer(Supplier<X509Credentials> generator, boolean useKeyStore,
                           int renewDaysBeforeExpire, boolean shareClientAndServer)
        {
            this.generator = generator;
            this.useKeyStore = useKeyStore;
            this.renewDaysBeforeExpire = renewDaysBeforeExpire;
            this.shareClientAndServer = shareClientAndServer;
        }

        public CompletableFuture<X509Credentials> generateCredentials()
        {
            called++;
            CompletableFuture<X509Credentials> ret = new CompletableFuture<>();
            try
            {
                ret.complete(generator.get());
            }
            catch (Exception e)
            {
                ret.completeExceptionally(e);
            }
            return ret;
        }

        public boolean useKeyStore()
        {
            return useKeyStore;
        }

        public int renewDaysBeforeExpire()
        {
            return renewDaysBeforeExpire;
        }

        public boolean shareClientAndServer(CertificateIssuer issuer)
        {
            return shareClientAndServer;
        }
    }
}
