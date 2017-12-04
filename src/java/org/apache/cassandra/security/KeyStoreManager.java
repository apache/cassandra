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
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.nio.file.attribute.FileAttribute;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.UnrecoverableEntryException;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.Collections;
import java.util.Date;
import java.util.Enumeration;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.concurrent.ScheduledExecutors;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.EncryptionOptions;
import org.apache.cassandra.config.EncryptionOptions.ServerEncryptionOptions;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.utils.FBUtilities;

/**
 * Certificates and corresponding private keys can be stored by {@link KeyStore} instances handled by the
 * KeyStoreManager. It is responsible for handling the complete life-cycle of such a keystore, including
 * instantiation and local persistence. Certificates will be checked if expiration and renewed
 * by a provided {@link CertificateIssuer}, if supported.
 */
public class KeyStoreManager
{
    private static final Logger logger = LoggerFactory.getLogger(KeyStoreManager.class);

    protected final static int RENEWAL_CHECK_DELAY_HOURS = 24;
    protected final static int CHECK_KS_REFRESH_INTERVAL_SECS = 120;

    public static KeyStoreManager internodeInstance;
    public static KeyStoreManager clientInstance;

    protected static String defaultAlias = "cassandra";

    protected Set<KeyStoreManagerListener> listeners = Collections.emptySet();

    protected CertificateIssuer issuer;

    protected EncryptionOptions encryptionOptions;

    protected volatile KeyStore keystore;
    protected volatile long keystoreModificationTime = 0;

    private ScheduledFuture<?> scheduledRenewalTask;
    private ScheduledFuture<?> periodicRenewalCheck;
    private ScheduledFuture<?> periodicRefreshCheck;

    private final AtomicReference<CompletableFuture<KeyStore>> pendingKeystoreCreationOp = new AtomicReference<>();

    static
    {
        try
        {
            defaultAlias = InetAddress.getLocalHost().getHostName();
        }
        catch (UnknownHostException e)
        {
            throw new RuntimeException(e);
        }
    }

    protected static final AtomicBoolean instantiated = new AtomicBoolean(false);
    protected final AtomicBoolean active = new AtomicBoolean(false);

    @VisibleForTesting
    KeyStoreManager(EncryptionOptions encryptionOptions, CertificateIssuer issuer) {
        this.encryptionOptions = encryptionOptions;
        this.issuer = issuer;
    }

    /**
     * Instantiate and initializes keystore singletons.
     */
    public static void instantiate(@Nullable ServerEncryptionOptions internodeEncryptionOptions,
                                   @Nullable EncryptionOptions clientEncryptionOptions)
    {
        if (!instantiated.compareAndSet(false, true))
            throw new RuntimeException("KeyStoreManager instances cannot be instantiated twice");

        CertificateIssuer internodeIssuer = DatabaseDescriptor.getServerCertificateIssuer();
        CertificateIssuer clientIssuer = DatabaseDescriptor.getClientCertificateIssuer();
        boolean distinctIssuers = internodeIssuer != null
                                  && clientIssuer != null
                                  && !internodeIssuer.shareClientAndServer(clientIssuer);

        boolean isInternodeEnabled = internodeEncryptionOptions != null && internodeEncryptionOptions.enabled;
        boolean isClientEnabled = clientEncryptionOptions != null && clientEncryptionOptions.enabled;
        boolean isKeystoreShared = isInternodeEnabled && isClientEnabled
                                    && internodeEncryptionOptions.keystore != null && clientEncryptionOptions.keystore != null
                                    && internodeEncryptionOptions.keystore.equals(clientEncryptionOptions.keystore);

        // We do not support storing multiple credentials in a shared keystore yet. This will help to reduce complexity now,
        // but may be added in a later version.
        if (isKeystoreShared && distinctIssuers)
            throw new ConfigurationException("Certificate issuer settings must be identical for client and server when using a shared keystore");

        // We also currently issue the same credentials twice, in case of different key stores and identical issuer
        // settings. But that can be avoided by simply using a shared keystore, as configured by default.

        if (isInternodeEnabled)
        {
            internodeInstance = new KeyStoreManager(internodeEncryptionOptions, internodeIssuer);
            internodeInstance.addListener(SSLFactory::clearInternodeSslContext);
            internodeInstance.initKeystore();
        }

        if (isClientEnabled)
        {
            if (isKeystoreShared)
            {
                clientInstance = internodeInstance;
            }
            else
            {
                clientInstance = new KeyStoreManager(clientEncryptionOptions, clientIssuer);
                clientInstance.addListener(SSLFactory::clearNativeTransportSslContext);
                clientInstance.initKeystore();
            }
        }
    }

    /*
     * Testing only.
     */
    protected static void shutdown()
    {
        if (!instantiated.compareAndSet(true, false))
            return;

        if (internodeInstance != null)
        {
            internodeInstance.destroy();
            internodeInstance = null;
        }
        if (clientInstance != null)
        {
            clientInstance.destroy();
            clientInstance = null;
        }
    }

    /**
     * Initializes keystore by loading it from the local filesystem, as configured by the specified {@link EncryptionOptions},
     * or generates new keystore credentials if needed and an {@link CertificateIssuer} has been provided.
     * This is a blocking operation that will return after the keystore has been successfully initialized or throws an
     * exception in case of any errors.
     */
    public synchronized void initKeystore() {
        if(!active.compareAndSet(false, true))
            throw new RuntimeException("Cannot initialize KeyStoreManager instance twice");
        if (!encryptionOptions.enabled)
            throw new RuntimeException("Encryption not enabled");

        KeyStore existingLocalKeyStore = null;
        long existingLocalKeyStoreModifiedAt = 0;
        File file = new File(encryptionOptions.keystore);
        if (file.isFile() && file.canRead())
        {
            existingLocalKeyStore = loadKeystore(encryptionOptions.keystore, encryptionOptions.keystore_password.toCharArray(), encryptionOptions.store_type);
            existingLocalKeyStoreModifiedAt = file.lastModified();
        }

        if (issuer != null)
        {
            // In case of a provided issuer, schedule a periodic task that will check if any certificates
            // need to be renewed. This needs to be as robust as possible, so forget about clever scheduling
            // algorithms based on expiration date. Simply run the check. Every. Single. Time.
            this.periodicRenewalCheck = ScheduledExecutors.scheduledTasks.scheduleAtFixedRate(this::renew,
                                                                                              RENEWAL_CHECK_DELAY_HOURS,
                                                                                              RENEWAL_CHECK_DELAY_HOURS,
                                                                                              TimeUnit.HOURS);
        }
        else if (existingLocalKeyStore != null)
        {
            // Monitor local keystore for changes in case it is not managed by an issuer implementation.
            // This allows operators or side car processes to renew the keystore without having to restart
            // the Cassandra instance.
            this.periodicRefreshCheck = ScheduledExecutors.scheduledTasks.scheduleAtFixedRate(this::refreshLocalKeystore,
                                                                                              CHECK_KS_REFRESH_INTERVAL_SECS,
                                                                                              CHECK_KS_REFRESH_INTERVAL_SECS,
                                                                                              TimeUnit.SECONDS);
        }

        final CompletableFuture<KeyStore> creationResult;
        if (existingLocalKeyStore != null)
        {
            logger.debug("Using X.509 credentials from keystore: {}", file.getAbsolutePath());
            X509Credentials cred = extractCredentials(existingLocalKeyStore, encryptionOptions.keystore_password.toCharArray());
            logCredentials(cred);

            if (issuer == null)
            {
                // no issuer provided - simply stick to the local KS
                switchKeystore(existingLocalKeyStore, existingLocalKeyStoreModifiedAt);
                notifyListenersOnUpdate();
                return;
            }
            else
            {
                // check if existing certificate needs to be renewed by provided issuer
                if (needsRenewal(cred, issuer.renewDaysBeforeExpire()))
                {
                    creationResult = generate();
                }
                else
                {
                    switchKeystore(existingLocalKeyStore, existingLocalKeyStoreModifiedAt);
                    notifyListenersOnUpdate();
                    return;
                }
            }
        }
        else
        {
            // bail in case keystore doesn't exist and no issuer for automatic creation has been configured
            if (issuer == null)
            {
                throw new RuntimeException(String.format("Keystore not found or unreadable: %s", file.getAbsolutePath()));
            }
            else
            {
                logger.info("Local keystore does not exist. Requesting new credentials.");
                creationResult = generate();
            }
        }

        try
        {
            // Keystore initialization is taking place as part of the Cassandra startup process.
            // We have to block here, so any exception during keystore creation is able to abort the node's startup process
            creationResult.get(1, TimeUnit.MINUTES);
        }
        catch (InterruptedException e)
        {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Interrupted while initializing keystore");
        }
        catch (Exception e)
        {
            throw new RuntimeException("Failed to initialize keystore", e);
        }
    }

    /**
     * Deallocate instance. Mainly required for testing.
     */
    protected void destroy()
    {
        if(!active.compareAndSet(true, false))
            return;

        if (periodicRenewalCheck != null && !periodicRenewalCheck.isCancelled())
            periodicRenewalCheck.cancel(true);
        periodicRenewalCheck = null;

        if (periodicRefreshCheck != null && !periodicRefreshCheck.isCancelled())
            periodicRefreshCheck.cancel(true);
        periodicRefreshCheck = null;

        if (scheduledRenewalTask != null && !scheduledRenewalTask.isDone())
            scheduledRenewalTask.cancel(true);
        scheduledRenewalTask = null;

        CompletableFuture<KeyStore> creationOp = pendingKeystoreCreationOp.get();
        if (creationOp != null && !creationOp.isDone())
            creationOp.cancel(true);
        pendingKeystoreCreationOp.set(null);
    }

    /**
     * Generates new keystore credentials using provided {@link CertificateIssuer} and optionally saves created keystore locally.
     * May return already pending operations.
     */
    protected CompletableFuture<KeyStore> generate()
    {
        assert issuer != null : "Issuer not set";
        return pendingKeystoreCreationOp.updateAndGet((f) -> f != null && !f.isDone() ? f : this.generateIntern());
    }

    private CompletableFuture<KeyStore> generateIntern()
    {
        return issuer.generateCredentials()
            .thenApply(this::logCredentials)
            .thenApply((cred) -> instantiateKeystore(cred, encryptionOptions.store_type, encryptionOptions.keystore_password.toCharArray()))
            .whenComplete((ks, e) -> {
            // save instantiated keystore locally, switch instances and notify listeners
            if (e != null)
            {
                logger.error("Failed to create new keystore. Check logs and encryption settings.");
                return;
            }
            if (!issuer.useKeyStore())
            {
                logger.debug("Acquired keystore will be handled in-memory only and not flushed to disk");
            }
            else
            {
                try
                {
                    saveKeystore(ks, encryptionOptions.keystore, encryptionOptions.keystore_password.toCharArray());
                }
                catch (Exception ex)
                {
                    logger.warn("Could not write local keystore. Acquired X.509 credentials will be lost after next restart.", ex);
                }
            }
            switchKeystore(ks, System.currentTimeMillis());
            notifyListenersOnUpdate();
        });
    }

    protected void switchKeystore(KeyStore ks, long updatedAt)
    {
        logger.debug("Switching keystore");
        keystore = ks;
        keystoreModificationTime = updatedAt;
    }

    protected void notifyListenersOnUpdate()
    {
        logger.debug("Notifying {} listeners on certificate update", listeners.size());
        listeners.forEach(KeyStoreManagerListener::onCertificateUpdate);
    }

    /**
     * Registers {@link KeyStoreManagerListener} instance that will be notified on keystore life-cycle events
     */
    public void addListener(KeyStoreManagerListener listener)
    {
        listeners = ImmutableSet.<KeyStoreManagerListener>builder().addAll(listeners).add(listener).build();
    }

    /**
     * Returns current keystore instance.
     */
    public KeyStore getKeystore()
    {
        return keystore;
    }

    /**
     * Returns current credentials.
     */
    public X509Credentials getCredentials()
    {
        // We could as well cache the credentials after retrieving them from the issuer, but
        // I'd rather not like to keep the private key on heap and leave this to the key store instead.
        return extractCredentials(keystore, encryptionOptions.keystore_password.toCharArray());
    }

    /**
     * Checks for certificate expiration and initializes creation of new certificates if needed.
     */
    protected synchronized void renew()
    {
        try
        {
            if (issuer != null && needsRenewal(getCredentials(), issuer.renewDaysBeforeExpire()))
            {
                generate().whenComplete(this::retryRenewalIfNeeded);
            }
        }
        catch (Throwable t)
        {
            // catch anything so we keep any periodic task scheduled
            logger.error(t.getMessage(), t);
        }
    }

    private synchronized void retryRenewalIfNeeded(KeyStore ignored, Throwable t)
    {
        if (t != null)
        {
            if (scheduledRenewalTask != null && !scheduledRenewalTask.isDone())
            {
                logger.warn("Scheduled renewal task still running, skipping re-scheduling");
            }
            else
            {
                // Any issue during certificate creation should be due to problems with the PKI backend
                // (and not in our code, of course ;)). So let's give the backend some time to recover and avoid
                // thunder-herding it. Remember that we should have time measure in days left to acquire new certificates.
                long minsLeft = (long) (Math.random() * 10 + 1);
                logger.warn("Certificate renewal failed, retrying in {} minutes", minsLeft);
                this.scheduledRenewalTask = ScheduledExecutors.scheduledTasks.schedule(this::renew, minsLeft, TimeUnit.MINUTES);
            }
        }
    }

    /**
     * Checks if local keystore file has been modified since last loaded and reloads keystore.
     */
    protected void refreshLocalKeystore()
    {
        try
        {
            File file = new File(encryptionOptions.keystore);
            if (!file.isFile() || !file.canRead())
            {
                logger.error("Keystore missing or inaccessible: " + file.getAbsolutePath());
                return;
            }

            long lastModified = file.lastModified();
            if (lastModified > this.keystoreModificationTime)
            {
                logger.info("Reloading modified keystore: " + file.getAbsolutePath());
                KeyStore ks = loadKeystore(encryptionOptions.keystore, encryptionOptions.keystore_password.toCharArray(), encryptionOptions.store_type);
                switchKeystore(ks, lastModified);
                notifyListenersOnUpdate();
            }
        }
        catch (Throwable t)
        {
            // catch anything so we keep any periodic task scheduled
            logger.error(t.getMessage(), t);
        }
    }

    protected static boolean expired(X509Credentials credentials)
    {
        return new Date().after(credentials.cert().getNotAfter());
    }

    protected static boolean needsRenewal(X509Credentials credentials, int renewalDaysBeforeExpire)
    {
        return needsRenewal(credentials, renewalDaysBeforeExpire, new Date());
    }

    protected static boolean needsRenewal(X509Credentials credentials, int renewalDaysBeforeExpire, Date now)
    {
        return daysUntilExpiration(credentials.cert(), now) <= renewalDaysBeforeExpire;
    }

    private X509Credentials logCredentials(X509Credentials credentials)
    {
        try
        {
            logger.debug("Got X509 key using {} algo in {} format", credentials.privateKey.getAlgorithm(), credentials.privateKey.getFormat());
            Date now = new Date();
            for (int i = 0; i < credentials.chain.length; i++)
            {
                X509Certificate cert = credentials.chain[i];
                if (i == 0)
                {
                    logger.info("Acquired certificate {}", cert.getSerialNumber());
                    logger.trace("Certificate details: {}", cert);
                }
                else if (i == credentials.chain.length - 1) logger.trace("Root certificate: {}", cert);
                else logger.trace("Intermediate certificate: {}", cert);
                if (now.before(cert.getNotBefore()) || now.after(cert.getNotAfter()))
                {
                    logger.error("Certificate {} expired, valid at {} - {}", cert.getSerialNumber(), cert.getNotBefore(), cert.getNotAfter());
                }
                else
                {
                    int daysUntilExpired = daysUntilExpiration(cert, now);
                    Consumer<String> log = logger::debug;
                    if (daysUntilExpired <= 7) log = logger::warn;
                    else if (daysUntilExpired <= 14) log = logger::info;
                    log.accept(String.format("Certificate %s about to expire in %s days", cert.getSerialNumber(), daysUntilExpired));
                }
            }
        }
        catch (Throwable t)
        {
            // this method is informative only and should not break ssl support
            logger.warn(t.getMessage(), t);
        }
        return credentials;
    }

    private static int daysUntilExpiration(X509Certificate cert, Date now)
    {
        Date exp = cert.getNotAfter();
        long millis = exp.getTime() - now.getTime();
        return (int) TimeUnit.MILLISECONDS.toDays(millis);
    }

    protected static KeyStore instantiateKeystore(X509Credentials cred, String type, char[] pass)
    {
        try
        {
            KeyStore store = KeyStore.getInstance(type);
            store.load(null, null);

            KeyStore.Entry entry = new KeyStore.PrivateKeyEntry(cred.privateKey, cred.chain);
            store.setEntry(defaultAlias, entry, new KeyStore.PasswordProtection(pass));
            return store;
        }
        catch (IOException | NoSuchAlgorithmException | CertificateException | KeyStoreException e)
        {
            logger.error("Failed to instanciate keystore for X.509 credentials", e);
            throw new RuntimeException(e);
        }
    }

    protected static X509Credentials extractCredentials(KeyStore ks, char[] pass)
    {
        try
        {
            KeyStore.PrivateKeyEntry entry = (KeyStore.PrivateKeyEntry) ks.getEntry(defaultAlias,
                                                                                    new KeyStore.PasswordProtection(pass));
            if (entry == null)
            {
                logger.debug("Default entry {} not found, using first entry found in keystore instead", defaultAlias);
                for (Enumeration<String> aliases = ks.aliases(); aliases.hasMoreElements(); )
                {
                    String alias = aliases.nextElement();
                    if (ks.entryInstanceOf(alias, KeyStore.PrivateKeyEntry.class))
                    {
                        entry = (KeyStore.PrivateKeyEntry) ks.getEntry(alias, new KeyStore.PasswordProtection(pass));
                        break;
                    }
                }
            }
            if (entry == null) throw new RuntimeException("Failed to get any private key entry from keystore");
            return new X509Credentials(entry.getPrivateKey(), (X509Certificate[]) entry.getCertificateChain());
        }
        catch (UnrecoverableEntryException | NoSuchAlgorithmException | KeyStoreException e)
        {
            throw new RuntimeException("Failed to get entry from keystore", e);
        }
    }

    @SuppressWarnings("ResultOfMethodCallIgnored")
    protected static synchronized void saveKeystore(KeyStore store, String path, char[] pass)
    {
        try
        {
            File target = new File(path);
            if (target.isDirectory())
                throw new IOException("Keystore path must not be a directory: " + path);

            // The keystore file contains sensitive data that must not be readable except for the Cassandra process.
            // We therefor will try to create the new keystore file as only accessible by the owner.
            Set<PosixFilePermission> perm = PosixFilePermissions.fromString("rw-------");
            FileAttribute<Set<PosixFilePermission>> attr = PosixFilePermissions.asFileAttribute(perm);
            File tmpFile;
            Path tmpPath = Paths.get(path + ".tmp");
            try
            {
                tmpFile = Files.createFile(tmpPath, attr).toFile();
            }
            catch (UnsupportedOperationException e)
            {
                if (!FBUtilities.isWindows) logger.warn(e.getMessage(), e);
                // fall back to io.File based solution, while still trying to lock down read access to owner only
                tmpFile = tmpPath.toFile();
                tmpFile.setWritable(false);
                tmpFile.setWritable(true, true);
                tmpFile.setExecutable(false);
                tmpFile.setReadable(false);
                tmpFile.setReadable(true, true);
            }
            catch (FileAlreadyExistsException e)
            {
                logger.warn("Found old tmp file for keystore creation, trying to remove it");
                Files.delete(tmpPath);
                if (Files.exists(tmpPath))
                    throw new IOException("Failed to delete tmp file");
                saveKeystore(store, path, pass);
                return;
            }
            try (FileOutputStream fout = new FileOutputStream(tmpFile))
            {
                store.store(fout, pass);
            }
            // set permissions to read-only after store has been writen to fs
            try
            {
                Files.setPosixFilePermissions(tmpPath, PosixFilePermissions.fromString("r--------"));
            }
            catch (UnsupportedOperationException e)
            {
                if (!FBUtilities.isWindows) logger.warn(e.getMessage(), e);
                tmpFile = tmpPath.toFile();
                tmpFile.setWritable(false, false);
            }
            catch (IOException e)
            {
                logger.error(e.getMessage(), e);
            }

            Files.move(tmpFile.toPath(), target.toPath(), StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
            logger.debug("Saved keystore: {}", target.getPath());
        }
        catch (IOException | NoSuchAlgorithmException | CertificateException | KeyStoreException e)
        {
            throw new RuntimeException("Error saving keystore for X.509 credentials", e);
        }
    }

    protected static KeyStore loadKeystore(String path, char[] pass, String type)
    {
        try
        {
            KeyStore store = KeyStore.getInstance(type);
            File file = new File(path);
            try (FileInputStream fin = new FileInputStream(file))
            {
                store.load(fin, pass);
            }
            return store;
        }
        catch (IOException | NoSuchAlgorithmException | CertificateException | KeyStoreException e)
        {
            logger.error("Error loading local keystore for X.509 credentials", e);
            throw new RuntimeException(e);
        }
    }
}