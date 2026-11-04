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

import java.io.InputStream;
import java.lang.reflect.Method;
import java.nio.file.Files;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.cert.Certificate;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Date;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLException;
import javax.net.ssl.TrustManagerFactory;

import com.google.common.annotations.VisibleForTesting;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.reflect.MethodUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.concurrent.ScheduledExecutors;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.utils.Clock;

import static java.lang.String.format;

/**
 * Abstract implementation for {@link ISslContextFactory} using file based, standard keystore format with the ability
 * to hot-reload the files upon file changes (detected by the {@code last modified timestamp}).
 * <p>
 * {@code CAUTION:} While this is a useful abstraction, please be careful if you need to modify this class
 * given possible custom implementations out there!
 */
public abstract class FileBasedSslContextFactory extends AbstractSslContextFactory
{
    private static final Logger logger = LoggerFactory.getLogger(FileBasedSslContextFactory.class);

    /**
     * Whether this instance's concrete class overrides {@link #checkExpiredCerts(KeyStore)}. Resolved once, at
     * construction: if it does we hand that override a {@link KeyStore}; otherwise we feed the already-scanned expiry
     * map straight to {@link #checkExpiredCerts(Map)} and skip a redundant scan.
     */
    @VisibleForTesting
    final boolean checkExpiredCertsOverridden = isCheckExpiredCertsOverridden(getClass());

    protected FileBasedStoreContext keystoreContext;
    protected FileBasedStoreContext outboundKeystoreContext;
    protected FileBasedStoreContext trustStoreContext;

    /**
     * List of files that trigger hot reloading of SSL certificates
     */
    protected volatile List<HotReloadableFile> hotReloadableFiles = new ArrayList<>();

    public FileBasedSslContextFactory()
    {
        keystoreContext = new FileBasedStoreContext("conf/.keystore", "cassandra", null);
        outboundKeystoreContext = new FileBasedStoreContext("conf/.keystore", "cassandra", null);
        trustStoreContext = new FileBasedStoreContext("conf/.truststore", "cassandra", null);
    }

    public FileBasedSslContextFactory(Map<String, Object> parameters)
    {
        super(parameters);
        keystoreContext = new FileBasedStoreContext(getString("keystore"), getString("keystore_password"), getString("keystore_password_file"));
        outboundKeystoreContext = new FileBasedStoreContext(StringUtils.defaultString(getString("outbound_keystore"), keystoreContext.filePath),
                                                            StringUtils.defaultString(getString("outbound_keystore_password"), keystoreContext.password),
                                                            StringUtils.defaultString(getString("outbound_keystore_password_file"), keystoreContext.passwordFilePath));
        trustStoreContext = new FileBasedStoreContext(getString("truststore"), getString("truststore_password"), getString("truststore_password_file"));
    }

    @Override
    public boolean shouldReload()
    {
        return hotReloadableFiles.stream().anyMatch(HotReloadableFile::shouldReload);
    }

    @Override
    public void onReloadSuccess()
    {
        for (HotReloadableFile file : hotReloadableFiles)
            file.commit();
    }

    @Override
    public void onReloadRequired()
    {
        // Force the next successful load to re-scan certificate expiry for the new keystore contents.
        keystoreContext.checkedExpiry = false;
        outboundKeystoreContext.checkedExpiry = false;
    }

    @Override
    public boolean hasKeystore()
    {
        return keystoreContext.hasKeystore();
    }

    @Override
    public boolean hasOutboundKeystore()
    {
        return outboundKeystoreContext.hasKeystore();
    }

    private boolean hasTruststore()
    {
        return trustStoreContext.filePath != null && new File(trustStoreContext.filePath).exists();
    }

    @Override
    public synchronized void initHotReloading()
    {
        boolean hasKeystore = hasKeystore();
        boolean hasOutboundKeystore = hasOutboundKeystore();
        boolean hasTruststore = hasTruststore();

        if (hasKeystore || hasOutboundKeystore || hasTruststore)
        {
            List<HotReloadableFile> fileList = new ArrayList<>();
            if (hasKeystore)
            {
                fileList.add(new HotReloadableFile(keystoreContext.filePath));
            }
            if (hasOutboundKeystore)
            {
                fileList.add(new HotReloadableFile(outboundKeystoreContext.filePath));
            }
            if (hasTruststore)
            {
                fileList.add(new HotReloadableFile(trustStoreContext.filePath));
            }
            hotReloadableFiles = fileList;
        }
    }

    /**
     * Validates the given keystore password.
     *
     * @param isOutboundKeystore {@code true} for the {@code outbound_keystore_password};{@code false} otherwise
     * @param password           value
     * @throws IllegalArgumentException if the {@code password} is null
     */
    protected void validatePassword(boolean isOutboundKeystore, String password)
    {
        if (password == null)
        {
            String keyName = isOutboundKeystore ? "outbound_" : "";
            final String msg = format("'%skeystore_password' must be specified", keyName);
            throw new IllegalArgumentException(msg);
        }
    }

    /**
     * Builds required KeyManagerFactory from the file based keystore. It also checks for the PrivateKey's certificate's
     * expiry and logs {@code warning} for each expired PrivateKey's certitificate.
     *
     * @return KeyManagerFactory built from the file based keystore.
     * @throws SSLException             if any issues encountered during the build process
     * @throws IllegalArgumentException if the validation for the {@code keystore_password} fails
     * @see #validatePassword(boolean, String)
     */
    @Override
    protected KeyManagerFactory buildKeyManagerFactory() throws SSLException
    {
        /*
         * Validation of the password is delayed until this point to allow nullable keystore passwords
         * for other use-cases (CASSANDRA-18124).
         */
        validatePassword(false, keystoreContext.password);
        return getKeyManagerFactory(keystoreContext);
    }

    @Override
    protected KeyManagerFactory buildOutboundKeyManagerFactory() throws SSLException
    {
        /*
         * Validation of the password is delayed until this point to allow nullable keystore passwords
         * for other use-cases (CASSANDRA-18124).
         */
        validatePassword(true, outboundKeystoreContext.password);
        return getKeyManagerFactory(outboundKeystoreContext);
    }

    /**
     * Builds TrustManagerFactory from the file based truststore.
     *
     * @return TrustManagerFactory from the file based truststore
     * @throws SSLException if any issues encountered during the build process
     */
    @Override
    protected TrustManagerFactory buildTrustManagerFactory() throws SSLException
    {
        try (InputStream tsf = Files.newInputStream(File.getPath(trustStoreContext.filePath)))
        {
            final String algorithm = this.algorithm == null ? TrustManagerFactory.getDefaultAlgorithm() : this.algorithm;
            TrustManagerFactory tmf = TrustManagerFactory.getInstance(algorithm);
            KeyStore ts = KeyStore.getInstance(store_type);

            final char[] truststorePassword = StringUtils.isEmpty(trustStoreContext.password) ? null : trustStoreContext.password.toCharArray();
            ts.load(tsf, truststorePassword);
            tmf.init(ts);
            return tmf;
        }
        catch (Exception e)
        {
            throw new SSLException("failed to build trust manager store for secure connections", e);
        }
    }

    private KeyManagerFactory getKeyManagerFactory(final FileBasedStoreContext context) throws SSLException
    {
        long nowInMillis = Clock.Global.currentTimeMillis();
        try (InputStream ksf = Files.newInputStream(File.getPath(context.filePath)))
        {
            final String algorithm = this.algorithm == null ? KeyManagerFactory.getDefaultAlgorithm() : this.algorithm;
            KeyManagerFactory kmf = KeyManagerFactory.getInstance(algorithm);
            KeyStore ks = KeyStore.getInstance(store_type);
            final char[] password = context.password.toCharArray();
            ks.load(ksf, password);

            Map<String, Date> expiries = certificateExpiries(ks);
            if (!context.checkedExpiry)
            {
                if (checkExpiredCertsOverridden)
                    checkExpiredCerts(ks);       // honor the custom override, its contract takes a KeyStore
                else
                    checkExpiredCerts(expiries, nowInMillis); // default path, reuse the scan we already did
                context.checkedExpiry = true;
            }
            kmf.init(ks, password);
            scheduleExpiryWarning(context, expiries, nowInMillis);
            return kmf;
        }
        catch (Exception e)
        {
            throw new SSLException("failed to build key manager store for secure connections", e);
        }
    }

    protected boolean checkExpiredCerts(Map<String, Date> expiries, long nowInMillis)
    {
        boolean hasExpiredCerts = false;
        final Date now = new Date(nowInMillis);
        for (Map.Entry<String, Date> entry : expiries.entrySet())
        {
            if (entry.getValue().before(now))
            {
                hasExpiredCerts = true;
                logger.warn("Certificate for {} expired on {}", entry.getKey(), entry.getValue());
            }
        }
        return hasExpiredCerts;
    }

    /**
     * Logs a warning for every already-expired X.509 certificate in the keystore. Retained with its original
     * {@code KeyStore} signature (this is inherited, overridable API) so custom {@link ISslContextFactory}
     * subclasses that override or call it are unaffected, the shared scan/log logic lives in
     * {@link #certificateExpiries}. Prefer overriding the pre-computed {@code Map<String, Date> expiries}
     * version
     */
    protected boolean checkExpiredCerts(KeyStore ks) throws KeyStoreException
    {
        return checkExpiredCerts(certificateExpiries(ks), Clock.Global.currentTimeMillis());
    }

    /**
     * @return whether {@code clazz} (or any class between it and {@link FileBasedSslContextFactory}) overrides
     * {@link #checkExpiredCerts(KeyStore)}. {@link MethodUtils#getMatchingMethod} resolves the method through the whole
     * hierarchy including non-public members (unlike {@link Class#getMethod}, which is public-only, or
     * {@link Class#getDeclaredMethod}, which does not walk); the override is present iff it resolves to a class other
     * than {@link FileBasedSslContextFactory}.
     */
    private static boolean isCheckExpiredCertsOverridden(Class<?> clazz)
    {
        Method resolved = MethodUtils.getMatchingMethod(clazz, "checkExpiredCerts", KeyStore.class);
        return resolved != null && resolved.getDeclaringClass() != FileBasedSslContextFactory.class;
    }

    /**
     * @return the earliest {@code notAfter} in {@code expiries}, or {@code null} if empty.
     */
    Date earliestExpiry(Map<String, Date> expiries)
    {
        Date earliest = null;
        for (Date expires : expiries.values())
        {
            if (earliest == null || expires.before(earliest))
                earliest = expires;
        }
        return earliest;
    }

    /**
     * Collects the {@code notAfter} expiry of every X.509 certificate in {@code ks}, keyed by alias. Non-X.509
     * entries and aliases without a certificate (e.g. secret-key entries, for which {@link KeyStore#getCertificate}
     * returns {@code null}) are skipped. This is the single point where the store is scanned for expiries, shared by
     * {@link #checkExpiredCerts} and {@link #earliestExpiry} so both see identical entries.
     * <p>
     * NOTE: this scans <em>every</em> alias, and for a private-key entry {@link KeyStore#getCertificate} returns only
     * the leaf certificate (not the chain). If the keystore holds unnecessary extra entries (e.g. a stale key entry
     * from a prior rotation, or a trusted-CA certificate dumped into the keystore) whose {@code notAfter} is earlier
     * than the certificate actually presented on connections, the earliest-expiry computed here will be skewed early
     * and the expiry warning may be misattributed to a certificate that is not operationally in use. This only affects
     * the warning/logging; it cannot break the handshake, since the {@link javax.net.ssl.KeyManager} selects the alias
     * to present independently at handshake time. Intermediate/root CAs inside a key entry's chain are not scanned.
     */
    protected Map<String, Date> certificateExpiries(KeyStore ks) throws KeyStoreException
    {
        Map<String, Date> expiries = new HashMap<>();
        for (Enumeration<String> aliases = ks.aliases(); aliases.hasMoreElements(); )
        {
            String alias = aliases.nextElement();
            Certificate cert = ks.getCertificate(alias);
            if (cert instanceof X509Certificate)
                expiries.put(alias, ((X509Certificate) cert).getNotAfter());
        }
        return expiries;
    }

    /**
     * Schedules a one-shot warning to fire when the earliest-expiring certificate in {@code expiries} reaches its
     * {@code notAfter}. Cancels and replaces any warning previously scheduled for {@code context}, so a reload
     * refreshes the timer. Certificates already expired at load time are handled by {@link #checkExpiredCerts}
     * (an immediate warning), so no future task is scheduled for them.
     */
    protected void scheduleExpiryWarning(FileBasedStoreContext context, Map<String, Date> expiries, long nowInMillis)
    {
        ScheduledFuture<?> newFuture = null;
        Date earliest = earliestExpiry(expiries);
        if (earliest != null)
        {
            long delayMillis = earliest.getTime() - nowInMillis;
            if (delayMillis > 0)
            {
                final String path = context.filePath;
                final Date expiresAt = earliest;
                newFuture = ScheduledExecutors.scheduledTasks.schedule(
                () -> logger.warn("Certificate in keystore {} has expired (notAfter={})", path, expiresAt),
                delayMillis, TimeUnit.MILLISECONDS);
            }
            // else already expired; checkExpiredCerts already warned
        }

        ScheduledFuture<?> old = FileBasedStoreContext.expiryWarningFutureUpdater.getAndSet(context, newFuture);
        if (old != null)
            old.cancel(false);
    }

    /**
     * Helper class for hot reloading SSL Contexts
     */
    protected static class HotReloadableFile
    {
        private final File file;
        private volatile long lastModTime;
        private volatile long lastShouldReloadTime;

        HotReloadableFile(String path)
        {
            file = new File(path);
            lastModTime = file.lastModified();
        }

        boolean shouldReload()
        {
            lastShouldReloadTime = file.lastModified();
            return lastShouldReloadTime != lastModTime;
        }

        void commit()
        {
            long last = file.lastModified();
            if (last == lastShouldReloadTime) // only commit if not updated since the check
                lastModTime = lastShouldReloadTime;
        }

        @Override
        public String toString()
        {
            return "HotReloadableFile{" +
                   "file=" + file +
                   ", lastModTime=" + lastModTime +
                   '}';
        }
    }

    protected static class FileBasedStoreContext
    {
        public volatile boolean checkedExpiry = false;
        volatile ScheduledFuture<?> expiryWarningFuture;
        public String filePath;
        public String password;
        public String passwordFilePath;

        private static final AtomicReferenceFieldUpdater<FileBasedStoreContext, ScheduledFuture> expiryWarningFutureUpdater
        = AtomicReferenceFieldUpdater.newUpdater(FileBasedStoreContext.class, ScheduledFuture.class, "expiryWarningFuture");

        public FileBasedStoreContext(String keystoreFilePath, String keystorePassword, String keystorePasswordFilePath)
        {
            this.filePath = keystoreFilePath;
            this.passwordFilePath = keystorePasswordFilePath;
            this.password = resolvePassword(keystoreFilePath, keystorePassword, keystorePasswordFilePath);
        }

        protected boolean hasKeystore()
        {
            return filePath != null && new File(filePath).exists();
        }

        protected boolean passwordMatchesIfPresent(String keyPassword)
        {
            return StringUtils.isEmpty(password) || keyPassword.equals(password);
        }

        private static String resolvePassword(String keystoreFilePath, String password, String passwordFilePath)
        {
            if (password != null)
                return password;

            if (StringUtils.isEmpty(passwordFilePath))
                return null;

            File keystorePasswordFile = new File(passwordFilePath);

            if (!keystorePasswordFile.exists())
            {
                final String msg = format("keystore password file %s does not exist", keystorePasswordFile.path());
                throw new ConfigurationException(msg);
            }

            try
            {
                // we expect a password to be on the first line
                List<String> lines = FileUtils.readLines(keystorePasswordFile);
                if (lines.isEmpty())
                    return "";

                return lines.get(0);
            }
            catch (RuntimeException e)
            {
                throw new ConfigurationException(format("'Failed to read keystore password from the %s for %s",
                                                        keystorePasswordFile, keystoreFilePath),
                                                 e);
            }
        }
    }
}
