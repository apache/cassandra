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
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.KeyStore;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Date;
import java.util.Enumeration;
import java.util.List;
import java.util.Map;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLException;
import javax.net.ssl.TrustManagerFactory;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.io.util.File;

/**
 * Abstract implementation for {@link ISslContextFactory} using file based, standard keystore format with the ability
 * to hot-reload the files upon file changes (detected by the {@code last modified timestamp}).
 * <p>
 * {@code CAUTION:} While this is a useful abstraction, please be careful if you need to modify this class
 * given possible custom implementations out there!
 */
abstract public class FileBasedSslContextFactory extends AbstractSslContextFactory
{
    private static final Logger logger = LoggerFactory.getLogger(FileBasedSslContextFactory.class);

    @VisibleForTesting
    protected volatile boolean checkedExpiry = false;

    /**
     * List of files that trigger hot reloading of SSL certificates
     */
    protected volatile List<HotReloadableFile> hotReloadableFiles = new ArrayList<>();

    protected String keystore;
    protected String keystore_password;
    protected String truststore;
    protected String truststore_password;

    public FileBasedSslContextFactory()
    {
        keystore = "conf/.keystore";
        keystore_password = "cassandra";
        truststore = "conf/.truststore";
        truststore_password = "cassandra";
    }

    public FileBasedSslContextFactory(Map<String, Object> parameters)
    {
        super(parameters);
        keystore = getString("keystore");
        keystore_password = getString("keystore_password");
        truststore = getString("truststore");
        truststore_password = getString("truststore_password");
    }

    @Override
    public boolean shouldReload()
    {
        return hotReloadableFiles.stream().anyMatch(HotReloadableFile::shouldReload);
    }

    @Override
    public boolean hasKeystore()
    {
        return keystore != null && new File(keystore).exists();
    }

    private boolean hasTruststore()
    {
        return truststore != null && new File(truststore).exists();
    }

    @Override
    public synchronized void initHotReloading()
    {
        boolean hasKeystore = hasKeystore();
        boolean hasTruststore = hasTruststore();

        if (hasKeystore || hasTruststore)
        {
            List<HotReloadableFile> fileList = new ArrayList<>();
            if (hasKeystore)
            {
                fileList.add(new HotReloadableFile(keystore));
            }
            if (hasTruststore)
            {
                fileList.add(new HotReloadableFile(truststore));
            }
            hotReloadableFiles = fileList;
        }
    }

    /**
     * Builds required KeyManagerFactory from the file based keystore. It also checks for the PrivateKey's certificate's
     * expiry and logs {@code warning} for each expired PrivateKey's certitificate.
     *
     * @return KeyManagerFactory built from the file based keystore.
     * @throws SSLException if any issues encountered during the build process
     */
    protected KeyManagerFactory buildKeyManagerFactory() throws SSLException
    {
        try (InputStream ksf = Files.newInputStream(Paths.get(keystore)))
        {
            KeyManagerFactory kmf = KeyManagerFactory.getInstance(
            algorithm == null ? KeyManagerFactory.getDefaultAlgorithm() : algorithm);
            KeyStore ks = KeyStore.getInstance(store_type);
            ks.load(ksf, keystore_password.toCharArray());
            if (!checkedExpiry)
            {
                for (Enumeration<String> aliases = ks.aliases(); aliases.hasMoreElements(); )
                {
                    String alias = aliases.nextElement();
                    if (ks.getCertificate(alias).getType().equals("X.509"))
                    {
                        Date expires = ((X509Certificate) ks.getCertificate(alias)).getNotAfter();
                        if (expires.before(new Date()))
                            logger.warn("Certificate for {} expired on {}", alias, expires);
                    }
                }
                checkedExpiry = true;
            }
            kmf.init(ks, keystore_password.toCharArray());
            return kmf;
        }
        catch (Exception e)
        {
            throw new SSLException("failed to build key manager store for secure connections", e);
        }
    }

    /**
     * Builds TrustManagerFactory from the file based truststore.
     *
     * @return TrustManagerFactory from the file based truststore
     * @throws SSLException if any issues encountered during the build process
     */
    protected TrustManagerFactory buildTrustManagerFactory() throws SSLException
    {
        try (InputStream tsf = Files.newInputStream(Paths.get(truststore)))
        {
            TrustManagerFactory tmf = TrustManagerFactory.getInstance(
            algorithm == null ? TrustManagerFactory.getDefaultAlgorithm() : algorithm);
            KeyStore ts = KeyStore.getInstance(store_type);
            ts.load(tsf, truststore_password.toCharArray());
            tmf.init(ts);
            return tmf;
        }
        catch (Exception e)
        {
            throw new SSLException("failed to build trust manager store for secure connections", e);
        }
    }

    /**
     * Helper class for hot reloading SSL Contexts
     */
    private static class HotReloadableFile
    {
        private final File file;
        private volatile long lastModTime;

        HotReloadableFile(String path)
        {
            file = new File(path);
            lastModTime = file.lastModified();
        }

        boolean shouldReload()
        {
            long curModTime = file.lastModified();
            boolean result = curModTime != lastModTime;
            lastModTime = curModTime;
            return result;
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
}
