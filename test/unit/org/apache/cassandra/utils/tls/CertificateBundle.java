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

package org.apache.cassandra.utils.tls;

import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.security.KeyPair;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.cert.X509Certificate;
import java.util.Objects;

public class CertificateBundle
{
    private final String signatureAlgorithm;
    private final X509Certificate[] chain;
    private final X509Certificate root;
    private final KeyPair keyPair;
    private final String alias;

    public CertificateBundle(String signatureAlgorithm, X509Certificate[] chain,
                             X509Certificate root, KeyPair keyPair, String alias)
    {
        this.signatureAlgorithm = Objects.requireNonNull(signatureAlgorithm);
        this.chain = chain;
        this.root = root;
        this.keyPair = keyPair;
        this.alias = Objects.requireNonNullElse(alias, "1");
    }

    public KeyStore toKeyStore(char[] keyEntryPassword) throws KeyStoreException
    {
        KeyStore keyStore;
        try
        {
            keyStore = KeyStore.getInstance("PKCS12");
            keyStore.load(null, null);
        }
        catch (Exception e)
        {
            throw new RuntimeException("Failed to initialize PKCS#12 KeyStore.", e);
        }
        keyStore.setCertificateEntry("1", root);
        if (!isCertificateAuthority())
        {
            keyStore.setKeyEntry(alias, keyPair.getPrivate(), keyEntryPassword, chain);
        }
        return keyStore;
    }

    public Path toTempKeyStorePath(Path baseDir, char[] pkcs12Password, char[] keyEntryPassword) throws Exception
    {
        KeyStore keyStore = toKeyStore(keyEntryPassword);
        Path tempFile = Files.createTempFile(baseDir, "ks", ".p12");
        try (OutputStream out = Files.newOutputStream(tempFile, StandardOpenOption.WRITE))
        {
            keyStore.store(out, pkcs12Password);
        }
        return tempFile;
    }

    public boolean isCertificateAuthority()
    {
        return chain[0].getBasicConstraints() != -1;
    }

    public X509Certificate certificate()
    {
        return chain[0];
    }

    public KeyPair keyPair()
    {
        return keyPair;
    }

    public X509Certificate[] certificatePath()
    {
        return chain.clone();
    }

    public X509Certificate rootCertificate()
    {
        return root;
    }

    public String signatureAlgorithm()
    {
        return signatureAlgorithm;
    }
}
