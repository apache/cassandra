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

package org.apache.cassandra.vault;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.security.KeyFactory;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.PKCS8EncodedKeySpec;
import java.util.Base64;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.security.CertificateIssuer;
import org.apache.cassandra.security.X509Credentials;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.vault.VaultIO.VaultResponse;

/**
 * Vault PKI backed {@link CertificateIssuer}. Instances will be able to retrieve certificates from Vault
 * with attributes as configured in cassandra.yaml. Requires at least Vault 0.9.0 for PKCS#8 private key
 * encoding support.
 */
public class VaultCertificateIssuer implements CertificateIssuer
{
    private static final Logger logger = LoggerFactory.getLogger(VaultCertificateIssuer.class);

    private final String pkiPath;
    private final String role;
    private final String commonName;
    private final String sanNames;
    private final String sanIps;
    private final boolean useKeyStore;
    private final int renewDaysBeforeExpire;

    private final KeyFactory keyFactory;
    private final CertificateFactory certFactory;

    public VaultCertificateIssuer(Map<String, Object> params)
    {
        pkiPath = (String) params.get("pki_path");
        if (pkiPath == null)
            throw new ConfigurationException("Missing pki_path value");

        useKeyStore = (boolean) params.getOrDefault("use_keystore", true);
        renewDaysBeforeExpire = (int) params.getOrDefault("renew_days_before_expire", -1);

        String fqdn = FBUtilities.getJustBroadcastAddress().getCanonicalHostName();
        role = (String) params.getOrDefault("role", fqdn);
        commonName = (String) params.getOrDefault("common_name", fqdn);

        String ip = FBUtilities.getJustBroadcastAddress().getHostAddress();
        sanIps = (String) params.getOrDefault("ip_sans", ip);

        // we can't default to fqdn here, as it may represent an IP as fallback
        sanNames = (String) params.get("alt_names");

        try
        {
            keyFactory = KeyFactory.getInstance("RSA");
        }
        catch (NoSuchAlgorithmException e)
        {
            throw new RuntimeException(e);
        }

        try
        {
            certFactory = CertificateFactory.getInstance("X.509");
        }
        catch (CertificateException e)
        {
            throw new RuntimeException(e);
        }

        // init vault IO instance
        VaultIO.instance.setup(false);
    }

    @Override
    public CompletableFuture<X509Credentials> generateCredentials()
    {
        try
        {
            String uri = pkiPath + "/issue/" + role;
            logger.debug("Requesting new certificate using path: {}", uri);
            return VaultIO.instance.post(uri, createPayload(), DatabaseDescriptor.getVaultAuthenticator()).thenApply(this::parseCredentials);
        }
        catch (IOException e)
        {
            CompletableFuture<X509Credentials> ret = new CompletableFuture<>();
            ret.completeExceptionally(e);
            return ret;
        }
    }

    @VisibleForTesting
    ByteBuf createPayload() throws IOException
    {
        StringBuilder builder = new StringBuilder();
        builder.append("{\"format\":\"der\",\"private_key_format\":\"pkcs8\"");
        builder.append(",\"common_name\":").append('"').append(commonName).append('"');
        if (sanNames != null)
            builder.append(",\"alt_names\":").append('"').append(sanNames).append('"');
        if (sanIps != null)
            builder.append(",\"ip_sans\":").append('"').append(sanIps).append('"');
        builder.append('}');
        return Unpooled.wrappedBuffer(builder.toString().getBytes(StandardCharsets.UTF_8));
    }

    @VisibleForTesting
    X509Credentials parseCredentials(VaultResponse response)
    {
        String skey = (String) response.data.get("private_key");
        if (skey == null) throw new NullPointerException("Missing private_key in Vault response");
        PrivateKey privateKey = decodeKey(skey);

        String scert = (String) response.data.get("certificate");
        if (scert == null) throw new NullPointerException("Missing certificate in Vault response");
        X509Certificate cert = decodeCertificate(scert);

        String sca = (String) response.data.get("issuing_ca");
        if (sca == null) throw new NullPointerException("Missing issuing_ca in Vault response");
        X509Certificate ca = decodeCertificate(sca);

        return new X509Credentials(privateKey, new X509Certificate[]{cert, ca});
    }

    @VisibleForTesting
    PrivateKey decodeKey(String encoded)
    {
        // this will be a PKCS#8 DER encoded key
        byte[] pkcs8 = Base64.getMimeDecoder().decode(encoded);
        try
        {
            PKCS8EncodedKeySpec keySpec = new PKCS8EncodedKeySpec(pkcs8);
            return keyFactory.generatePrivate(keySpec);
        }
        catch (InvalidKeySpecException e)
        {
            throw new RuntimeException(e);
        }
    }

    @VisibleForTesting
    X509Certificate decodeCertificate(String cert)
    {
        try
        {
            byte[] certDecoded = Base64.getMimeDecoder().decode(cert);
            return (X509Certificate) certFactory.generateCertificate(new ByteArrayInputStream(certDecoded));
        }
        catch (CertificateException e)
        {
            throw new RuntimeException(e);
        }
    }

    @Override
    public boolean useKeyStore()
    {
        return useKeyStore;
    }

    @Override
    public int renewDaysBeforeExpire()
    {
        return renewDaysBeforeExpire;
    }

    @Override
    public boolean shareClientAndServer(CertificateIssuer issuer)
    {
        return equals(issuer);
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        VaultCertificateIssuer that = (VaultCertificateIssuer) o;

        if (useKeyStore != that.useKeyStore) return false;
        if (renewDaysBeforeExpire != that.renewDaysBeforeExpire) return false;
        if (!pkiPath.equals(that.pkiPath)) return false;
        if (!role.equals(that.role)) return false;
        if (!commonName.equals(that.commonName)) return false;
        if (sanNames != null ? !sanNames.equals(that.sanNames) : that.sanNames != null) return false;
        return sanIps != null ? sanIps.equals(that.sanIps) : that.sanIps == null;
    }

    @Override
    public int hashCode()
    {
        int result = pkiPath.hashCode();
        result = 31 * result + role.hashCode();
        result = 31 * result + commonName.hashCode();
        result = 31 * result + (sanNames != null ? sanNames.hashCode() : 0);
        result = 31 * result + (sanIps != null ? sanIps.hashCode() : 0);
        result = 31 * result + (useKeyStore ? 1 : 0);
        result = 31 * result + renewDaysBeforeExpire;
        return result;
    }
}