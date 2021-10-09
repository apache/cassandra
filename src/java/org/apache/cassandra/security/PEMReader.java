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

import java.io.ByteArrayInputStream;
import java.security.AlgorithmParameters;
import java.security.Key;
import java.security.KeyFactory;
import java.security.PrivateKey;
import java.security.cert.Certificate;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.security.spec.PKCS8EncodedKeySpec;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.crypto.Cipher;
import javax.crypto.EncryptedPrivateKeyInfo;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.PBEKeySpec;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class PEMReader
{
    private static final Logger logger = LoggerFactory.getLogger(PEMReader.class);

    private static final Pattern CERT_PATTERN = Pattern.compile("-+BEGIN\\s+.*CERTIFICATE[^-]*-+(?:\\s|\\r|\\n)+([a-z0-9+/=\\r\\n]+)-+END\\s+.*CERTIFICATE[^-]*-+", 2);
    private static final Pattern KEY_PATTERN = Pattern.compile("-+BEGIN\\s+.*PRIVATE\\s+KEY[^-]*-+(?:\\s|\\r|\\n)+([a-z0-9+/=\\r\\n]+)-+END\\s+.*PRIVATE\\s+KEY[^-]*-+", 2);

    public static PrivateKey extractPrivateKey(String unencryptedPEMKey) throws Exception
    {
        return extractPrivateKey(unencryptedPEMKey, null);
    }

    public static PrivateKey extractPrivateKey(String pemKey, String keyPassword) throws Exception
    {
        PKCS8EncodedKeySpec keySpec;
        String base64EncodedKey = extractBase64EncodedKey(pemKey);
        byte[] derKeyBytes = Base64.getDecoder().decode(base64EncodedKey);

        if (keyPassword != null)
        {
            logger.debug("Encrypted key's length: {}",derKeyBytes.length);
            logger.debug("Key's password length: {}", keyPassword.length());

            EncryptedPrivateKeyInfo epki = new EncryptedPrivateKeyInfo(derKeyBytes);
            logger.debug("Encrypted private key info's algorithm name: {}", epki.getAlgName());

            AlgorithmParameters params = epki.getAlgParameters();
            PBEKeySpec pbeKeySpec = new PBEKeySpec(keyPassword.toCharArray());
            Key encryptionKey = SecretKeyFactory.getInstance(epki.getAlgName()).generateSecret(pbeKeySpec);
            pbeKeySpec.clearPassword();
            logger.debug("Key algorithm: {}", encryptionKey.getAlgorithm());
            logger.debug("Key format: {}", encryptionKey.getFormat());

            Cipher cipher = Cipher.getInstance(epki.getAlgName());
            cipher.init(Cipher.DECRYPT_MODE, encryptionKey, params);
            byte[] rawKeyBytes = cipher.doFinal(epki.getEncryptedData());
            logger.debug("Decrypted private key's length: {}", rawKeyBytes.length);

            keySpec = new PKCS8EncodedKeySpec(rawKeyBytes);
        }
        else
        {
            logger.debug("Key length: {}",derKeyBytes.length);
            keySpec = new PKCS8EncodedKeySpec(derKeyBytes);
        }

       return KeyFactory.getInstance("RSA").generatePrivate(keySpec);
    }

    public static Certificate[] extractCertificates(String pemCerts) throws Exception {
        List<Certificate> certificateList = new ArrayList<>();
        List<String> base64EncodedCerts = extractBase64EncodedCerts(pemCerts);
        for (String base64EncodedCertificate : base64EncodedCerts)
        {
            certificateList.add(generateCertificate(base64EncodedCertificate));
        }
        return certificateList.toArray(new Certificate[0]);
    }

    private static Certificate generateCertificate(String base64Certificate) throws Exception {
        byte[] decodedCertificateBytes = Base64.getDecoder().decode(base64Certificate);
        CertificateFactory certificateFactory = CertificateFactory.getInstance("X.509");
        X509Certificate certificate =
        (X509Certificate) certificateFactory.generateCertificate(new ByteArrayInputStream(decodedCertificateBytes));
        printCertificateDetails(certificate);
        return certificate;
    }

    private static void printCertificateDetails(X509Certificate certificate)
    {
        logger.debug("*********** Certificate Details *****************");
        logger.debug("Subject DN: {}", certificate.getSubjectDN());
        logger.debug("Issuer DN: {}", certificate.getIssuerDN());
        logger.debug("Serial Number: {}", certificate.getSerialNumber());
        logger.debug("Expiry: {}", certificate.getNotAfter());
    }

    private static String extractBase64EncodedKey(String pemKey) {
        Matcher matcher = KEY_PATTERN.matcher(pemKey);
        if (matcher.find())
        {
            return matcher.group(1).replaceAll("\\s","");
        }
        else
        {
            throw new RuntimeException("Invalid private key format");
        }
    }

    private static List<String> extractBase64EncodedCerts(String pemCerts) {
        List<String> certificateList = new ArrayList<>();
        Matcher matcher = CERT_PATTERN.matcher(pemCerts);

        for(int start = 0; matcher.find(start); start = matcher.end())
        {
            String certificate = matcher.group(1).replaceAll("\\s","");
            certificateList.add(certificate);
        }
        return certificateList;
    }
}
