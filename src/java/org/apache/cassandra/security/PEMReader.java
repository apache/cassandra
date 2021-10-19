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
import java.io.IOException;
import java.security.AlgorithmParameters;
import java.security.GeneralSecurityException;
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

/**
 * This is a helper class to read private keys and X509 certifificates encoded based on <a href="https://datatracker.ietf.org/doc/html/rfc1421">PEM (RFC 1421)</a>
 * format. It can read Password Based Encrypted (PBE henceforth) private keys as well as non-encrypred private keys
 * along with the X509 certificates/cert-chain based on the textual encoding defined in the <a href="https://datatracker.ietf.org/doc/html/rfc7468">RFC 7468</a>
 *
 * The input private key must be in PKCS#8 format.
 *
 * It returns PKCS#8 formatted private key and X509 certificates.
 */
public final class PEMReader
{
    private static final Logger logger = LoggerFactory.getLogger(PEMReader.class);

    private static final Pattern CERT_PATTERN = Pattern.compile("-+BEGIN\\s+.*CERTIFICATE[^-]*-+(?:\\s|\\r|\\n)+([a-z0-9+/=\\r\\n]+)-+END\\s+.*CERTIFICATE[^-]*-+", 2);
    private static final Pattern KEY_PATTERN = Pattern.compile("-+BEGIN\\s+.*PRIVATE\\s+KEY[^-]*-+(?:\\s|\\r|\\n)+([a-z0-9+/=\\r\\n]+)-+END\\s+.*PRIVATE\\s+KEY[^-]*-+", 2);

    /**
     * The private key can be with any of these algorithms in order for this read to successfully parse it.
     * Currently supported algorithms are,
     * <pre>
     *     RSA, DSA or EC
     * </pre>
     * The first one to be evaluated is RSA, being the most common for private keys.
     */
    public static final String[] SUPPORTED_PRIVATE_KEY_ALGORITHMS = new String[]{"RSA", "DSA", "EC"};

    /**
     * Extracts private key from the PEM content for the private key, assuming its not PBE.
     * @param unencryptedPEMKey private key stored as PEM content
     * @return {@link PrivateKey} upon successful reading of the private key
     * @throws IOException in case PEM reading fails
     * @throws GeneralSecurityException in case any issue encountered while reading the private key
     */
    public static PrivateKey extractPrivateKey(String unencryptedPEMKey) throws IOException, GeneralSecurityException
    {
        return extractPrivateKey(unencryptedPEMKey, null);
    }

    /**
     * Extracts private key from the Password Based Encrypted PEM content for the private key.
     * @param pemKey PBE private key stored as PEM content
     * @param keyPassword password to be used for the private key decryption
     * @return {@link PrivateKey} upon successful reading of the private key
     * @throws IOException in case PEM reading fails
     * @throws GeneralSecurityException in case any issue encountered while reading the private key
     */
    public static PrivateKey extractPrivateKey(String pemKey, String keyPassword) throws IOException,
                                                                                         GeneralSecurityException
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

        PrivateKey privateKey = null;

        /*
         * Ideally we can inspect the OID (Object Identifier) from the private key with ASN.1 parser and identify the
         * actual algorithm of the private key. For doing that, we have to use some special library like BouncyCastle.
         * However in the absence of that, below brute-force approach can work- that is to try out all the supported
         * private key algorithms given that there are only three major algorithms to verify against.
         */
        for(int i=0;i<SUPPORTED_PRIVATE_KEY_ALGORITHMS.length;i++)
        {
            try
            {
                privateKey = KeyFactory.getInstance(SUPPORTED_PRIVATE_KEY_ALGORITHMS[i]).generatePrivate(keySpec);
                logger.info("Parsing for the private key finished with {} algorithm.",
                            SUPPORTED_PRIVATE_KEY_ALGORITHMS[i]);
                return privateKey;
            }
            catch(Exception e)
            {
                logger.debug("Failed to parse the private key with {} algorithm. Will try the other supported " +
                             "algorithms.", SUPPORTED_PRIVATE_KEY_ALGORITHMS[i]);
            }
        }

       if(privateKey==null)
       {
           throw new GeneralSecurityException("The given private key could not be parsed with any of the supported " +
                                              "algorithms. Please see PEMReader#SUPPORTED_PRIVATE_KEY_ALGORITHMS.");
       }
       // Must never come here
       return null;
    }

    /**
     * Extracts the certificates/cert-chain from the PEM content.
     * @param pemCerts certificates/cert-chain stored as PEM content
     * @return X509 certiificate list
     * @throws GeneralSecurityException in case any issue encountered while reading the certificates
     */
    public static Certificate[] extractCertificates(String pemCerts) throws GeneralSecurityException {
        List<Certificate> certificateList = new ArrayList<>();
        List<String> base64EncodedCerts = extractBase64EncodedCerts(pemCerts);
        for (String base64EncodedCertificate : base64EncodedCerts)
        {
            certificateList.add(generateCertificate(base64EncodedCertificate));
        }
        Certificate[] certificates = certificateList.toArray(new Certificate[0]);
        return certificates;
    }

    /**
     * Generates the X509 certificate object given the base64 encoded PEM content.
     * @param base64Certificate base64 encoded PEM content for the certificate
     * @return X509 certificate
     * @throws GeneralSecurityException in case any issue encountered while reading the certificate
     */
    private static Certificate generateCertificate(String base64Certificate) throws GeneralSecurityException {
        byte[] decodedCertificateBytes = Base64.getDecoder().decode(base64Certificate);
        CertificateFactory certificateFactory = CertificateFactory.getInstance("X.509");
        X509Certificate certificate =
        (X509Certificate) certificateFactory.generateCertificate(new ByteArrayInputStream(decodedCertificateBytes));
        printCertificateDetails(certificate);
        return certificate;
    }

    /**
     * Prints X509 certificate details for the debugging purpose with {@code DEBUG} level log.
     * Namely, it prints- Subject DN, Issuer DN, Certificate serial number and the certificate expiry date which
     * could be very valuable for debugging any certificate related issues.
     *
     * @param certificate
     */
    private static void printCertificateDetails(X509Certificate certificate)
    {
        logger.debug("*********** Certificate Details *****************");
        logger.debug("Subject DN: {}", certificate.getSubjectDN());
        logger.debug("Issuer DN: {}", certificate.getIssuerDN());
        logger.debug("Serial Number: {}", certificate.getSerialNumber());
        logger.debug("Expiry: {}", certificate.getNotAfter());
    }

    /**
     * Parses the PEM formatted private key based on the standard pattern specified by the <a href="https://datatracker.ietf.org/doc/html/rfc7468#section-11">RFC 7468</a>.
     * @param pemKey private key stored as PEM content
     * @return base64 string contained within the defined encapsulation boundaries by the above RFC
     * @throws GeneralSecurityException in case any issue encountered while parsing the key
     */
    private static String extractBase64EncodedKey(String pemKey) throws GeneralSecurityException
    {
        Matcher matcher = KEY_PATTERN.matcher(pemKey);
        if (matcher.find())
        {
            return matcher.group(1).replaceAll("\\s","");
        }
        else
        {
            throw new GeneralSecurityException("Invalid private key format");
        }
    }

    /**
     * Parses the PEM formatted certificate/public-key based on the standard pattern specified by the
     * <a href="https://datatracker.ietf.org/doc/html/rfc7468#section-13">RFC 7468</a>.
     * @param pemCerts certificate/public-key stored as PEM content
     * @return list of base64 encoded certificates within the defined encapsulation boundaries by the above RFC
     * @throws GeneralSecurityException in case any issue encountered parsing the certificate
     */
    private static List<String> extractBase64EncodedCerts(String pemCerts) throws GeneralSecurityException
    {
        List<String> certificateList = new ArrayList<>();
        Matcher matcher = CERT_PATTERN.matcher(pemCerts);
        if (!matcher.find())
        {
            throw new GeneralSecurityException("Invalid certificate format");
        }

        for(int start = 0; matcher.find(start); start = matcher.end())
        {
            String certificate = matcher.group(1).replaceAll("\\s","");
            certificateList.add(certificate);
        }
        return certificateList;
    }
}
