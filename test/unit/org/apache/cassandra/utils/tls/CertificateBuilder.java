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

import java.io.IOException;
import java.math.BigInteger;
import java.security.GeneralSecurityException;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.SecureRandom;
import java.security.cert.X509Certificate;
import java.security.spec.AlgorithmParameterSpec;
import java.security.spec.ECGenParameterSpec;
import java.security.spec.RSAKeyGenParameterSpec;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Objects;
import javax.security.auth.x500.X500Principal;

import org.bouncycastle.asn1.x500.X500Name;
import org.bouncycastle.asn1.x509.BasicConstraints;
import org.bouncycastle.asn1.x509.Extension;
import org.bouncycastle.asn1.x509.GeneralName;
import org.bouncycastle.asn1.x509.GeneralNames;
import org.bouncycastle.cert.X509CertificateHolder;
import org.bouncycastle.cert.jcajce.JcaX509CertificateConverter;
import org.bouncycastle.cert.jcajce.JcaX509v3CertificateBuilder;
import org.bouncycastle.operator.ContentSigner;
import org.bouncycastle.operator.jcajce.JcaContentSignerBuilder;

/**
 * A utility class to generate certificates for tests
 */
public class CertificateBuilder
{
    private static final GeneralName[] EMPTY_SAN = {};
    private static final SecureRandom SECURE_RANDOM = new SecureRandom();

    private boolean isCertificateAuthority;
    private String alias;
    private X500Name subject;
    private SecureRandom random;
    private Date notBefore = Date.from(Instant.now().minus(1, ChronoUnit.DAYS));
    private Date notAfter = Date.from(Instant.now().plus(1, ChronoUnit.DAYS));
    private String algorithm;
    private AlgorithmParameterSpec algorithmParameterSpec;
    private String signatureAlgorithm;
    private BigInteger serial;
    private final List<GeneralName> subjectAlternativeNames = new ArrayList<>();

    public CertificateBuilder()
    {
        ecp256Algorithm();
    }

    public CertificateBuilder isCertificateAuthority(boolean isCertificateAuthority)
    {
        this.isCertificateAuthority = isCertificateAuthority;
        return this;
    }

    public CertificateBuilder subject(String subject)
    {
        this.subject = new X500Name(Objects.requireNonNull(subject));
        return this;
    }

    public CertificateBuilder notBefore(Instant notBefore)
    {
        return notBefore(Date.from(Objects.requireNonNull(notBefore)));
    }

    private CertificateBuilder notBefore(Date notBefore)
    {
        this.notBefore = Objects.requireNonNull(notBefore);
        return this;
    }

    public CertificateBuilder notAfter(Instant notAfter)
    {
        return notAfter(Date.from(Objects.requireNonNull(notAfter)));
    }

    private CertificateBuilder notAfter(Date notAfter)
    {
        this.notAfter = Objects.requireNonNull(notAfter);
        return this;
    }

    public CertificateBuilder addSanUriName(String uri)
    {
        subjectAlternativeNames.add(new GeneralName(GeneralName.uniformResourceIdentifier, uri));
        return this;
    }

    public CertificateBuilder addSanDnsName(String dnsName)
    {
        subjectAlternativeNames.add(new GeneralName(GeneralName.dNSName, dnsName));
        return this;
    }

    public CertificateBuilder clearSubjectAlternativeNames()
    {
        subjectAlternativeNames.clear();
        return this;
    }

    public CertificateBuilder secureRandom(SecureRandom secureRandom)
    {
        this.random = Objects.requireNonNull(secureRandom);
        return this;
    }

    public CertificateBuilder alias(String alias)
    {
        this.alias = Objects.requireNonNull(alias);
        return this;
    }

    public CertificateBuilder serial(BigInteger serial)
    {
        this.serial = serial;
        return this;
    }

    public CertificateBuilder ecp256Algorithm()
    {
        this.algorithm = "EC";
        this.algorithmParameterSpec = new ECGenParameterSpec("secp256r1");
        this.signatureAlgorithm = "SHA256WITHECDSA";
        return this;
    }

    public CertificateBuilder rsa2048Algorithm()
    {
        this.algorithm = "RSA";
        this.algorithmParameterSpec = new RSAKeyGenParameterSpec(2048, RSAKeyGenParameterSpec.F4);
        this.signatureAlgorithm = "SHA256WITHRSA";
        return this;
    }

    public CertificateBundle buildSelfSigned() throws Exception
    {
        KeyPair keyPair = generateKeyPair();

        JcaX509v3CertificateBuilder builder = createCertBuilder(subject, subject, keyPair);
        addExtensions(builder);

        ContentSigner signer = new JcaContentSignerBuilder(signatureAlgorithm).build(keyPair.getPrivate());
        X509CertificateHolder holder = builder.build(signer);
        X509Certificate root = new JcaX509CertificateConverter().getCertificate(holder);
        return new CertificateBundle(signatureAlgorithm, new X509Certificate[]{ root }, root, keyPair, alias);
    }

    public CertificateBundle buildIssuedBy(CertificateBundle issuer) throws Exception
    {
        String issuerSignAlgorithm = issuer.signatureAlgorithm();
        return buildIssuedBy(issuer, issuerSignAlgorithm);
    }

    public CertificateBundle buildIssuedBy(CertificateBundle issuer, String issuerSignAlgorithm) throws Exception
    {
        KeyPair keyPair = generateKeyPair();

        X500Principal issuerPrincipal = issuer.certificate().getSubjectX500Principal();
        X500Name issuerName = X500Name.getInstance(issuerPrincipal.getEncoded());
        JcaX509v3CertificateBuilder builder = createCertBuilder(issuerName, subject, keyPair);

        addExtensions(builder);

        PrivateKey issuerPrivateKey = issuer.keyPair().getPrivate();
        if (issuerPrivateKey == null)
        {
            throw new IllegalArgumentException("Cannot sign certificate with issuer that does not have a private key.");
        }
        ContentSigner signer = new JcaContentSignerBuilder(issuerSignAlgorithm).build(issuerPrivateKey);
        X509CertificateHolder holder = builder.build(signer);
        X509Certificate cert = new JcaX509CertificateConverter().getCertificate(holder);
        X509Certificate[] issuerPath = issuer.certificatePath();
        X509Certificate[] path = new X509Certificate[issuerPath.length + 1];
        path[0] = cert;
        System.arraycopy(issuerPath, 0, path, 1, issuerPath.length);
        return new CertificateBundle(signatureAlgorithm, path, issuer.rootCertificate(), keyPair, alias);
    }

    private SecureRandom secureRandom()
    {
        return Objects.requireNonNullElse(random, SECURE_RANDOM);
    }

    private KeyPair generateKeyPair() throws GeneralSecurityException
    {
        KeyPairGenerator keyGen = KeyPairGenerator.getInstance(algorithm);
        keyGen.initialize(algorithmParameterSpec, secureRandom());
        return keyGen.generateKeyPair();
    }

    private JcaX509v3CertificateBuilder createCertBuilder(X500Name issuer, X500Name subject, KeyPair keyPair)
    {
        BigInteger serial = this.serial != null ? this.serial : new BigInteger(159, secureRandom());
        PublicKey pubKey = keyPair.getPublic();
        return new JcaX509v3CertificateBuilder(issuer, serial, notBefore, notAfter, subject, pubKey);
    }

    private void addExtensions(JcaX509v3CertificateBuilder builder) throws IOException
    {
        if (isCertificateAuthority)
        {
            builder.addExtension(Extension.basicConstraints, true, new BasicConstraints(true));
        }

        boolean criticality = false;
        if (!subjectAlternativeNames.isEmpty())
        {
            builder.addExtension(Extension.subjectAlternativeName, criticality,
                                 new GeneralNames(subjectAlternativeNames.toArray(EMPTY_SAN)));
        }
    }
}
