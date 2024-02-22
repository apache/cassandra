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

package org.apache.cassandra.auth;

import java.security.cert.Certificate;
import java.security.cert.X509Certificate;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.function.Consumer;

import org.apache.cassandra.exceptions.AuthenticationException;

/**
 * Interface for certificate validation and authorization for mTLS authenticators.
 * <p>
 * This interface can be implemented to provide logic for extracting custom identities from client certificates
 * to uniquely identify the certificates. It can also be used to provide custom authorization logic to authenticate
 * clients using client certificates during mTLS connections.
 */
public interface MutualTlsCertificateValidator
{
    /**
     * Perform any checks that are to be performed on the certificate before making authorization check to grant the
     * access to the client during mTLS connection.
     *
     * <p>For example:
     * <ul>
     *  <li>Verifying CA information
     *  <li>Checking CN information
     *  <li>Validating Issuer information
     *  <li>Checking organization information etc
     * </ul>
     *
     * @param clientCertificateChain client certificate chain
     * @return {@code true} if the certificate is valid, {@code false} otherwise
     */
    boolean isValidCertificate(Certificate[] clientCertificateChain);

    /**
     * Extracts the certificate(s) age(s) from the {@code clientCertificateChain} and provides it to the
     * {@code ageConsumer} for further processing.
     *
     * @param clientCertificateChain client certificate chain
     * @param ageConsumer            a consumer of certificate ages (in minutes)
     */
    void certificateAgeConsumer(Certificate[] clientCertificateChain, Consumer<Integer> ageConsumer);

    /**
     * This method should provide logic to extract identity out of a certificate to perform mTLS authentication.
     *
     * <p>An example of identity could be the following:
     * <ul>
     *  <li>an identifier in SAN of the certificate like SPIFFE
     *  <li>CN of the certificate
     *  <li>any other fields in the certificate can be combined and be used as identifier of the certificate
     * </ul>
     *
     * @param clientCertificateChain client certificate chain
     * @return identifier extracted from certificate
     * @throws AuthenticationException when identity cannot be extracted
     */
    String identity(Certificate[] clientCertificateChain) throws AuthenticationException;

    /**
     * Filters out non-{@link X509Certificate}s and casts the certificate chain to {@link X509Certificate}s.
     *
     * @param clientCertificateChain client certificate chain
     * @return an array of certificates that were cast to {@link X509Certificate}
     */
    default X509Certificate[] castCertsToX509(Certificate[] clientCertificateChain)
    {
        return Arrays.stream(clientCertificateChain)
                     .filter(certificate -> certificate instanceof X509Certificate)
                     .toArray(X509Certificate[]::new);
    }

    /**
     * @param certificate the client certificate
     * @return the age of the certificate in minutes
     */
    default int certificateAgeInMinutes(X509Certificate certificate)
    {
        return (int) ChronoUnit.MINUTES.between(certificate.getNotBefore().toInstant(),
                                                certificate.getNotAfter().toInstant());
    }
}
