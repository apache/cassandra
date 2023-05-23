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

import org.apache.cassandra.exceptions.AuthenticationException;

/**
 * Interface for certificate validation and authorization for mTLS authenticators.
 *
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
     * For example
     *  - Verifying CA information
     *  - Checking CN information
     *  - Validating Issuer information
     *  - Checking organization information etc
     *
     * @param clientCertificateChain client certificate chain
     * @return returns if the certificate is valid or not
     */
    boolean isValidCertificate(Certificate[] clientCertificateChain);

    /**
     * This method should provide logic to extract identity out of a certificate to perform mTLS authentication.
     *
     * An example of identity could be the following
     *  - an identifier in SAN of the certificate like SPIFFE
     *  - CN of the certificate
     *  - any other fields in the certificate can be combined and be used as identifier of the certificate
     *
     * @param clientCertificateChain client certificate chain
     * @return identifier extracted from certificate
     * @throws AuthenticationException when identity cannot be extracted
     */
    String identity(Certificate[] clientCertificateChain) throws AuthenticationException;

}
