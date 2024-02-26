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
import java.util.Arrays;

public final class MutualTlsUtil
{
    /**
     * Filters instances of {@link X509Certificate} certificates and returns the certificate chain as
     * {@link X509Certificate} certificates.
     *
     * @param clientCertificateChain client certificate chain
     * @return an array of certificates that were cast to {@link X509Certificate}
     */
    public static X509Certificate[] castCertsToX509(Certificate[] clientCertificateChain)
    {
        if (clientCertificateChain == null || clientCertificateChain.length == 0)
        {
            return null;
        }
        return Arrays.stream(clientCertificateChain)
                     .filter(certificate -> certificate instanceof X509Certificate)
                     .toArray(X509Certificate[]::new);
    }
}
