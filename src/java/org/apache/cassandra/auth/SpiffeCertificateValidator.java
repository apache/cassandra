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
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import org.apache.cassandra.exceptions.AuthenticationException;

/**
 * This class assumes that the identity of a certificate is SPIFFE which is a URI that is present as part of the SAN
 * of the client certificate. It has logic to extract identity (Spiffe) out of a certificate & knows how to validate
 * the client certificates.
 * <p>
 *
 * <p>
 * Example:
 * internode_authenticator:
 * class_name : org.apache.cassandra.auth.MutualTlsAuthenticator
 * parameters :
 * validator_class_name: org.apache.cassandra.auth.SpiffeCertificateValidator
 * authenticator:
 * class_name : org.apache.cassandra.auth.MutualTlsInternodeAuthenticator
 * parameters :
 * validator_class_name: org.apache.cassandra.auth.SpiffeCertificateValidator
 */
public class SpiffeCertificateValidator implements MutualTlsCertificateValidator
{
    @Override
    public boolean isValidCertificate(Certificate[] clientCertificateChain)
    {
        return true;
    }

    @Override
    public String identity(Certificate[] clientCertificateChain) throws AuthenticationException
    {
        // returns spiffe
        try
        {
            return getSANSpiffe(clientCertificateChain);
        }
        catch (CertificateException e)
        {
            throw new AuthenticationException(e.getMessage(), e);
        }
    }

    private static String getSANSpiffe(final Certificate[] clientCertificates) throws CertificateException
    {
        int URI_TYPE = 6;
        X509Certificate[] castedCerts = castCertsToX509(clientCertificates);
        Collection<List<?>> subjectAltNames = castedCerts[0].getSubjectAlternativeNames();

        if (subjectAltNames != null)
        {
            for (List<?> item : subjectAltNames)
            {
                Integer type = (Integer) item.get(0);
                String spiffe = (String) item.get(1);
                if (type == URI_TYPE && spiffe.startsWith("spiffe://"))
                {  // Spiffe is a URI
                    return spiffe;
                }
            }
        }
        throw new CertificateException("Unable to extract Spiffe from the certificate");
    }

    private static X509Certificate[] castCertsToX509(Certificate[] clientCertificateChain)
    {
        return Arrays.asList(clientCertificateChain).toArray(new X509Certificate[0]);
    }
}
