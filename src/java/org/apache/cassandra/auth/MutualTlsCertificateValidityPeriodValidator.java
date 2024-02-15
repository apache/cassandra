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
import java.util.Date;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.cassandra.config.DurationSpec;
import org.apache.cassandra.exceptions.AuthenticationException;
import org.apache.cassandra.utils.FBUtilities;

public class MutualTlsCertificateValidityPeriodValidator
{
    @Nonnull
    private final int maxCertificateValidityPeriodMinutes;

    public MutualTlsCertificateValidityPeriodValidator(@Nullable DurationSpec.IntMinutesBound maxCertificateValidityPeriod)
    {
        maxCertificateValidityPeriodMinutes = maxCertificateValidityPeriod != null
                                              ? maxCertificateValidityPeriod.toMinutes()
                                              // Sufficiently large value that exceeds any reasonable valid configuration
                                              : Integer.MAX_VALUE;
    }

    /**
     * Validates that the certificate's validity period does not exceed the {@code #maxCertificateValidityPeriod}
     * and returns the number of minutes to certificate expiration since the verification started.
     *
     * @param certificates the certificate chain
     * @return the number of minutes to certificate expiration since the verification started, or {@code -1}
     * if no certificate chain was provided
     * @throws AuthenticationException when the {@link X509Certificate#getNotBefore()} or
     *                                 {@link X509Certificate#getNotAfter()} dates are null, or when the certificate
     *                                 validity exceeds the maximum allowed certificate validity
     */
    public int validate(Certificate[] certificates) throws AuthenticationException
    {
        X509Certificate[] x509Certificates = MutualTlsUtil.castCertsToX509(certificates);
        if (x509Certificates == null || x509Certificates.length == 0)
        {
            return -1;
        }

        Date notAfter = x509Certificates[0].getNotAfter();

        int minutesToCertificateExpiration = (int) ChronoUnit.MINUTES.between(FBUtilities.now(), notAfter.toInstant());
        int certificateValidityPeriodMinutes = certificateValidityPeriodInMinutes(x509Certificates[0]);
        if (certificateValidityPeriodMinutes > maxCertificateValidityPeriodMinutes)
        {
            String errorMessage = String.format("The validity period of the provided certificate (%s) exceeds " +
                                                "the maximum allowed validity period of %s",
                                                MutualTlsUtil.toHumanReadableCertificateExpiration(certificateValidityPeriodMinutes),
                                                MutualTlsUtil.toHumanReadableCertificateExpiration(maxCertificateValidityPeriodMinutes));
            throw new AuthenticationException(errorMessage);
        }

        return minutesToCertificateExpiration;
    }

    int certificateValidityPeriodInMinutes(X509Certificate certificate)
    {
        return (int) ChronoUnit.MINUTES.between(certificate.getNotBefore().toInstant(),
                                                certificate.getNotAfter().toInstant());
    }
}
