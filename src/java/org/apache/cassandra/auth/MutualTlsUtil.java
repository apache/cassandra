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
import java.util.concurrent.TimeUnit;

public final class MutualTlsUtil
{
    private static final int ONE_DAY_IN_MINUTES = (int) TimeUnit.DAYS.toMinutes(1);
    private static final int ONE_HOUR_IN_MINUTES = (int) TimeUnit.HOURS.toMinutes(1);

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

    public static String toHumanReadableCertificateExpiration(int minutesToExpiration)
    {
        if (minutesToExpiration >= ONE_DAY_IN_MINUTES)
        {
            return formatHelper(minutesToDays(minutesToExpiration), "day")
                   + maybeAppendRemainder(minutesToExpiration % ONE_DAY_IN_MINUTES);
        }
        if (minutesToExpiration >= ONE_HOUR_IN_MINUTES)
        {
            return formatHelper((int) TimeUnit.MINUTES.toHours(minutesToExpiration), "hour")
                   + maybeAppendRemainder(minutesToExpiration % ONE_HOUR_IN_MINUTES);
        }
        return formatHelper(minutesToExpiration, "minute");
    }

    public static int minutesToDays(int minutes)
    {
        return (int) TimeUnit.MINUTES.toDays(minutes);
    }

    static String formatHelper(int unit, String singularForm)
    {
        if (unit == 1)
            return unit + " " + singularForm;
        // assumes plural form just adds s at the end
        return unit + " " + singularForm + 's';
    }

    static String maybeAppendRemainder(int remainderInMinutes)
    {
        if (remainderInMinutes == 0)
            return "";
        return ' ' + toHumanReadableCertificateExpiration(remainderInMinutes);
    }
}
