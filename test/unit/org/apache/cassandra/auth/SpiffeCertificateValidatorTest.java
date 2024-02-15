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
import java.util.concurrent.TimeUnit;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import org.apache.cassandra.exceptions.AuthenticationException;

import static org.apache.cassandra.auth.AuthTestUtils.loadCertificateChain;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class SpiffeCertificateValidatorTest
{
    private static final String CERTIFICATE_PATH = "auth/SampleMtlsClientCertificate.pem";
    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Test
    public void getIdentityShouldReturnSpiffeTest() throws CertificateException
    {
        SpiffeCertificateValidator validator = new SpiffeCertificateValidator();
        Certificate[] chain = loadCertificateChain(CERTIFICATE_PATH);
        String spiffe = validator.identity(chain);
        assertEquals("spiffe://testdomain.com/testIdentifier/testValue", spiffe);
    }

    @Test
    public void getIdentityShouldThrowExceptionOnNoSpiffeInSAN() throws CertificateException
    {
        SpiffeCertificateValidator validator = new SpiffeCertificateValidator();
        String invalidCertificate = "auth/SampleInvalidCertificate.pem";
        Certificate[] chain = loadCertificateChain(invalidCertificate);
        expectedException.expectMessage("Unable to extract Spiffe from the certificate");
        expectedException.expect(AuthenticationException.class);
        validator.identity(chain);
    }

    @Test
    public void testThrowsAuthenticationExceptionOnMaxAgeExceeded() throws CertificateException
    {
        SpiffeCertificateValidator validator = new SpiffeCertificateValidator();
        Certificate[] chain = loadCertificateChain(CERTIFICATE_PATH);
        expectedException.expectMessage("The age of the provided certificate exceeds the maximum allowed age of 43200 minutes");
        expectedException.expect(AuthenticationException.class);
        validator.isValidCertificate(chain, (int) TimeUnit.DAYS.toMinutes(30));
    }

    @Test
    public void testCertificateIsValidOnValidMaxAgeLimit() throws CertificateException
    {
        SpiffeCertificateValidator validator = new SpiffeCertificateValidator();
        Certificate[] chain = loadCertificateChain(CERTIFICATE_PATH);
        // roughly 11 years in days, the test certificate expires in 10 years
        int elevenYearsInDays = 365 * 11;
        int elevenYearsInMinutes = (int) TimeUnit.DAYS.toMinutes(elevenYearsInDays);
        assertTrue(validator.isValidCertificate(chain, elevenYearsInMinutes));
    }
}
