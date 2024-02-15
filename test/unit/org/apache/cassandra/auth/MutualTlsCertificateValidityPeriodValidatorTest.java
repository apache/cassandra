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

import org.junit.Test;

import org.apache.cassandra.config.DurationSpec;
import org.assertj.core.api.Assertions;

import static org.apache.cassandra.auth.AuthTestUtils.loadCertificateChain;
import static org.apache.cassandra.auth.SpiffeCertificateValidatorTest.CERTIFICATE_PATH;
import static org.mockito.Mockito.mock;

/**
 * Unit tests for {@link MutualTlsCertificateValidityPeriodValidator}
 */
public class MutualTlsCertificateValidityPeriodValidatorTest
{
    @Test
    public void testValidator() throws CertificateException
    {
        // Create a validator that allows certificates to be issued for 4000 days or less
        MutualTlsCertificateValidityPeriodValidator validator = new MutualTlsCertificateValidityPeriodValidator(new DurationSpec.IntMinutesBound("4000d"));
        Certificate[] chain = loadCertificateChain(CERTIFICATE_PATH);
        Assertions.assertThat(validator.validate(chain))
                  .isNotNull()
                  .isGreaterThan(0)
                  .isLessThan((int) TimeUnit.DAYS.toMinutes(3650));
    }

    @Test
    public void testValidatorWithNullInput()
    {
        MutualTlsCertificateValidityPeriodValidator validator = new MutualTlsCertificateValidityPeriodValidator(null);
        Assertions.assertThat(validator.validate(null)).isEqualTo(-1);
    }

    @Test
    public void testValidatorWithNoX509Certs()
    {
        Certificate[] chain = { mock(Certificate.class) };
        MutualTlsCertificateValidityPeriodValidator validator = new MutualTlsCertificateValidityPeriodValidator(null);
        Assertions.assertThat(validator.validate(chain)).isEqualTo(-1);
    }

    @Test
    public void testValidatorWithoutMaxCertificateAge() throws CertificateException
    {
        // Create a validator that does not validate for certificate age
        MutualTlsCertificateValidityPeriodValidator validator = new MutualTlsCertificateValidityPeriodValidator(null);
        Certificate[] chain = loadCertificateChain(CERTIFICATE_PATH);
        Assertions.assertThat(validator.validate(chain))
                  .isGreaterThan(0)
                  .isLessThan((int) TimeUnit.DAYS.toMinutes(3650));
    }

    @Test
    public void testThrowsWhenAgeExceedsMaximumAllowedAge() throws CertificateException
    {
        // Create a validator that allows certificates to be issued for 30 days or less
        MutualTlsCertificateValidityPeriodValidator validator = new MutualTlsCertificateValidityPeriodValidator(new DurationSpec.IntMinutesBound("30d"));
        Certificate[] chain = loadCertificateChain(CERTIFICATE_PATH);
        Assertions.assertThatThrownBy(() -> validator.validate(chain))
                  .hasMessageContaining("The validity period of the provided certificate (3650 days) exceeds the maximum allowed validity period of 30 days");
    }
}
