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

import org.junit.Test;

import org.assertj.core.api.Assertions;

import static org.apache.cassandra.auth.MutualTlsUtil.toHumanReadableCertificateExpiration;

/**
 * Unit tests for {@link MutualTlsUtil}
 */
public class MutualTlsUtilTest
{
    @Test
    public void testToHumanReadableCertificateExpiration()
    {
        Assertions.assertThat(toHumanReadableCertificateExpiration(0)).isEqualTo("0 minutes");
        Assertions.assertThat(toHumanReadableCertificateExpiration(1)).isEqualTo("1 minute");
        Assertions.assertThat(toHumanReadableCertificateExpiration(2)).isEqualTo("2 minutes");
        Assertions.assertThat(toHumanReadableCertificateExpiration(10)).isEqualTo("10 minutes");
        Assertions.assertThat(toHumanReadableCertificateExpiration(60)).isEqualTo("1 hour");
        Assertions.assertThat(toHumanReadableCertificateExpiration(61)).isEqualTo("1 hour 1 minute");
        Assertions.assertThat(toHumanReadableCertificateExpiration(80)).isEqualTo("1 hour 20 minutes");
        Assertions.assertThat(toHumanReadableCertificateExpiration(240)).isEqualTo("4 hours");
        Assertions.assertThat(toHumanReadableCertificateExpiration(1440)).isEqualTo("1 day");
        Assertions.assertThat(toHumanReadableCertificateExpiration(1501)).isEqualTo("1 day 1 hour 1 minute");
        Assertions.assertThat(toHumanReadableCertificateExpiration(1740)).isEqualTo("1 day 5 hours");
        Assertions.assertThat(toHumanReadableCertificateExpiration(2880)).isEqualTo("2 days");
        Assertions.assertThat(toHumanReadableCertificateExpiration(3180)).isEqualTo("2 days 5 hours");
        Assertions.assertThat(toHumanReadableCertificateExpiration(525600)).isEqualTo("365 days");
        Assertions.assertThat(toHumanReadableCertificateExpiration(Integer.MAX_VALUE)).isEqualTo("1491308 days 2 hours 7 minutes");
    }
}
