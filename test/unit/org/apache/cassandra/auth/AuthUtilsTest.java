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

import org.mindrot.jbcrypt.BCrypt;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for the pure helpers in {@link AuthUtils}
 */
public class AuthUtilsTest
{
    @Test
    public void escapeCqlLiteralLeavesValuesWithoutQuotesUnchanged()
    {
        assertThat(AuthUtils.escapeCqlLiteral("cassandra")).isEqualTo("cassandra");
        assertThat(AuthUtils.escapeCqlLiteral("")).isEqualTo("");
        assertThat(AuthUtils.escapeCqlLiteral("role_with-various.chars")).isEqualTo("role_with-various.chars");
    }

    @Test
    public void escapeCqlLiteralDoublesSingleQuotes()
    {
        assertThat(AuthUtils.escapeCqlLiteral("o'brien")).isEqualTo("o''brien");
    }

    @Test
    public void escapeCqlLiteralDoublesEveryQuote()
    {
        assertThat(AuthUtils.escapeCqlLiteral("'")).isEqualTo("''");
        assertThat(AuthUtils.escapeCqlLiteral("''")).isEqualTo("''''");
        assertThat(AuthUtils.escapeCqlLiteral("a'b'c")).isEqualTo("a''b''c");
        assertThat(AuthUtils.escapeCqlLiteral("'lead")).isEqualTo("''lead");
        assertThat(AuthUtils.escapeCqlLiteral("trail'")).isEqualTo("trail''");
    }

    @Test
    public void escapeCqlLiteralNeutralisesInjectionAttempt()
    {
        String malicious = "x'; DROP KEYSPACE system_auth; --";
        assertThat(AuthUtils.escapeCqlLiteral(malicious)).isEqualTo("x''; DROP KEYSPACE system_auth; --");
    }

    @Test
    public void escapeCqlLiteralReturnsNullUnchanged()
    {
        assertThat(AuthUtils.escapeCqlLiteral(null)).isNull();
    }

    @Test
    public void hashpwProducesVerifiableBcryptHash()
    {
        String hash = AuthUtils.hashpw("cassandra");
        assertThat(hash).startsWith("$2a$");
        assertThat(BCrypt.checkpw("cassandra", hash)).isTrue();
        assertThat(BCrypt.checkpw("wrong", hash)).isFalse();
    }

    @Test
    public void hashpwIsSaltedSoRepeatedHashesDiffer()
    {
        assertThat(AuthUtils.hashpw("cassandra")).isNotEqualTo(AuthUtils.hashpw("cassandra"));
    }
}
