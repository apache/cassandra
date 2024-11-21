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

package org.apache.cassandra.utils;

import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

import com.google.common.net.InternetDomainName;
import org.junit.Assert;
import org.junit.Test;

import org.assertj.core.api.Assertions;
import org.quicktheories.core.Gen;
import org.quicktheories.core.RandomnessSource;
import org.quicktheories.impl.JavaRandom;

import static org.quicktheories.QuickTheory.qt;

public class GeneratorsTest
{
    @Test
    public void randomUUID()
    {
        qt().forAll(Generators.UUID_RANDOM_GEN).checkAssert(uuid -> {
            Assertions.assertThat(uuid.version())
                      .as("version was not random uuid")
                      .isEqualTo(4);
            Assertions.assertThat(uuid.variant())
                      .as("varient not set to IETF (2)")
                      .isEqualTo(2);
        });
    }

    @Test
    public void randomUUIDEntropy()
    {
        UUIDEntropy(Generators.UUID_RANDOM_GEN);
    }

    @Test
    public void timeUUIDEntropy()
    {
        UUIDEntropy(Generators.UUID_TIME_GEN);
    }

    private void UUIDEntropy(Gen<UUID> gen)
    {
        RandomnessSource rng = new JavaRandom(100);
        Set<UUID> uuids = new HashSet<>();
        for (int i = 0; i < 100_000; i++)
            uuids.add(gen.generate(rng));
        Assert.assertEquals(100_000, uuids.size());
    }


    @Test
    public void dnsDomainName()
    {
        qt().forAll(Generators.DNS_DOMAIN_NAME).checkAssert(InternetDomainName::from);
    }
}
