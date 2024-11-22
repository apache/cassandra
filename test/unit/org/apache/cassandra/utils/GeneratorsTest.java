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

import java.util.List;
import java.util.Objects;
import java.util.Random;

import com.google.common.net.InternetDomainName;
import org.junit.Test;

import accord.utils.Property;
import org.apache.cassandra.db.marshal.AsciiType;
import org.assertj.core.api.Assertions;
import org.quicktheories.core.Gen;
import org.quicktheories.generators.SourceDSL;
import org.quicktheories.impl.JavaRandom;

import static org.apache.cassandra.utils.AbstractTypeGenerators.stringComparator;
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
    public void dnsDomainName()
    {
        qt().forAll(Generators.DNS_DOMAIN_NAME).checkAssert(InternetDomainName::from);
    }

    @Test
    public void asciiDeterministic()
    {
        AbstractTypeGenerators.TypeSupport<String> support = AbstractTypeGenerators.TypeSupport.of(AsciiType.instance, SourceDSL.strings().ascii().ofLengthBetween(1, 10), stringComparator(AsciiType.instance));
        int samples = 100;
        int attempts = 100;
        Property.qt().check(rs -> checkDeterministicGeneration(attempts, samples, rs.nextLong(), support.valueGen));
        Property.qt().check(rs -> checkDeterministicGeneration(attempts, samples, rs.nextLong(), support.bytesGen()));
    }

    @Test
    public void asciiThereAndBackAgain()
    {
        qt().forAll(SourceDSL.strings().ascii().ofLengthBetween(1, 100)).checkAssert(ascii -> {
            String accum = ascii;
            for (int i = 0; i < 100; i++)
                accum = AsciiType.instance.compose(AsciiType.instance.decompose(accum));
            Assertions.assertThat(accum).isEqualTo(ascii);
        });
    }

    private static <T> void checkDeterministicGeneration(int attempts, int samples, long seed, Gen<T> gen)
    {
        List<T> goldSet = null;
        for (int i = 0; i < attempts; i++)
        {
            JavaRandom qt = new JavaRandom(new Random(seed));
            List<T> sample = SourceDSL.lists().of(gen).ofSize(samples).generate(qt);
            if (goldSet == null)
            {
                goldSet = sample;
                continue;
            }
            if (!Objects.equals(sample, goldSet))
                throw new AssertionError("seed=" + seed);
        }
    }
}
