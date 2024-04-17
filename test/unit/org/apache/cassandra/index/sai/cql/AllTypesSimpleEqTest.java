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

package org.apache.cassandra.index.sai.cql;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import accord.utils.Gen;
import accord.utils.Gens;
import org.apache.cassandra.cql3.CQL3Type;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.DecimalType;
import org.apache.cassandra.index.sai.StorageAttachedIndex;
import org.apache.cassandra.utils.AbstractTypeGenerators;
import org.apache.cassandra.utils.Generators;
import org.quicktheories.generators.SourceDSL;

@RunWith(Parameterized.class)
public class AllTypesSimpleEqTest extends AbstractSimpleEqTestBase
{
    private static final Map<AbstractType<?>, Long> LARGE_DOMAIN_FAILING_SEEDS = Map.of();
    private static final Map<AbstractType<?>, Long> SHORT_DOMAIN_FAILING_SEEDS = Map.of();

    private final AbstractType<?> type;

    public AllTypesSimpleEqTest(AbstractType<?> type)
    {
        this.type = type;
    }

    @Parameterized.Parameters(name = "{0}")
    public static Collection<Object[]> data()
    {
        return StorageAttachedIndex.SUPPORTED_TYPES.stream()
                                                   .map(CQL3Type::getType)
                                                   .distinct()
                                                   .map(t -> new Object[]{ t })
                                                   .collect(Collectors.toList());
    }

    @Test
    public void largeDomain()
    {
        Gen<ByteBuffer> gen = genFromType();
        test(type, LARGE_DOMAIN_FAILING_SEEDS.get(type), 10, rs -> gen);
    }

    @Test
    public void smallDomain()
    {
        Gen<ByteBuffer> gen = genFromType();
        test(type, SHORT_DOMAIN_FAILING_SEEDS.get(type), 10, rs -> {
            List<ByteBuffer> domain = Gens.lists(gen).uniqueBestEffort().ofSizeBetween(5, 100).next(rs);
            return r -> r.pick(domain);
        });
    }

    private Gen<ByteBuffer> genFromType()
    {
        if (type == DecimalType.instance)
            // 0.0 != 0.000 but compare(0.0, 0.000) == 0, this breaks the test
            return Generators.toGen(Generators.bigDecimal(SourceDSL.arbitrary().constant(42), Generators.bigInt())
                                              .map(DecimalType.instance::decompose));
        
        return Generators.toGen(AbstractTypeGenerators.getTypeSupport(type).bytesGen());
    }
}