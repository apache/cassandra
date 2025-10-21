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

import java.util.ArrayList;

import org.junit.Test;

import accord.utils.LazyToString;
import org.apache.cassandra.db.marshal.AbstractType;
import org.assertj.core.api.Assertions;
import org.quicktheories.core.Gen;
import org.quicktheories.generators.SourceDSL;

import static org.quicktheories.QuickTheory.qt;

public class AbstractTypeGeneratorsTest
{
    @Test
    public void withoutPrimitive()
    {
        Gen<AbstractType<?>> primitiveGen = SourceDSL.arbitrary().pick(new ArrayList<>(AbstractTypeGenerators.primitiveTypes()));
        qt().forAll(r -> r).checkAssert(rs -> {
            AbstractType<?> primitiveType = primitiveGen.generate(rs);
            Gen<AbstractType<?>> gen = AbstractTypeGenerators.builder().withoutPrimitive(primitiveType).build();
            for (int i = 0; i < 1000; i++)
            {
                AbstractType<?> type = gen.generate(rs);
                Assertions.assertThat(AbstractTypeGenerators.contains(type, primitiveType))
                          .describedAs("Expected type %s not to be found in %s", primitiveType.asCQL3Type(), new LazyToString(() -> AbstractTypeGenerators.typeTree(type)))
                          .isFalse();
                if (type.subTypes().isEmpty())
                    break; // not worth checking this type again...
            }
        });
    }
}