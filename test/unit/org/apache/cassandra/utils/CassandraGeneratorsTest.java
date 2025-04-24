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

import java.util.Arrays;
import java.util.List;

import org.assertj.core.api.Assertions;
import org.junit.Test;

import accord.utils.Gens;
import accord.utils.LazyToString;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.CounterColumnType;
import org.apache.cassandra.db.marshal.DecimalType;
import org.apache.cassandra.db.marshal.DurationType;
import org.apache.cassandra.db.marshal.EmptyType;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.utils.CassandraGenerators.TableMetadataBuilder;

import static accord.utils.Property.qt;
import static org.apache.cassandra.utils.Generators.toGen;

public class CassandraGeneratorsTest
{
    private static final List<AbstractType<?>> NOT_ALLOWED_IN_PRIMARY_KEY = Arrays.asList(EmptyType.instance,
                                                                                          DurationType.instance,
                                                                                          DecimalType.instance,
                                                                                          CounterColumnType.instance);

    @Test
    public void partitionerToToken()
    {
        qt().forAll(Gens.random(), toGen(CassandraGenerators.partitioners()))
            .check((rs, p) -> Assertions.assertThat(toGen(CassandraGenerators.token(p)).next(rs)).isNotNull());
    }

    @Test
    public void partitionerKeys()
    {
        qt().forAll(Gens.random(), toGen(CassandraGenerators.partitioners()))
            .check((rs, p) -> Assertions.assertThat(toGen(CassandraGenerators.decoratedKeys(i -> p)).next(rs)).isNotNull());
    }

    @Test
    public void primaryKeysNoUnsafeTypes()
    {
        qt().forAll(toGen(new TableMetadataBuilder().build())).check(table -> {
            for (ColumnMetadata pk : table.primaryKeyColumns())
            {
                for (AbstractType<?> t : NOT_ALLOWED_IN_PRIMARY_KEY)
                {
                    Assertions.assertThat(AbstractTypeGenerators.contains(pk.type, t))
                              .describedAs("Expected type %s not to be found in %s", t.asCQL3Type(), new LazyToString(() -> AbstractTypeGenerators.typeTree(pk.type)))
                              .isFalse();
                }
            }
        });
    }
}
