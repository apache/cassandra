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

import org.junit.Test;

import accord.utils.Gens;
import org.assertj.core.api.Assertions;

import static accord.utils.Property.qt;
import static org.apache.cassandra.utils.Generators.toGen;

public class CassandraGeneratorsTest
{
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
}