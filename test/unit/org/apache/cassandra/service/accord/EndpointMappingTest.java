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

package org.apache.cassandra.service.accord;

import org.junit.Test;

import accord.local.Node;
import org.apache.cassandra.utils.CassandraGenerators;
import org.assertj.core.api.Assertions;
import static org.quicktheories.QuickTheory.qt;

import org.quicktheories.generators.SourceDSL;


public class EndpointMappingTest
{
    @Test
    public void identityTest() throws Throwable
    {
        qt().forAll(CassandraGenerators.INET_ADDRESS_AND_PORT_GEN, SourceDSL.integers().between(1, Integer.MAX_VALUE).map(Node.Id::new)).checkAssert((endpoint, id) -> {
            EndpointMapping mapping = EndpointMapping.builder(1).add(endpoint, id).build();
            Assertions.assertThat(mapping.mappedEndpoint(id)).isEqualTo(endpoint);
        });
    }
}
