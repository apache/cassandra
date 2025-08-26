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

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.io.Serializers;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.utils.AccordGenerators;

import static accord.utils.Property.qt;

public class FetchTopologiesTest
{

    static
    {
        DatabaseDescriptor.clientInitialization();
    }

    @Test
    public void fetchTopologiesSerde()
    {
        @SuppressWarnings({ "resource", "IOResourceOpenedButNotSafelyClosed" }) DataOutputBuffer output = new DataOutputBuffer();
        qt().forAll(AccordGenerators.fetchTopologiesGen()).check(expected -> {
            Serializers.testSerde(output, FetchTopologies.serializer, expected);
        });
    }

    @Test
    public void topologyRangeSerde()
    {
        @SuppressWarnings({ "resource", "IOResourceOpenedButNotSafelyClosed" }) DataOutputBuffer output = new DataOutputBuffer();
        qt().forAll(AccordGenerators.topologyRangeGen()).check(expected -> {
            AccordGenerators.maybeUpdatePartitioner(expected.topologies);
            Serializers.testSerde(output, FetchTopologies.responseSerializer, expected);
        });
    }
}