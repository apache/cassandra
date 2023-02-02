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

package org.apache.cassandra.db.marshal;

import org.apache.cassandra.transport.ProtocolVersion;
import org.assertj.core.api.Assertions;
import org.junit.Test;

import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class PartitionerDefinedOrderTest
{
    private static final String key = "key";
    private static final AbstractType<?> type = UTF8Type.instance;

    @Test
    public void testToJsonStringWithBaseType()
    {
        TypeParserTest.assertForEachPartitioner(partitioner -> {
            if (partitioner.partitionOrdering(null) instanceof PartitionerDefinedOrder)
            {
                PartitionerDefinedOrder partitionerDefinedOrder = (PartitionerDefinedOrder) partitioner.partitionOrdering(null);
                String jsonString = partitionerDefinedOrder.withPartitionKeyType(type).toJSONString(UTF8Type.instance.decompose(key), ProtocolVersion.V4);
                assertTrue(jsonString.contains(key));
            }
        });
    }

    @Test
    public void testToJsonStringWithOutBaseType()
    {
        TypeParserTest.assertForEachPartitioner(partitioner -> {
            if (partitioner.partitionOrdering(null) instanceof PartitionerDefinedOrder)
            {
                PartitionerDefinedOrder partitionerDefinedOrder = (PartitionerDefinedOrder) partitioner.partitionOrdering(null);
                assertNull(partitionerDefinedOrder.getPartitionKeyType());
                Assertions.assertThatThrownBy(() -> partitionerDefinedOrder.toJSONString(UTF8Type.instance.decompose(key), ProtocolVersion.V4))
                          .hasMessageContaining("PartitionerDefinedOrder's toJSONString method needs a partition key type but now is null.");
            }
        });
    }
}
