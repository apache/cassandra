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

package org.apache.cassandra.stress.generate;

import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

import org.junit.Test;

import org.apache.cassandra.db.marshal.UUIDType;
import org.apache.cassandra.stress.generate.values.GeneratorConfig;
import org.apache.cassandra.stress.generate.values.UUIDs;
import org.apache.cassandra.stress.settings.OptionDistribution;

import static org.junit.Assert.assertEquals;

public class PartitionIteratorTest
{
    @Test
    public void seedDiffersForGeneratedUuids()
    {
        UUIDs generator = new UUIDs("id", new GeneratorConfig("id", null, null, OptionDistribution.get("seq(1..10)")));
        Set<UUID> uuids = new HashSet<>();
        Set<Long> seeds = new HashSet<>();
        for (int i = 0; i < 10; i++)
        {
            UUID uuid = generator.generate();
            uuids.add(uuid);
            seeds.add(PartitionIterator.seed(uuid, generator.type, 0));
        }
        assertEquals(10, uuids.size());
        assertEquals(10, seeds.size());
    }

    @Test
    public void seedUnchangedForUuidsWithDifferentHalves()
    {
        UUID uuid = new UUID(5, 9);
        assertEquals(7 * 31 + (5 ^ 9), PartitionIterator.seed(uuid, UUIDType.instance, 7));
    }
}
