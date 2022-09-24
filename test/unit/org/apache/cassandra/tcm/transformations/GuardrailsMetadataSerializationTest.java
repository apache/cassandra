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

package org.apache.cassandra.tcm.transformations;

import java.nio.ByteBuffer;
import java.util.Set;

import org.junit.Test;

import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.tcm.GuardrailsMetadata;
import org.apache.cassandra.tcm.serialization.Version;
import org.apache.cassandra.tcm.transformations.GuardrailTransformations.Flag;
import org.apache.cassandra.tcm.transformations.GuardrailTransformations.Thresholds;
import org.apache.cassandra.tcm.transformations.GuardrailTransformations.Values;

import static org.apache.cassandra.db.ConsistencyLevel.EACH_QUORUM;
import static org.apache.cassandra.tcm.Epoch.EMPTY;
import static org.junit.Assert.assertEquals;

public class GuardrailsMetadataSerializationTest
{
    private final GuardrailsMetadata metadata = new GuardrailsMetadata(EMPTY);

    @Test
    public void testFlagSerDe() throws Exception
    {
        checkSerDe(metadata.with(new Flag("simplestrategy", false)));
    }

    @Test
    public void testThresholdsSerDe() throws Exception
    {
        checkSerDe(metadata.with(new Thresholds("keyspaces", 10, 20)));
    }

    @Test
    public void testValuesSerDe() throws Exception
    {
        checkSerDe(metadata.with(new Values("read_consistency_levels",
                                            Set.of(),
                                            Set.of(EACH_QUORUM.name()),
                                            null)));
    }

    @Test
    public void testCustomSerDe() throws Exception
    {
        checkSerDe(metadata.with(new Values("read_consistency_levels",
                                            Set.of(),
                                            Set.of(EACH_QUORUM.name()),
                                            null)));
    }

    private void checkSerDe(GuardrailsMetadata metadata) throws Exception
    {
        ByteBuffer bb = ByteBuffer.allocate(1024);
        DataOutputPlus dop = new DataOutputBuffer(bb);
        GuardrailsMetadata.serializer.serialize(metadata, dop, Version.V2);

        // rewind so it is read
        bb.position(0);

        DataInputPlus dip = new DataInputBuffer(bb, true);
        GuardrailsMetadata deserialized = GuardrailsMetadata.serializer.deserialize(dip, Version.V2);
        assertEquals(metadata, deserialized);
    }
}
