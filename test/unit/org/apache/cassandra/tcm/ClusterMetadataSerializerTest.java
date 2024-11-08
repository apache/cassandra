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

package org.apache.cassandra.tcm;

import org.junit.Test;

import accord.utils.Gen;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.service.accord.AccordFastPath;
import org.apache.cassandra.service.accord.AccordStaleReplicas;
import org.apache.cassandra.service.consensus.migration.ConsensusMigrationState;
import org.apache.cassandra.tcm.membership.NodeVersion;
import org.apache.cassandra.tcm.serialization.AsymmetricMetadataSerializers;
import org.apache.cassandra.tcm.serialization.Version;
import org.apache.cassandra.utils.CassandraGenerators.ClusterMetadataBuilder;
import org.apache.cassandra.utils.Generators;
import org.assertj.core.api.Assertions;

import static accord.utils.Property.qt;

public class ClusterMetadataSerializerTest
{
    static
    {
        DatabaseDescriptor.toolInitialization();
    }

    @Test
    public void serdeLatest()
    {
        DataOutputBuffer output = new DataOutputBuffer();
        qt().forAll(Generators.toGen(new ClusterMetadataBuilder().build())).check(cm -> {
            AsymmetricMetadataSerializers.testSerde(output, ClusterMetadata.serializer, cm, NodeVersion.CURRENT_METADATA_VERSION);
        });
    }

    @Test
    public void serdeWithoutAccord()
    {
        DataOutputBuffer output = new DataOutputBuffer();
        Gen<ClusterMetadata> gen = Generators.toGen(new ClusterMetadataBuilder().build()).filter(cm -> {
            if (!cm.consensusMigrationState.equals(ConsensusMigrationState.EMPTY))
                return true;
            if (!cm.accordStaleReplicas.equals(AccordStaleReplicas.EMPTY))
                return true;
            if (!cm.accordFastPath.equals(AccordFastPath.EMPTY))
                return true;
            return false;
        });
        qt().forAll(gen).check(cm -> {
            output.clear();
            Version version = Version.V2; // this is the version before accord
            long expectedSize = ClusterMetadata.serializer.serializedSize(cm, version);
            ClusterMetadata.serializer.serialize(cm, output, version);
            Assertions.assertThat(output.getLength()).describedAs("The serialized size and bytes written do not match").isEqualTo(expectedSize);
            DataInputBuffer in = new DataInputBuffer(output.unsafeGetBufferAndFlip(), false);
            ClusterMetadata read = ClusterMetadata.serializer.deserialize(in, version);
            Assertions.assertThat(read).isNotEqualTo(cm);

            Assertions.assertThat(read.consensusMigrationState).isEqualTo(ConsensusMigrationState.EMPTY);
            Assertions.assertThat(read.accordStaleReplicas).isEqualTo(AccordStaleReplicas.EMPTY);
            Assertions.assertThat(read.accordFastPath).isEqualTo(AccordFastPath.EMPTY);
        });
    }
}
