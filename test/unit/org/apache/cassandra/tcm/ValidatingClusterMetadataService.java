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

import java.io.IOException;
import java.util.List;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.tcm.serialization.AsymmetricMetadataSerializer;
import org.apache.cassandra.tcm.serialization.AsymmetricMetadataSerializers;
import org.apache.cassandra.tcm.serialization.Version;
import org.assertj.core.api.Assertions;

public class ValidatingClusterMetadataService extends StubClusterMetadataService
{
    private final List<Version> supportedVersions;

    private ValidatingClusterMetadataService(List<Version> supportedVersions)
    {
        super(new ClusterMetadata(safeGetPartitioner()));
        this.supportedVersions = supportedVersions;
    }

    public static ValidatingClusterMetadataService createAndRegister(Version minVersion)
    {
        return createAndRegister(minVersion.greaterThanOrEqual());
    }

    public static ValidatingClusterMetadataService createAndRegister(List<Version> supportedVersions)
    {
        ValidatingClusterMetadataService cms = new ValidatingClusterMetadataService(supportedVersions);

        ClusterMetadataService.unsetInstance();
        ClusterMetadataService.setInstance(cms);
        return cms;
    }

    private static IPartitioner safeGetPartitioner()
    {
        IPartitioner partitioner = DatabaseDescriptor.getPartitioner();
        return partitioner == null ? Murmur3Partitioner.instance : partitioner;
    }

    private <In, Out> void testSerde(AsymmetricMetadataSerializer<In, Out> serializer, In input)
    {
        for (Version version : supportedVersions)
        {
            try (DataOutputBuffer buffer = DataOutputBuffer.scratchBuffer.get())
            {
                AsymmetricMetadataSerializers.testSerde(buffer, serializer, input, version);
            }
            catch (IOException e)
            {
                throw new AssertionError(String.format("Serde error for version=%s; input=%s", version, input), e);
            }
        }
    }

    @Override
    protected Transformation.Result execute(Transformation transform)
    {
        Transformation.Result result = super.execute(transform);
        if (result.isSuccess())
        {
            Transformation.Success success = result.success();
            Assertions.assertThat(success.affectedMetadata)
                      .describedAs("Affected Metadata keys do not match")
                      .isEqualTo(MetadataKeys.diffKeys(metadata(), success.metadata));
        }
        return result;
    }

    @Override
    public <T1> T1 commit(Transformation transform, CommitSuccessHandler<T1> onSuccess, CommitFailureHandler<T1> onFailure)
    {
        testSerde(transform.kind().serializer(), transform);
        return super.commit(transform, onSuccess, onFailure);
    }

    @Override
    public void setMetadata(ClusterMetadata metadata)
    {
        if (!metadata.epoch.equals(metadata().epoch.nextEpoch()))
            throw new AssertionError("Epochs were not sequential: expected " + metadata().epoch.nextEpoch() + " but given " + metadata.epoch);
        testSerde(ClusterMetadata.serializer, metadata);
        super.setMetadata(metadata);
    }
}
