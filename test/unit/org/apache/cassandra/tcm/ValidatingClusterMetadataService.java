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
import java.util.TreeMap;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.tcm.log.Entry;
import org.apache.cassandra.tcm.log.LogState;
import org.apache.cassandra.tcm.sequences.LockedRanges;
import org.apache.cassandra.tcm.serialization.AsymmetricMetadataSerializer;
import org.apache.cassandra.tcm.serialization.AsymmetricMetadataSerializers;
import org.apache.cassandra.tcm.serialization.Version;
import org.assertj.core.api.Assertions;

public class ValidatingClusterMetadataService extends StubClusterMetadataService
{
    private final List<Version> supportedVersions;
    private final TreeMap<Epoch, ClusterMetadata> epochs = new TreeMap<>();

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
        epochs.put(metadata.epoch, metadata);
        super.setMetadata(metadata);
    }

    @Override
    public Processor processor()
    {
        Processor delegate = super.processor();
        return new Processor()
        {
            @Override
            public Commit.Result commit(Entry.Id entryId, Transformation transform, Epoch lastKnown, Retry.Deadline retryPolicy)
            {
                return delegate.commit(entryId, transform, lastKnown, retryPolicy);
            }

            @Override
            public ClusterMetadata fetchLogAndWait(Epoch waitFor, Retry.Deadline retryPolicy)
            {
                return delegate.fetchLogAndWait(waitFor, retryPolicy);
            }

            @Override
            public LogState getLocalState(Epoch lowEpoch, Epoch highEpoch, boolean includeSnapshot)
            {
                if (!epochs.containsKey(lowEpoch))
                    throw new AssertionError("Unknown epoch: " + lowEpoch);
                ClusterMetadata base = epochs.get(lowEpoch);
                ImmutableList.Builder<Entry> entries = ImmutableList.builder();
                int id = 0;
                for (ClusterMetadata cm : epochs.subMap(lowEpoch, false, highEpoch, true).values())
                    entries.add(new Entry(new Entry.Id(id++), cm.epoch, new MockTransformer(cm)));
                return new LogState(includeSnapshot ? base : null, entries.build());
            }

            @Override
            public LogState getLogState(Epoch lowEpoch, Epoch highEpoch, boolean includeSnapshot, Retry.Deadline retryPolicy)
            {
                return getLocalState(lowEpoch, highEpoch, includeSnapshot);
            }
        };
    }

    private static class MockTransformer implements Transformation
    {
        private final ClusterMetadata result;

        private MockTransformer(ClusterMetadata result)
        {
            this.result = result;
        }

        @Override
        public Kind kind()
        {
            return null;
        }

        @Override
        public Result execute(ClusterMetadata metadata)
        {
            return new Success(result, LockedRanges.AffectedRanges.EMPTY, ImmutableSet.of());
        }
    }
}
