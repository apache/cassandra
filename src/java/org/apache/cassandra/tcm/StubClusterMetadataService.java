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

import java.util.Collections;

import com.google.common.collect.ImmutableMap;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.schema.DistributedMetadataLogKeyspace;
import org.apache.cassandra.schema.DistributedSchema;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.Keyspaces;
import org.apache.cassandra.tcm.Commit.Replicator;
import org.apache.cassandra.tcm.log.Entry;
import org.apache.cassandra.tcm.log.LocalLog;
import org.apache.cassandra.tcm.membership.Directory;
import org.apache.cassandra.tcm.ownership.DataPlacements;
import org.apache.cassandra.tcm.ownership.PlacementProvider;
import org.apache.cassandra.tcm.ownership.TokenMap;
import org.apache.cassandra.tcm.ownership.UniformRangePlacement;
import org.apache.cassandra.tcm.sequences.InProgressSequences;
import org.apache.cassandra.tcm.sequences.LockedRanges;

public class StubClusterMetadataService extends ClusterMetadataService
{

    public static StubClusterMetadataService forClientTools()
    {
        DatabaseDescriptor.setLocalDataCenter("DC1");
        KeyspaceMetadata ks = DistributedMetadataLogKeyspace.initialMetadata(Collections.singleton("DC1"));
        DistributedSchema schema = new DistributedSchema(Keyspaces.of(ks));
        return new StubClusterMetadataService(new ClusterMetadata(DatabaseDescriptor.getPartitioner(),
                                                                  Directory.EMPTY,
                                                                  schema));
    }

    public static StubClusterMetadataService forClientTools(DistributedSchema initialSchema)
    {
        DatabaseDescriptor.setLocalDataCenter("DC1");
        ClusterMetadata metadata = new ClusterMetadata(DatabaseDescriptor.getPartitioner());
        metadata = metadata.transformer().with(initialSchema).build().metadata.forceEpoch(Epoch.EMPTY);
        return new StubClusterMetadataService(metadata);
    }

    public static StubClusterMetadataService forTesting()
    {
        return new StubClusterMetadataService(new ClusterMetadata(DatabaseDescriptor.getPartitioner()));
    }

    public static StubClusterMetadataService forTesting(ClusterMetadata metadata)
    {
        return new StubClusterMetadataService(metadata);
    }

    private ClusterMetadata metadata;

    private StubClusterMetadataService(ClusterMetadata initial)
    {
        super(new UniformRangePlacement(),
              MetadataSnapshots.NO_OP,
              LocalLog.logSpec()
                      .loadSSTables(false)
                      .sync()
                      .withInitialState(initial)
                      .createLog(),
              new StubProcessor(),
              Replicator.NO_OP,
              false);
        this.metadata = initial;
        this.log().readyUnchecked();
    }

    private StubClusterMetadataService(PlacementProvider placement,
                                       MetadataSnapshots snapshots,
                                       LocalLog log,
                                       Processor processor,
                                       Replicator replicator,
                                       boolean isMember)
    {
       super(placement, snapshots, log, processor, replicator, isMember);
       this.metadata = log.metadata();
       this.log().readyUnchecked();
    }

    @Override
    public <T1> T1 commit(Transformation transform, CommitSuccessHandler<T1> onSuccess, CommitFailureHandler<T1> onFailure)
    {
        Transformation.Result result = transform.execute(metadata);
        if (result.isSuccess())
        {
            metadata = result.success().metadata;
            return  onSuccess.accept(result.success().metadata);
        }
        return onFailure.accept(result.rejected().code, result.rejected().reason);
    }

    @Override
    public ClusterMetadata fetchLogFromCMS(Epoch awaitAtLeast)
    {
        return metadata;
    }

    @Override
    public ClusterMetadata metadata()
    {
        return metadata;
    }

    public void setMetadata(ClusterMetadata metadata)
    {
        this.metadata = metadata;
    }

    private static class StubProcessor implements Processor
    {

        private StubProcessor() {}

        @Override
        public Commit.Result commit(Entry.Id entryId, Transformation transform, Epoch lastKnown, Retry.Deadline retryPolicy)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public ClusterMetadata fetchLogAndWait(Epoch waitFor, Retry.Deadline retryPolicy)
        {
            throw new UnsupportedOperationException();
        }
    }


    public static Builder builder()
    {
        return new Builder();
    }

    public static Builder builder(IPartitioner partitioner)
    {
        return new Builder(partitioner);
    }

    public static class Builder
    {
        IPartitioner partitioner;
        ClusterMetadata initial;
        MetadataSnapshots snapshots = MetadataSnapshots.NO_OP;

        public StubClusterMetadataService build()
        {
            if (initial == null)
                initial = new ClusterMetadata(Epoch.EMPTY,
                                              partitioner,
                                              DistributedSchema.empty(),
                                              Directory.EMPTY,
                                              new TokenMap(partitioner),
                                              DataPlacements.EMPTY,
                                              LockedRanges.EMPTY,
                                              InProgressSequences.EMPTY,
                                              ImmutableMap.of());
            return new StubClusterMetadataService(new UniformRangePlacement(),
                                                  snapshots != null ? snapshots : MetadataSnapshots.NO_OP,
                                                  LocalLog.logSpec().withInitialState(initial).createLog(),
                                                  new StubProcessor(),
                                                  Replicator.NO_OP,
                                                  false);
        }

        private Builder()
        {
            this(DatabaseDescriptor.getPartitioner());
        }

        private Builder(IPartitioner partitioner)
        {
            this.partitioner = partitioner;
        }

        public Builder withInitial(ClusterMetadata initial)
        {
            this.initial = initial;
            return this;
        }

        public Builder withSnapshots(MetadataSnapshots snapshots)
        {
            this.snapshots = snapshots;
            return this;
        }
    }
}
