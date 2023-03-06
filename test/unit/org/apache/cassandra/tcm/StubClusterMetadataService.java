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

import java.util.function.Function;
import java.util.function.Supplier;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.tcm.log.Entry;
import org.apache.cassandra.tcm.log.LocalLog;
import org.apache.cassandra.tcm.log.LogStorage;
import org.apache.cassandra.tcm.log.Replication;
import org.apache.cassandra.tcm.ownership.PlacementProvider;
import org.apache.cassandra.tcm.ownership.UniformRangePlacement;

public class StubClusterMetadataService extends ClusterMetadataService
{

    public static StubClusterMetadataService forTesting()
    {
        return new StubClusterMetadataService(null,
                                              new ClusterMetadata(DatabaseDescriptor.getPartitioner()),
                                              null,
                                              null);
    }

    @Override
    public ClusterMetadata metadata()
    {
        return ((StubProcessor)processor()).metadata;
    }

    public void setMetadata(ClusterMetadata metadata)
    {
        ((StubProcessor)processor()).metadata = metadata;
    }

    private StubClusterMetadataService(PlacementProvider placementProvider,
                                       ClusterMetadata initial,
                                       Function<Processor, Processor> wrapProcessor,
                                       Supplier<State> cmsStateSupplier)
    {
        super(new UniformRangePlacement(),
              MetadataSnapshots.NO_OP,
              LocalLog.asyncForTests(LogStorage.None, initial, false),
              new StubProcessor(initial),
              Commit.Replicator.NO_OP,
              false);
    }

    private static class StubProcessor implements Processor
    {
        private ClusterMetadata metadata;
        StubProcessor(ClusterMetadata metadata)
        {
            this.metadata = metadata;
        }

        @Override
        public Commit.Result commit(Entry.Id entryId, Transformation transform, Epoch lastKnown)
        {
            Transformation.Result result = transform.execute(metadata);
            if (result.isSuccess())
            {
                metadata = result.success().metadata;
                return new Commit.Result.Success(metadata.epoch, Replication.EMPTY);
            }

            return new Commit.Result.Failure(result.rejected().reason, true);
        }

        @Override
        public ClusterMetadata replayAndWait()
        {
            return metadata;
        }
    }
}
