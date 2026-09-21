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

package org.apache.cassandra.distributed.test.cql3;

import java.util.List;

import accord.utils.RandomSource;

import org.apache.cassandra.cql3.ast.CreateIndexDDL;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.IInstanceConfig;
import org.apache.cassandra.schema.TableMetadata;

public class MultiNodeTableWalkWithWitnessesTest extends MultiNodeTableWalkWithMutationTrackingTest
{
    @Override
    protected void clusterConfig(IInstanceConfig c)
    {
        super.clusterConfig(c);

        // Enable transient replication replication
        c.set("transient_replication_enabled", "true");
    }

    @Override
    protected List<CreateIndexDDL.Indexer> supportedIndexers()
    {
        // TODO (expected): Implement supported indexers for witnesses
        return List.of();
    }

    /**
     * A range read that crosses from a fully replicated range into a witnessed one is answered as if it were full
     * throughout, so a full table scan here can lose a partition depending on the seed. That is a defect in the
     * generic replica plan, not in the tracked read path, and it is fixed by "Split a range read where full
     * replication ends and witnessing begins"; until then this walk runs the partition restricted reads it always
     * ran.
     */
    @Override
    protected boolean allowRangeReads()
    {
        return false;
    }

    protected class WitnessState extends MultiNodeTableWalkBase.MultiNodeState
    {
        public WitnessState(RandomSource rs, Cluster cluster)
        {
            super(rs, cluster);
        }

        @Override
        protected String createKeyspaceCQL(TableMetadata metadata)
        {
            return createKeyspaceCQL(metadata, "3/1");
        }
    }

    @Override
    protected State createState(RandomSource rs, Cluster cluster)
    {
        return new WitnessState(rs, cluster);
    }
}
