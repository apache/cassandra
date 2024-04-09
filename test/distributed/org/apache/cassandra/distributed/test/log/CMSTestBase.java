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
package org.apache.cassandra.distributed.test.log;

import java.net.InetAddress;
import java.net.UnknownHostException;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.guardrails.Guardrails;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.distributed.api.IIsolatedExecutor;
import org.apache.cassandra.harry.sut.TokenPlacementModel;
import org.apache.cassandra.locator.IEndpointSnitch;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.locator.Replica;
import org.apache.cassandra.locator.ReplicaCollection;
import org.apache.cassandra.schema.DistributedSchema;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.Keyspaces;
import org.apache.cassandra.schema.SchemaProvider;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.ClusterMetadataService;
import org.apache.cassandra.tcm.Commit;
import org.apache.cassandra.tcm.MetadataSnapshots;
import org.apache.cassandra.tcm.Processor;
import org.apache.cassandra.tcm.Transformation;
import org.apache.cassandra.tcm.log.LocalLog;
import org.apache.cassandra.tcm.ownership.EntireRange;
import org.apache.cassandra.tcm.ownership.UniformRangePlacement;
import org.apache.cassandra.tcm.transformations.AlterSchema;
import org.apache.cassandra.tcm.transformations.cms.Initialize;
import org.apache.cassandra.utils.FBUtilities;
import org.mockito.Mockito;

public class CMSTestBase
{
    static
    {
        DatabaseDescriptor.daemonInitialization();
        DatabaseDescriptor.setEndpointSnitch(new IEndpointSnitch()
        {
            public String getRack(InetAddressAndPort endpoint)
            {
                ClusterMetadata metadata = ClusterMetadata.current();
                return metadata.directory.location(metadata.directory.peerId(endpoint)).rack;
            }
            public String getDatacenter(InetAddressAndPort endpoint)
            {
                ClusterMetadata metadata = ClusterMetadata.current();
                return metadata.directory.location(metadata.directory.peerId(endpoint)).datacenter;
            }
            public <C extends ReplicaCollection<? extends C>> C sortedByProximity(InetAddressAndPort address, C addresses) {return null;}
            public int compareEndpoints(InetAddressAndPort target, Replica r1, Replica r2) {return 0;}
            public void gossiperStarting() {}
            public boolean isWorthMergingForRangeQuery(ReplicaCollection<?> merged, ReplicaCollection<?> l1, ReplicaCollection<?> l2) {return false;}
        });
        DatabaseDescriptor.setDefaultKeyspaceRF(1);
        Guardrails.instance.setMinimumReplicationFactorThreshold(1, 1);

        try
        {
            DatabaseDescriptor.setBroadcastAddress(InetAddress.getByName("127.0.0.1"));
        }
        catch (UnknownHostException e)
        {
            e.printStackTrace();
        }
    }

    public static class CMSSut implements AutoCloseable
    {
        public final Murmur3Partitioner partitioner;
        public final LocalLog log;
        public final ClusterMetadataService service;
        public final SchemaProvider schemaProvider;
        public final TokenPlacementModel.ReplicationFactor rf;

        public CMSSut(IIsolatedExecutor.SerializableFunction<LocalLog, Processor> processorFactory, boolean addListeners, TokenPlacementModel.ReplicationFactor rf)
        {
            partitioner = Murmur3Partitioner.instance;
            this.rf = rf;
            schemaProvider = Mockito.mock(SchemaProvider.class);
            ClusterMetadata initial = new ClusterMetadata(partitioner);
            log = LocalLog.logSpec()
                          .sync()
                          .withInitialState(initial)
                          .withDefaultListeners(addListeners)
                          .createLog();

            service = new ClusterMetadataService(new UniformRangePlacement(),
                                                 MetadataSnapshots.NO_OP,
                                                 log,
                                                 processorFactory.apply(log),
                                                 Commit.Replicator.NO_OP,
                                                 true);

            ClusterMetadataService.setInstance(service);
            log.readyUnchecked();
            log.bootstrap(FBUtilities.getBroadcastAddressAndPort());
            service.commit(new Initialize(ClusterMetadata.current()) {
                public Result execute(ClusterMetadata prev)
                {
                    ClusterMetadata next = baseState;
                    DistributedSchema initialSchema = new DistributedSchema(prev.schema.getKeyspaces());
                    ClusterMetadata.Transformer transformer = next.transformer().with(initialSchema);
                    return Transformation.success(transformer, EntireRange.affectedRanges(prev));
                }

            });
            service.commit(new AlterSchema((cm) -> {
                return cm.schema.getKeyspaces().with(Keyspaces.of(KeyspaceMetadata.create("test", rf.asKeyspaceParams())));
            }, schemaProvider));
        }

        public void close() throws Exception
        {
            ClusterMetadataService.unsetInstance();
        }
    }
}
