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

package org.apache.cassandra.tcm.transformations.cms;

import java.io.IOException;

import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.locator.MetaStrategy;
import org.apache.cassandra.locator.Replica;
import org.apache.cassandra.schema.DistributedMetadataLogKeyspace;
import org.apache.cassandra.schema.DistributedSchema;
import org.apache.cassandra.schema.Keyspaces;
import org.apache.cassandra.tcm.Epoch;
import org.apache.cassandra.tcm.Transformation;
import org.apache.cassandra.tcm.serialization.AsymmetricMetadataSerializer;
import org.apache.cassandra.tcm.serialization.Version;
import org.apache.cassandra.schema.ReplicationParams;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.ownership.DataPlacement;
import org.apache.cassandra.tcm.ownership.DataPlacements;
import org.apache.cassandra.tcm.sequences.LockedRanges;

public class PreInitialize implements Transformation
{
    public static Serializer serializer = new Serializer();

    public final InetAddressAndPort addr;
    public final String datacenter;

    private PreInitialize(InetAddressAndPort addr, String datacenter)
    {
        this.addr = addr;
        this.datacenter = datacenter;
    }

    public static PreInitialize forTesting()
    {
        return new PreInitialize(null, null);
    }

    public static PreInitialize blank()
    {
        return new PreInitialize(null, null);
    }

    public static PreInitialize withFirstCMS(InetAddressAndPort addr, String datacenter)
    {
        return new PreInitialize(addr, datacenter);
    }

    public Kind kind()
    {
        return Kind.PRE_INITIALIZE_CMS;
    }

    public Result execute(ClusterMetadata metadata)
    {
        assert metadata.epoch.isBefore(Epoch.FIRST);

        ClusterMetadata.Transformer transformer = metadata.transformer();

        if (addr != null)
        {
            // If addr != null, then this is being executed on the peer which is actually initializing the log
            // for the very first time.

            // addr and datacenter are only used to bootstrap the replication of the distributed metatada
            // keyspace on the first CMS node. They are never serialized into the distributed metadata log or
            // passed to any other peer.
            //
            // PRE_INITIALIZE_CMS @ Epoch.FIRST, must be followed in the log by INITIALIZE_CMS @ (Epoch.FIRST + 1).
            // The serialization of INITIALIZE_CMS includes the full ClusterMetadata at that point, which is
            // obviously minimal, but will necessarily include the distributed metadata keyspace definition with
            // the replication settings bootstrapped by PRE_INITIALIZE. This full ClusterMetadata becomes the
            // starting point upon which further log entries are applied. So this means that once INITIALIZE_CMS
            // has been committed to the log, the actual content of PRE_INITIALIZE_CMS is irrelevant, even on
            // the first CMS node if it happens to replay it from its local storage after a restart.

            DataPlacement.Builder dataPlacementBuilder = DataPlacement.builder();
            Replica replica = new Replica(addr,
                                          MetaStrategy.partitioner.getMinimumToken(),
                                          MetaStrategy.partitioner.getMinimumToken(),
                                          true);
            dataPlacementBuilder.reads.withReplica(Epoch.FIRST, replica);
            dataPlacementBuilder.writes.withReplica(Epoch.FIRST, replica);
            DataPlacements initialPlacement = metadata.placements.unbuild()
                                                                 .with(ReplicationParams.simpleMeta(1, datacenter),
                                                                       dataPlacementBuilder.build()).build();

            transformer.with(initialPlacement);
            // re-initialise the schema distributed metadata keyspace so it gets the
            // correct replication settings based on the DC of the initial CMS node
            Keyspaces updated = metadata.schema.getKeyspaces()
                                               .withAddedOrReplaced(DistributedMetadataLogKeyspace.initialMetadata(datacenter));
            transformer.with(new DistributedSchema(updated));
        }

        ClusterMetadata.Transformer.Transformed transformed = transformer.build();
        metadata = transformed.metadata.forceEpoch(Epoch.FIRST);
        assert metadata.epoch.is(Epoch.FIRST) : metadata.epoch;

        return new Success(metadata, LockedRanges.AffectedRanges.EMPTY, transformed.modifiedKeys);
    }

    public String toString()
    {
        return "PreInitialize";
    }

    public static class Serializer implements AsymmetricMetadataSerializer<Transformation, PreInitialize>
    {

        public void serialize(Transformation t, DataOutputPlus out, Version version) throws IOException
        {
            assert t.kind() == Kind.PRE_INITIALIZE_CMS;
            PreInitialize bcms = (PreInitialize)t;
            out.writeBoolean(bcms.addr != null);
            if (bcms.addr != null)
                InetAddressAndPort.MetadataSerializer.serializer.serialize(bcms.addr, out, version);
            if (bcms.datacenter != null && version.isAtLeast(Version.V5))
                out.writeUTF(bcms.datacenter);
        }

        public PreInitialize deserialize(DataInputPlus in, Version version) throws IOException
        {
            boolean hasAddr = in.readBoolean();
            if (!hasAddr)
                return PreInitialize.blank();

            InetAddressAndPort addr = InetAddressAndPort.MetadataSerializer.serializer.deserialize(in, version);
            String datacenter = version.isAtLeast(Version.V5) ? in.readUTF() : "";
            return new PreInitialize(addr, datacenter);
        }

        public long serializedSize(Transformation t, Version version)
        {
            PreInitialize bcms = (PreInitialize)t;
            long size = TypeSizes.sizeof(bcms.addr != null);

            if (bcms.addr != null)
                size += InetAddressAndPort.MetadataSerializer.serializer.serializedSize(bcms.addr, version);
            if (bcms.datacenter != null && version.isAtLeast(Version.V5))
                size += TypeSizes.sizeof(bcms.datacenter);
            return size;
        }
    }
}