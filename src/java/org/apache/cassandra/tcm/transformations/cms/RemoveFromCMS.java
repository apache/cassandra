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
import java.util.Objects;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.schema.ReplicationParams;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.Transformation;
import org.apache.cassandra.tcm.ownership.DataPlacement;
import org.apache.cassandra.tcm.serialization.AsymmetricMetadataSerializer;
import org.apache.cassandra.tcm.serialization.Version;

import static org.apache.cassandra.exceptions.ExceptionCode.INVALID;
import static org.apache.cassandra.tcm.transformations.cms.EntireRange.affectedRanges;

public class RemoveFromCMS extends BaseMembershipTransformation
{
    private static final Logger logger = LoggerFactory.getLogger(RemoveFromCMS.class);
    public static final Serializer serializer = new Serializer();
    // Note: this is not configurable as its value must always be consistent across all nodes
    // TODO make this a configurable setting stored in ClusterMetadata itself
    public static final int MIN_SAFE_CMS_SIZE = 3;
    public final boolean force;

    public RemoveFromCMS(InetAddressAndPort addr, boolean force)
    {
        super(addr);
        this.force = force;
    }

    public Kind kind()
    {
        return Kind.REMOVE_FROM_CMS;
    }

    public Result execute(ClusterMetadata prev)
    {
        ClusterMetadata.Transformer transformer = prev.transformer();
        if (!prev.fullCMSMembers().contains(endpoint))
            return new Rejected(INVALID, String.format("%s is not currently a CMS member, cannot remove it", endpoint));

        DataPlacement.Builder builder = prev.placements.get(ReplicationParams.meta()).unbuild();
        builder.reads.withoutReplica(replica);
        builder.writes.withoutReplica(replica);
        DataPlacement proposed = builder.build();
        int minProposedSize = Math.min(proposed.reads.forRange(replica.range()).size(),
                                       proposed.writes.forRange(replica.range()).size());
        if ( minProposedSize < MIN_SAFE_CMS_SIZE)
        {
            logger.warn("Removing {} from CMS members would reduce the service size to {} which is below the " +
                        "configured safe quorum {}. This requires the force option which is set to {}, {}proceeding",
                        endpoint, minProposedSize, MIN_SAFE_CMS_SIZE, force, force ? "" : "not ");
            if (!force)
            {
                return new Rejected(INVALID, String.format("Removing %s from the CMS would reduce the number of members to " +
                                                           "%d, below the configured soft minimum %d. " +
                                                           "To perform this operation anyway, resubmit with force=true",
                                                           endpoint, minProposedSize, MIN_SAFE_CMS_SIZE));
            }
        }

        return success(transformer.with(prev.placements.unbuild().with(ReplicationParams.meta(), proposed).build()),
                       affectedRanges);
    }

    @Override
    public String toString()
    {
        return getClass().getSimpleName() + '{' +
               "endpoint=" + endpoint +
               ", replica=" + replica +
               ", force=" + force +
               '}';
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (!(o instanceof RemoveFromCMS)) return false;
        RemoveFromCMS that = (RemoveFromCMS) o;
        return Objects.equals(endpoint, that.endpoint) && Objects.equals(replica, that.replica) && force == that.force;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(kind(), endpoint, replica, force);
    }

    public static class Serializer implements AsymmetricMetadataSerializer<Transformation, RemoveFromCMS>
    {
        public void serialize(Transformation t, DataOutputPlus out, Version version) throws IOException
        {
            RemoveFromCMS remove = (RemoveFromCMS)t;
            InetAddressAndPort.MetadataSerializer.serializer.serialize(remove.endpoint, out, version);
            out.writeBoolean(remove.force);
        }

        public RemoveFromCMS deserialize(DataInputPlus in, Version version) throws IOException
        {
            InetAddressAndPort addr = InetAddressAndPort.MetadataSerializer.serializer.deserialize(in, version);
            boolean force = in.readBoolean();
            return new RemoveFromCMS(addr, force);
        }

        public long serializedSize(Transformation t, Version version)
        {
            RemoveFromCMS remove = (RemoveFromCMS)t;
            return InetAddressAndPort.MetadataSerializer.serializer.serializedSize(remove.endpoint, version) +
                   TypeSizes.sizeof(remove.force);
        }
    }
}
