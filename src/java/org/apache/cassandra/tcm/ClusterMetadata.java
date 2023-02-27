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
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.locator.EndpointsForRange;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.tcm.extensions.ExtensionKey;
import org.apache.cassandra.tcm.extensions.ExtensionValue;
import org.apache.cassandra.tcm.serialization.MetadataSerializer;
import org.apache.cassandra.tcm.serialization.Version;
import org.apache.cassandra.tcm.transformations.cms.EntireRange;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.vint.VIntCoding;

import static org.apache.cassandra.db.TypeSizes.sizeof;
import static org.apache.cassandra.tcm.transformations.cms.EntireRange.entireRange;

public class ClusterMetadata
{
    public static final Serializer serializer = new Serializer();

    public final Epoch epoch;
    public final long period;
    public final boolean lastInPeriod;
    public final IPartitioner partitioner;       // Set during (initial) construction and not modifiable via Transformer
    public final ImmutableMap<ExtensionKey<?,?>, ExtensionValue<?>> extensions;

    public final EndpointsForRange cmsReplicas;
    public final ImmutableSet<InetAddressAndPort> cmsMembers;

    public ClusterMetadata(IPartitioner partitioner)
    {
        this(Epoch.EMPTY,
             Period.EMPTY,
             true,
             partitioner,
             ImmutableSet.of(),
             ImmutableMap.of());
    }

    public ClusterMetadata(Epoch epoch,
                           long period,
                           boolean lastInPeriod,
                           IPartitioner partitioner,
                           Set<InetAddressAndPort> cmsMembers,
                           Map<ExtensionKey<?, ?>, ExtensionValue<?>> extensions)
    {
        this.epoch = epoch;
        this.period = period;
        this.lastInPeriod = lastInPeriod;
        this.partitioner = partitioner;
        this.cmsMembers = ImmutableSet.copyOf(cmsMembers);
        this.extensions = ImmutableMap.copyOf(extensions);

        this.cmsReplicas = EndpointsForRange.builder(entireRange)
                                            .addAll(cmsMembers.stream()
                                                              .map(EntireRange::replica)
                                                              .collect(Collectors.toList()))
                                            .build();
    }

    public boolean isCMSMember(InetAddressAndPort endpoint)
    {
        return cmsMembers.contains(endpoint);
    }

    public Set<InetAddressAndPort> cmsMembers()
    {
        return cmsMembers;
    }

    public Transformer transformer()
    {
        return new Transformer(this, this.nextEpoch(), false);
    }

    public Transformer transformer(boolean sealPeriod)
    {
        return new Transformer(this, this.nextEpoch(), sealPeriod);
    }

    public Epoch nextEpoch()
    {
        return epoch.nextEpoch();
    }

    public long nextPeriod()
    {
        return lastInPeriod ? period + 1 : period;
    }

    public static class Transformer
    {
        private final ClusterMetadata base;
        private final Epoch epoch;
        private final long period;
        private final boolean lastInPeriod;
        private final IPartitioner partitioner;
        private final Set<InetAddressAndPort> cmsMembers;
        private final Map<ExtensionKey<?, ?>, ExtensionValue<?>> extensions;
        private final Set<MetadataKey> modifiedKeys;

        private Transformer(ClusterMetadata metadata, Epoch epoch, boolean lastInPeriod)
        {
            this.base = metadata;
            this.epoch = epoch;
            this.period = metadata.nextPeriod();
            this.lastInPeriod = lastInPeriod;
            this.partitioner = metadata.partitioner;
            this.cmsMembers = new HashSet<>(metadata.cmsMembers);
            extensions = new HashMap<>(metadata.extensions);
            modifiedKeys = new HashSet<>();
        }

        public Transformer withCMSMember(InetAddressAndPort member)
        {
            cmsMembers.add(member);
            return this;
        }

        public Transformer withoutCMSMember(InetAddressAndPort member)
        {
            cmsMembers.remove(member);
            return this;
        }

        public Transformer with(ExtensionKey<?, ?> key, ExtensionValue<?> obj)
        {
            if (MetadataKeys.CORE_METADATA.contains(key))
                throw new IllegalArgumentException("Core cluster metadata objects should be addressed directly, " +
                                                   "not using the associated MetadataKey");

            if (!key.valueType.isInstance(obj))
                throw new IllegalArgumentException("Value of type " + obj.getClass() +
                                                   " is incompatible with type for key " + key +
                                                   " (" + key.valueType + ")");

            extensions.put(key, obj);
            modifiedKeys.add(key);
            return this;
        }

        public Transformer withIfAbsent(ExtensionKey<?, ?> key, ExtensionValue<?> obj)
        {
            if (extensions.containsKey(key))
                return this;
            return with(key, obj);
        }

        public Transformer without(ExtensionKey<?, ?> key)
        {
            if (MetadataKeys.CORE_METADATA.contains(key))
                throw new IllegalArgumentException("Core cluster metadata objects should be addressed directly, " +
                                                   "not using the associated MetadataKey");
            if (extensions.remove(key) != null)
                modifiedKeys.add(key);
            return this;
        }

        public Transformed build()
        {
            // Process extension first as a) these are actually mutable and b) they are added to the set of
            // modified keys when added/updated/removed
            for (MetadataKey key : modifiedKeys)
            {
                ExtensionValue<?> mutable = extensions.get(key);
                if (null != mutable)
                    mutable.withLastModified(epoch);
            }

            return new Transformed(new ClusterMetadata(epoch,
                                                       period,
                                                       lastInPeriod,
                                                       partitioner,
                                                       cmsMembers,
                                                       extensions),
                                   ImmutableSet.copyOf(modifiedKeys));
        }

        @Override
        public String toString()
        {
            return "Transformer{" +
                   "baseEpoch=" + base.epoch +
                   ", epoch=" + epoch +
                   ", lastInPeriod=" + lastInPeriod +
                   ", partitioner=" + partitioner +
                   ", extensions=" + extensions +
                   ", cmsMembers=" + cmsMembers +
                   ", modifiedKeys=" + modifiedKeys +
                   '}';
        }

        public static class Transformed
        {
            public final ClusterMetadata metadata;
            public final ImmutableSet<MetadataKey> modifiedKeys;

            public Transformed(ClusterMetadata metadata, ImmutableSet<MetadataKey> modifiedKeys)
            {
                this.metadata = metadata;
                this.modifiedKeys = modifiedKeys;
            }
        }
    }

    @Override
    public String toString()
    {
        return "ClusterMetadata{" +
               "epoch=" + epoch +
               '}';
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (!(o instanceof ClusterMetadata)) return false;
        ClusterMetadata that = (ClusterMetadata) o;
        return epoch.equals(that.epoch) &&
               lastInPeriod == that.lastInPeriod &&
               extensions.equals(that.extensions);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(epoch, lastInPeriod, extensions);
    }

    public static ClusterMetadata current()
    {
        return ClusterMetadataService.instance().metadata();
    }

    /**
     * Startup of some services may race with cluster metadata initialization. We allow those services to
     * gracefully handle scenarios when it is not yet initialized.
     */
    public static ClusterMetadata currentNullable()
    {
        ClusterMetadataService service = ClusterMetadataService.instance();
        if (service == null)
            return null;
        return service.metadata();
    }

    public static class Serializer implements MetadataSerializer<ClusterMetadata>
    {
        @Override
        public void serialize(ClusterMetadata metadata, DataOutputPlus out, Version version) throws IOException
        {
            Epoch.serializer.serialize(metadata.epoch, out);
            out.writeUnsignedVInt(metadata.period);
            out.writeBoolean(metadata.lastInPeriod);
            out.writeUTF(metadata.partitioner.getClass().getCanonicalName());
            out.writeInt(metadata.extensions.size());
            for (Map.Entry<ExtensionKey<?, ?>, ExtensionValue<?>> entry : metadata.extensions.entrySet())
            {
                ExtensionKey<?, ?> key = entry.getKey();
                ExtensionValue<?> value = entry.getValue();
                ExtensionKey.serializer.serialize(key, out, version);
                assert key.valueType.isInstance(value);
                value.serialize(out, version);
            }
            out.writeInt(metadata.cmsMembers.size());
            for (InetAddressAndPort member : metadata.cmsMembers)
                InetAddressAndPort.MetadataSerializer.serializer.serialize(member, out, version);
        }

        @Override
        public ClusterMetadata deserialize(DataInputPlus in, Version version) throws IOException
        {
            Epoch epoch = Epoch.serializer.deserialize(in);
            long period = in.readUnsignedVInt();
            boolean lastInPeriod = in.readBoolean();
            IPartitioner partitioner = FBUtilities.newPartitioner(in.readUTF());
            int items = in.readInt();
            Map<ExtensionKey<?, ?>, ExtensionValue<?>> extensions = new HashMap<>(items);
            for (int i = 0; i < items; i++)
            {
                ExtensionKey<?, ?> key = ExtensionKey.serializer.deserialize(in, version);
                ExtensionValue<?> value = key.newValue();
                value.deserialize(in, version);
                extensions.put(key, value);
            }
            int memberCount = in.readInt();
            Set<InetAddressAndPort> members = new HashSet<>(memberCount);
            for (int i = 0; i < memberCount; i++)
                members.add(InetAddressAndPort.MetadataSerializer.serializer.deserialize(in, version));
            return new ClusterMetadata(epoch,
                                       period,
                                       lastInPeriod,
                                       partitioner,
                                       members,
                                       extensions);
        }

        @Override
        public long serializedSize(ClusterMetadata metadata, Version version)
        {
            long size = TypeSizes.INT_SIZE;
            for (Map.Entry<ExtensionKey<?, ?>, ExtensionValue<?>> entry : metadata.extensions.entrySet())
                size += ExtensionKey.serializer.serializedSize(entry.getKey(), version) +
                        entry.getValue().serializedSize(version);

            size += Epoch.serializer.serializedSize(metadata.epoch) +
                    VIntCoding.computeUnsignedVIntSize(metadata.period) +
                    TypeSizes.BOOL_SIZE +
                    sizeof(metadata.partitioner.getClass().getCanonicalName());

            size += TypeSizes.INT_SIZE;
            for (InetAddressAndPort member : metadata.cmsMembers)
                size += InetAddressAndPort.MetadataSerializer.serializer.serializedSize(member, version);

            return size;
        }
    }
}
