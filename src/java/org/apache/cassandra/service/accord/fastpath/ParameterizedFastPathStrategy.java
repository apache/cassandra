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

package org.apache.cassandra.service.accord.fastpath;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import accord.api.VisibleForImplementation;
import accord.local.Node;
import accord.topology.Shard;
import accord.utils.Invariants;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.tcm.serialization.MetadataSerializer;
import org.apache.cassandra.tcm.serialization.Version;

import javax.annotation.Nonnull;

public class ParameterizedFastPathStrategy implements FastPathStrategy
{
    static final String SIZE = "size";
    static final String DCS = "dcs";
    private static final Joiner DC_JOINER = Joiner.on(',');
    private static final Pattern COMMA_SEPARATOR = Pattern.compile(",");
    private static final Pattern COLON_SEPARATOR = Pattern.compile(":");

    static class WeightedDc implements Comparable<WeightedDc>
    {
        private static final WeightedDc UNSPECIFIED = new WeightedDc("<unspecified>", Integer.MAX_VALUE, true);
        private static final MetadataSerializer<WeightedDc> serializer = new MetadataSerializer<WeightedDc>()
        {
            public void serialize(WeightedDc dc, DataOutputPlus out, Version version) throws IOException
            {
                out.writeUTF(dc.name);
                out.writeUnsignedVInt(dc.weight);
                out.writeBoolean(dc.autoWeight);
            }

            public WeightedDc deserialize(DataInputPlus in, Version version) throws IOException
            {
                return new WeightedDc(in.readUTF(),
                                      in.readUnsignedVInt32(),
                                      in.readBoolean());
            }

            public long serializedSize(WeightedDc dc, Version version)
            {
                return TypeSizes.sizeof(dc.name) + TypeSizes.sizeofUnsignedVInt(dc.weight) + TypeSizes.BOOL_SIZE;
            }
        };

        final String name;
        final int weight;
        final boolean autoWeight;

        public WeightedDc(String name, int weight, boolean autoWeight)
        {
            this.name = name;
            this.weight = weight;
            this.autoWeight = autoWeight;
        }

        public boolean equals(Object o)
        {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            WeightedDc that = (WeightedDc) o;
            return weight == that.weight && autoWeight == that.autoWeight && Objects.equals(name, that.name);
        }

        public int hashCode()
        {
            return Objects.hash(name, weight, autoWeight);
        }

        @Override
        public int compareTo(@Nonnull WeightedDc that)
        {
            int cmp = Integer.compare(this.weight, that.weight);
            if (cmp != 0) return cmp;
            return this.name.compareTo(that.name);
        }

        public String toString()
        {
            return autoWeight ? name : name + ':' + weight;
        }

        static String validateDC(String dc)
        {
            dc = dc.trim();
            if (dc.isEmpty())
                throw cfe("dc name must not be empty", DCS);
            return dc;
        }

        static int validateWeight(String w)
        {
            int weight = Integer.parseInt(w);
            if (weight < 0)
                throw cfe("DC weights must be zero or positive");
            return weight;
        }

        static WeightedDc fromString(String s, int idx)
        {
            s = s.trim();
            if (s.isEmpty())
                throw cfe("%s entries must not be empty", DCS);

            String[] parts = COLON_SEPARATOR.split(s);
            if (parts.length == 1)
                return new WeightedDc(validateDC(parts[0]), idx, true);
            else if (parts.length == 2)
                return new WeightedDc(validateDC(parts[0]), validateWeight(parts[1]), false);
            else
                throw cfe("Invalid dc weighting syntax %s, use <dc>:<weight>");
        }
    }

    public final int size;
    private final ImmutableMap<String, WeightedDc> dcs;

    ParameterizedFastPathStrategy(int size, ImmutableMap<String, WeightedDc> dcs)
    {
        this.size = size;
        this.dcs = dcs;
    }

    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ParameterizedFastPathStrategy that = (ParameterizedFastPathStrategy) o;
        return size == that.size && Objects.equals(dcs, that.dcs);
    }

    public int hashCode()
    {
        return Objects.hash(size, dcs);
    }

    private static class NodeSorter implements Comparable<NodeSorter>
    {
        private final Node.Id id;
        private final int sortPos;
        private final int dcIndex;
        private final int health;

        public NodeSorter(Node.Id id, int sortPos, int dcIndex, int health)
        {
            this.id = id;
            this.sortPos = sortPos;
            this.dcIndex = dcIndex;
            this.health = health;
        }

        @Override
        public int compareTo(@Nonnull NodeSorter that)
        {
            int cmp = this.health - that.health;
            if (cmp != 0) return cmp;

            cmp = this.dcIndex - that.dcIndex;
            if (cmp != 0) return cmp;

            cmp = this.sortPos - that.sortPos;
            if (cmp != 0) return cmp;

            Invariants.checkState(this.id.equals(that.id));
            return 0;
        }
    }

    @Override
    public Set<Node.Id> calculateFastPath(List<Node.Id> nodes, Set<Node.Id> unavailable, Map<Node.Id, String> dcMap)
    {
        List<NodeSorter> sorters = new ArrayList<>(nodes.size());

        for (int i = 0, mi = nodes.size(); i < mi; i++)
        {
            Node.Id node = nodes.get(i);
            String dc = dcMap.get(node);
            int dcScore = dcs.getOrDefault(dc, WeightedDc.UNSPECIFIED).weight;
            NodeSorter sorter = new NodeSorter(node, i, dcScore, unavailable.contains(node) ? 1 : 0);
            sorters.add(sorter);
        }

        sorters.sort(Comparator.naturalOrder());

        int slowQuorum = Shard.slowPathQuorumSize(nodes.size());
        int fpSize = Math.max(size, slowQuorum);
        ImmutableSet.Builder<Node.Id> builder = ImmutableSet.builder();

        for (int i=0; i<fpSize; i++)
            builder.add(sorters.get(i).id);

        ImmutableSet<Node.Id> fastPath = builder.build();
        Invariants.checkState(fastPath.size() >= slowQuorum);
        return fastPath;
    }

    private static ConfigurationException cfe(String fmt, Object... args)
    {
        return new ConfigurationException(String.format(fmt, args));
    }

    static ParameterizedFastPathStrategy fromMap(Map<String, String> map)
    {
        if (!map.containsKey(SIZE))
            throw cfe("fast_path must be set to 'keyspace' or 'default' or a map defining '%s' and optionally '%s'", SIZE, DCS);

        int size;
        try
        {
            size = Integer.parseInt(map.get(SIZE));
        }
        catch (NumberFormatException e)
        {
            throw cfe("%s must be a positive number, got %s", SIZE, map.get(SIZE));
        }

        if (size < 1)
            throw cfe("%s must be greater than zero", SIZE);

        ImmutableMap<String, WeightedDc> dcMap;
        if (map.containsKey(DCS))
        {

            Map<String, WeightedDc> mutableDcs = new HashMap<>();
            String dcsString = map.get(DCS);
            if (dcsString.trim().isEmpty())
                throw cfe("%s must specify at least one DC", DCS);

            int autoIdx = 0;
            boolean hasAuto = false;
            boolean hasManual = false;
            for (String dcString : COMMA_SEPARATOR.split(dcsString))
            {
                WeightedDc dc = WeightedDc.fromString(dcString, autoIdx++);
                if (mutableDcs.containsKey(dc.name))
                    throw cfe("Multiple entries for DC %s", dc.name);

                if (dc.autoWeight)
                {
                    if (hasManual) throw cfe("Cannot mix auto and manual DC weights");
                    hasAuto = true;
                }
                else
                {
                    if (hasAuto) throw cfe("Cannot mix auto and manual DC weights");
                    hasManual = true;
                }

                mutableDcs.put(dc.name, dc);
            }
            dcMap = ImmutableMap.copyOf(mutableDcs);
        }
        else
        {
            dcMap = ImmutableMap.of();
        }

        Set<String> keys = new HashSet<>(map.keySet());
        keys.remove(SIZE);
        keys.remove(DCS);
        if (!keys.isEmpty())
            throw cfe("Unrecognized fast path options provided: ", keys);

        return new ParameterizedFastPathStrategy(size, dcMap);
    }

    @Override
    public Kind kind()
    {
        return Kind.PARAMETERIZED;
    }

    @VisibleForImplementation
    public Iterable<String> dcStrings()
    {
        return dcs.values().stream().sorted().map(Object::toString).collect(Collectors.toList());
    }

    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder("{");
        sb.append(SIZE).append(':').append(size);
        if (!dcs.isEmpty())
            sb.append(", ").append(DCS).append(':').append('\'').append(DC_JOINER.join(dcStrings())).append('\'');

        return sb.append('}').toString();
    }

    public Map<String, String> asMap()
    {
        Map<String, String> params = new HashMap<>();
        params.put(Kind.KEY, kind().name());
        params.put(SIZE, Integer.toString(size));
        params.put(DCS, DC_JOINER.join(dcStrings()));
        return params;
    }

    @Override
    public String asCQL()
    {
        return toString();
    }

    public static final MetadataSerializer<ParameterizedFastPathStrategy> serializer = new MetadataSerializer<ParameterizedFastPathStrategy>()
    {
        public void serialize(ParameterizedFastPathStrategy strategy, DataOutputPlus out, Version version) throws IOException
        {
            out.writeUnsignedVInt32(strategy.size);
            out.writeUnsignedVInt32(strategy.dcs.size());
            for (WeightedDc dc : strategy.dcs.values())
                WeightedDc.serializer.serialize(dc, out, version);
        }

        public ParameterizedFastPathStrategy deserialize(DataInputPlus in, Version version) throws IOException
        {
            int size = in.readUnsignedVInt32();
            int numDCs = in.readUnsignedVInt32();
            ImmutableMap.Builder<String, WeightedDc> builder = ImmutableMap.builder();
            for (int i=0; i<numDCs; i++)
            {
                WeightedDc dc = WeightedDc.serializer.deserialize(in, version);
                builder.put(dc.name, dc);
            }

            return new ParameterizedFastPathStrategy(size, builder.build());
        }

        public long serializedSize(ParameterizedFastPathStrategy strategy, Version version)
        {
            long size = TypeSizes.sizeofUnsignedVInt(strategy.size) + TypeSizes.sizeofUnsignedVInt(strategy.dcs.size());
            for (WeightedDc dc : strategy.dcs.values())
                size += WeightedDc.serializer.serializedSize(dc, version);

            return size;
        }
    };
}
