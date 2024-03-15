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

package org.apache.cassandra.harry.sut;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.distributed.api.TokenSupplier;
import org.apache.cassandra.harry.gen.rng.PCGFastPure;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.locator.SimpleStrategy;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.ReplicationParams;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.membership.Location;
import org.apache.cassandra.tcm.membership.NodeId;

public class TokenPlacementModel
{
    public abstract static class ReplicationFactor
    {
        protected final Lookup lookup;
        protected final int nodesTotal;
        protected final Map<String, DCReplicas> replication;

        public ReplicationFactor(Function<Lookup, Map<String, DCReplicas>> perDcMapProvider)
        {
            this.lookup = new DefaultLookup();
            this.replication = perDcMapProvider.apply(lookup);
            this.nodesTotal = replication.values().stream().map(r -> r.totalCount).reduce(0, Integer::sum);
        }

        public int total()
        {
            return nodesTotal;
        }

        public int dcs()
        {
            return replication.size();
        };

        public abstract KeyspaceParams asKeyspaceParams();

        public Map<String, DCReplicas> asMap()
        {
            return replication;
        }

        public ReplicatedRanges replicate(List<Node> nodes)
        {
            return replicate(toRanges(nodes), nodes);
        }

        public abstract ReplicatedRanges replicate(Range[] ranges, List<Node> nodes);
    }

    public static class DCReplicas
    {
        public final int totalCount;
        public final int transientCount;
        public DCReplicas(int totalCount, int transientCount)
        {
            this.totalCount = totalCount;
            this.transientCount = transientCount;
        }

        public String toString()
        {
            return totalCount + ((transientCount > 0) ? "/" + transientCount : "");
        }
    }

    public static class ReplicatedRanges
    {
        public final Range[] ranges;
        public final NavigableMap<Range, List<Replica>> placementsForRange;

        public ReplicatedRanges(Range[] ranges, NavigableMap<Range, List<Replica>> placementsForRange)
        {
            this.ranges = ranges;
            this.placementsForRange = placementsForRange;
        }

        public List<Replica> replicasFor(long token)
        {
            int idx = indexedBinarySearch(ranges, range -> {
                // exclusive start, so token at the start belongs to a lower range
                if (token <= range.start)
                    return 1;
                // ie token > start && token <= end
                if (token <= range.end || range.end == Long.MIN_VALUE)
                    return 0;

                return -1;
            });
            assert idx >= 0 : String.format("Somehow ranges %s do not contain token %d", Arrays.toString(ranges), token);
            return placementsForRange.get(ranges[idx]);
        }

        public NavigableMap<Range, List<Replica>> asMap()
        {
            return placementsForRange;
        }

        private static <T> int indexedBinarySearch(T[] arr, CompareTo<T> comparator)
        {
            int low = 0;
            int high = arr.length - 1;

            while (low <= high)
            {
                int mid = (low + high) >>> 1;
                T midEl = arr[mid];
                int cmp = comparator.compareTo(midEl);

                if (cmp < 0)
                    low = mid + 1;
                else if (cmp > 0)
                    high = mid - 1;
                else
                    return mid;
            }
            return -(low + 1); // key not found
        }
    }

    public interface CompareTo<V>
    {
        int compareTo(V v);
    }

    public static void addIfUnique(List<Replica> replicas, Set<Integer> names, Replica replica)
    {
        if (names.contains(replica.node().idx()))
            return;
        replicas.add(replica);
        names.add(replica.node().idx());
    }

    /**
     * Finds a primary replica
     */
    public static int primaryReplica(List<Node> nodes, Range range)
    {
        for (int i = 0; i < nodes.size(); i++)
        {
            long token = nodes.get(i).token();
            if (token == Long.MIN_VALUE)
            {
                if (range.end == token)
                    return i;
            }
            else if (range.end != Long.MIN_VALUE && token >= range.end)
                return i;
        }
        return -1;
    }


    /**
     * Generates token ranges from the list of nodes
     */
    public static Range[] toRanges(List<Node> nodes)
    {
        boolean hasMinToken = nodes.get(0).token() == Long.MIN_VALUE;
        List<Long> tokens = new ArrayList<>();
        for (Node node : nodes)
            tokens.add(node.token());
        if (!hasMinToken)
            tokens.add(Long.MIN_VALUE);
        tokens.sort(Long::compareTo);

        Range[] ranges = new Range[nodes.size() + (hasMinToken ? 0 : 1)];
        long prev = tokens.get(0);
        int cnt = 0;
        for (int i = 1; i < tokens.size(); i++)
        {
            long current = tokens.get(i);
            ranges[cnt++] = new Range(prev, current);
            prev = current;
        }
        ranges[ranges.length - 1] = new Range(prev, Long.MIN_VALUE);
        return ranges;

    }

    public static List<Node> peerStateToNodes(Object[][] resultset)
    {
        List<Node> nodes = new ArrayList<>();
        for (Object[] row : resultset)
        {
            InetAddress address = (InetAddress) row[0];
            Set<String> tokens = (Set<String>) row[1];
            String dc = (String) row[2];
            String rack = (String) row[3];
            for (String token : tokens)
            {
                nodes.add(new Node(0, 0, 0, 0,
                                   constantLookup(address.toString(),
                                                  Long.parseLong(token),
                                                  dc,
                                                  rack)));
            }
        }
        return nodes;
    }

    public static class NtsReplicationFactor extends ReplicationFactor
    {
        private KeyspaceParams keyspaceParams;

        public NtsReplicationFactor(int... nodesPerDc)
        {
            super(mapFunction(nodesPerDc));
        }

        public NtsReplicationFactor(int dcs, int nodesPerDc)
        {
            this(dcs, nodesPerDc, 0);
        }

        public NtsReplicationFactor(int dcs, int nodesPerDc, int transientPerDc)
        {
            super(mapFunction(dcs, nodesPerDc, transientPerDc));
            if (transientPerDc >= nodesPerDc)
                throw new IllegalArgumentException("Transient replicas must be zero, or less than total replication factor per dc");
        }

        public NtsReplicationFactor(Map<String, Integer> m)
        {
            super((lookup) -> m.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, e -> new DCReplicas(e.getValue(), 0))));
        }

        public KeyspaceParams asKeyspaceParams()
        {
            if (this.keyspaceParams == null)
                this.keyspaceParams = toKeyspaceParams();
            return this.keyspaceParams;
        }

        public ReplicatedRanges replicate(Range[] ranges, List<Node> nodes)
        {
            return replicate(ranges, nodes, asMap());
        }

        public static ReplicatedRanges replicate(Range[] ranges, List<Node> nodes, Map<String, DCReplicas> rfs)
        {
            assertStrictlySorted(nodes);
            boolean minTokenOwned = nodes.stream().anyMatch(n -> n.token() == Long.MIN_VALUE);
            Map<String, DatacenterNodes> template = new HashMap<>();

            Map<String, List<Node>> nodesByDC = nodesByDC(nodes);
            Map<String, Set<String>> racksByDC = racksByDC(nodes);

            for (Map.Entry<String, DCReplicas> entry : rfs.entrySet())
            {
                String dc = entry.getKey();
                DCReplicas dcRf = entry.getValue();
                List<Node> nodesInThisDC = nodesByDC.get(dc);
                Set<String> racksInThisDC = racksByDC.get(dc);
                int nodeCount = nodesInThisDC == null ? 0 : nodesInThisDC.size();
                int rackCount = racksInThisDC == null ? 0 : racksInThisDC.size();
                if (dcRf.totalCount <= 0 || nodeCount == 0)
                    continue;

                template.put(dc, new DatacenterNodes(dcRf, rackCount, nodeCount));
            }

            NavigableMap<Range, Map<String, List<Replica>>> replication = new TreeMap<>();
            Range skipped = null;
            for (Range range : ranges)
            {
                final int idx = primaryReplica(nodes, range);
                int cnt = 0;
                if (idx >= 0)
                {
                    int dcsToFill = template.size();

                    Map<String, DatacenterNodes> nodesInDCs = new HashMap<>();
                    for (Map.Entry<String, DatacenterNodes> e : template.entrySet())
                        nodesInDCs.put(e.getKey(), e.getValue().copy());

                    while (dcsToFill > 0 && cnt < nodes.size())
                    {
                        Node node = nodes.get((idx + cnt) % nodes.size());
                        DatacenterNodes dcNodes = nodesInDCs.get(node.dc());
                        if (dcNodes != null && dcNodes.addAndCheckIfDone(node, new Location(node.dc(), node.rack())))
                            dcsToFill--;

                        cnt++;
                    }

                    replication.put(range, mapValues(nodesInDCs, DatacenterNodes::asReplicas));
                }
                else
                {
                    if (minTokenOwned)
                        skipped = range;
                    else
                        // if the range end is larger than the highest assigned token, then treat it
                        // as part of the wraparound and replicate it to the same nodes as the first
                        // range. This is most likely caused by a decommission removing the node with
                        // the largest token.
                        replication.put(range, replication.get(ranges[0]));
                }
            }

            // Since we allow owning MIN_TOKEN, when it is owned, we have to replicate the range explicitly.
            if (skipped != null)
                replication.put(skipped, replication.get(ranges[ranges.length - 1]));

            return combine(replication);
        }

        /**
         * Replicate ranges to rf nodes.
         */
        private static ReplicatedRanges combine(NavigableMap<Range, Map<String, List<Replica>>> orig)
        {

            Range[] ranges = new Range[orig.size()];
            int idx = 0;
            NavigableMap<Range, List<Replica>> flattened = new TreeMap<>();
            for (Map.Entry<Range, Map<String, List<Replica>>> e : orig.entrySet())
            {
                List<Replica> placementsForRange = new ArrayList<>();
                for (List<Replica> v : e.getValue().values())
                    placementsForRange.addAll(v);
                ranges[idx++] = e.getKey();
                flattened.put(e.getKey(), placementsForRange);
            }
            return new ReplicatedRanges(ranges, flattened);
        }

        private KeyspaceParams toKeyspaceParams()
        {
            Object[] args = new Object[replication.size() * 2];
            int i = 0;
            for (Map.Entry<String, DCReplicas> e : replication.entrySet())
            {
                args[i * 2] = e.getKey();
                args[i * 2 + 1] = e.getValue().toString();
                i++;
            }

            return KeyspaceParams.nts(args);
        }

        private static Function<Lookup, Map<String, DCReplicas>> mapFunction(int dcs, int nodesPerDc, int transientsPerDc)
        {
            return (lookup) -> {
                int[] totals = new int[dcs];
                Arrays.fill(totals, nodesPerDc);
                int[] transients = new int[dcs];
                Arrays.fill(transients, transientsPerDc);
                return toMap(totals, transients, lookup);
            };
        }

        private static Function<Lookup, Map<String, DCReplicas>> mapFunction(final int[] totalPerDc)
        {
            return (lookup) -> {
                int[] transients = new int[totalPerDc.length];
                Arrays.fill(transients, 0);
                return toMap(totalPerDc, transients, lookup);
            };
        }

        private static Map<String, DCReplicas> toMap(int[] totalPerDc, int[] transientPerDc, Lookup lookup)
        {
            Map<String, DCReplicas> map = new TreeMap<>();
            for (int i = 0; i < totalPerDc.length; i++) {
                map.put(lookup.dc(i + 1), new DCReplicas(totalPerDc[i], transientPerDc[i]));
            }
            return map;
        }

        public String toString()
        {
            return "NtsReplicationFactor{" +
                   "map=" + asMap() +
                   '}';
        }
    }

    private static final class DatacenterNodes
    {
        private final List<Node> nodes = new ArrayList<>();
        private final Set<Location> racks = new HashSet<>();

        /** Number of replicas left to fill from this DC. */
        int rfLeft;
        int acceptableRackRepeats;
        DCReplicas rf;

        public DatacenterNodes copy()
        {
            return new DatacenterNodes(this);
        }

        DatacenterNodes(DCReplicas rf, int rackCount, int nodeCount)
        {
            this.rf = rf;
            this.rfLeft = Math.min(rf.totalCount, nodeCount);
            this.acceptableRackRepeats = rf.totalCount - rackCount;
        }

        // for copying
        DatacenterNodes(DatacenterNodes source)
        {
            this.rf = source.rf;
            this.rfLeft = source.rfLeft;
            this.acceptableRackRepeats = source.acceptableRackRepeats;
        }

        boolean addAndCheckIfDone(Node node, Location location)
        {
            if (done())
                return false;

            if (nodes.contains(node))
                // Cannot repeat a node.
                return false;

            if (racks.add(location))
            {
                // New rack.
                --rfLeft;
                nodes.add(node);
                return done();
            }
            if (acceptableRackRepeats <= 0)
                // There must be rfLeft distinct racks left, do not add any more rack repeats.
                return false;

            nodes.add(node);

            // Added a node that is from an already met rack to match RF when there aren't enough racks.
            --acceptableRackRepeats;
            --rfLeft;
            return done();
        }

        boolean done()
        {
            assert rfLeft >= 0;
            return rfLeft == 0;
        }

        public List<Replica> asReplicas()
        {
            List<Replica> replicas = new ArrayList<>(nodes.size());
            for (Node node : nodes)
            {
                boolean full = replicas.size() < rf.totalCount - rf.transientCount;
                replicas.add(new Replica(node, full));
            }
            return replicas;
        }

        @Override
        public String toString()
        {
            return "DatacenterNodes{" +
                   "nodes=" + nodes +
                   ", racks=" + racks +
                   ", rfLeft=" + rfLeft +
                   ", acceptableRackRepeats=" + acceptableRackRepeats +
                   '}';
        }
    }

    private static <T extends Comparable<T>> void assertStrictlySorted(Collection<T> coll)
    {
        if (coll.size() <= 1) return;

        Iterator<T> iter = coll.iterator();
        T prev = iter.next();
        while (iter.hasNext())
        {
            T next = iter.next();
            assert next.compareTo(prev) > 0 : String.format("Collection does not seem to be sorted. %s and %s are in wrong order", prev, next);
            prev = next;
        }
    }

    private static <K extends Comparable<K>, T1, T2> Map<K, T2> mapValues(Map<K, T1> allDCs, Function<T1, T2> map)
    {
        NavigableMap<K, T2> res = new TreeMap<>();
        for (Map.Entry<K, T1> e : allDCs.entrySet())
        {
            res.put(e.getKey(), map.apply(e.getValue()));
        }
        return res;
    }

    public static Map<String, List<Node>> nodesByDC(List<Node> nodes)
    {
        Map<String, List<Node>> nodesByDC = new HashMap<>();
        for (Node node : nodes)
            nodesByDC.computeIfAbsent(node.dc(), (k) -> new ArrayList<>()).add(node);

        return nodesByDC;
    }

    public static Map<String, Integer> dcLayout(List<Node> nodes)
    {
        Map<String, List<Node>> nodesByDC = nodesByDC(nodes);
        Map<String, Integer> layout = new HashMap<>();
        for (Map.Entry<String, List<Node>> e : nodesByDC.entrySet())
            layout.put(e.getKey(), e.getValue().size());

        return layout;
    }

    public static Map<String, Set<String>> racksByDC(List<Node> nodes)
    {
        Map<String, Set<String>> racksByDC = new HashMap<>();
        for (Node node : nodes)
            racksByDC.computeIfAbsent(node.dc(), (k) -> new HashSet<>()).add(node.rack());

        return racksByDC;
    }


    public static class SimpleReplicationFactor extends ReplicationFactor
    {
        public SimpleReplicationFactor(int total)
        {
            this(total, 0);
        }

        public SimpleReplicationFactor(int total, int transientReplicas)
        {
            super(mapFunction(total, transientReplicas));
            if (transientReplicas >= total)
                throw new IllegalArgumentException("Transient replicas must be zero, or less than total replication factor");
        }

        public static Function<Lookup, Map<String, DCReplicas>> mapFunction(int totalReplicas, int transientReplicas)
        {
            return (lookup) -> Collections.singletonMap(lookup.dc(1), new DCReplicas(totalReplicas, transientReplicas));
        }

        private DCReplicas dcReplicas()
        {
            return replication.get(lookup.dc(1));
        }

        public KeyspaceParams asKeyspaceParams()
        {
            Map<String, String> options = new HashMap<>();
            options.put(ReplicationParams.CLASS, SimpleStrategy.class.getName());
            options.put(SimpleStrategy.REPLICATION_FACTOR, dcReplicas().toString());
            return KeyspaceParams.create(true, options);
        }

        public ReplicatedRanges replicate(Range[] ranges, List<Node> nodes)
        {
            return replicate(ranges, nodes, dcReplicas());
        }

        public static ReplicatedRanges replicate(Range[] ranges, List<Node> nodes, DCReplicas dcReplicas)
        {
            assertStrictlySorted(nodes);
            NavigableMap<Range, List<Replica>> replication = new TreeMap<>();
            boolean minTokenOwned = nodes.stream().anyMatch(n -> n.token() == Long.MIN_VALUE);
            Range skipped = null;
            for (Range range : ranges)
            {
                Set<Integer> names = new HashSet<>();
                List<Replica> replicas = new ArrayList<>();
                int idx = primaryReplica(nodes, range);
                if (idx >= 0)
                {
                    for (int i = 0; i < nodes.size() && replicas.size() < dcReplicas.totalCount; i++)
                    {
                        boolean full = replicas.size() < dcReplicas.totalCount - dcReplicas.transientCount;
                        addIfUnique(replicas, names, new Replica(nodes.get((idx + i) % nodes.size()), full));
                    }
                    if (!minTokenOwned && range.start == Long.MIN_VALUE)
                        replication.put(ranges[ranges.length - 1], replicas);
                    replication.put(range, replicas);
                }
                else
                {
                    if (minTokenOwned)
                        skipped = range;
                    else
                        // if the range end is larger than the highest assigned token, then treat it
                        // as part of the wraparound and replicate it to the same nodes as the first
                        // range. This is most likely caused by a decommission removing the node with
                        // the largest token.
                        replication.put(range, replication.get(ranges[0]));
                }
            }

            // Since we allow owning MIN_TOKEN, when it is owned, we have to replicate the range explicitly.
            if (skipped != null)
                replication.put(skipped, replication.get(ranges[ranges.length - 1]));

            return new ReplicatedRanges(ranges, Collections.unmodifiableNavigableMap(replication));
        }

        public String toString()
        {
            return "SimpleReplicationFactor{" +
                   "rf=" + dcReplicas().toString() +
                   '}';
        }
    }

    /**
     * A Range is responsible for the tokens between (start, end].
     */
    public static class Range implements Comparable<Range>
    {
        public final long start;
        public final long end;

        public Range(long start, long end)
        {
            assert end > start || end == Long.MIN_VALUE : String.format("Start (%d) should be smaller than end (%d)", start, end);
            this.start = start;
            this.end = end;
        }

        public boolean contains(long min, long max)
        {
            assert max > min;
            return min > start && (max <= end || end == Long.MIN_VALUE);
        }

        public boolean contains(long token)
        {
            return token > start && (token <= end || end == Long.MIN_VALUE);
        }

        public int compareTo(Range o)
        {
            int res = Long.compare(start, o.start);
            if (res == 0)
                return Long.compare(end, o.end);
            return res;
        }

        public boolean equals(Object o)
        {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Range range = (Range) o;
            return start == range.start && end == range.end;
        }

        public int hashCode()
        {
            return Objects.hash(start, end);
        }

        public String toString()
        {
            return "(" +
                   "" + (start == Long.MIN_VALUE ? "MIN" : start) +
                   ", " + (end == Long.MIN_VALUE ? "MIN" : end) +
                   ']';
        }
    }

    public interface Lookup
    {
        String id(int nodeIdx);
        String dc(int dcIdx);
        String rack(int rackIdx);
        long token(int tokenIdx);
        Lookup forceToken(int tokenIdx, long token);
        void reset();

        default NodeId nodeId(int nodeIdx)
        {
            return ClusterMetadata.current().directory.peerId(addr(nodeIdx));
        }

        default InetAddressAndPort addr(int idx)
        {
            try
            {
                return InetAddressAndPort.getByName(id(idx));
            }
            catch (UnknownHostException e)
            {
                throw new RuntimeException(e);
            }
        }
    }

    public static Lookup constantLookup(String id, long token, String dc, String rack)
    {
        return new Lookup()
        {
            public String id(int nodeIdx)
            {
                return id;
            }

            public String dc(int dcIdx)
            {
                return dc;
            }

            public String rack(int rackIdx)
            {
                return rack;
            }

            @Override
            public org.apache.cassandra.tcm.membership.NodeId nodeId(int nodeIdx)
            {
                return null;
            }

            public long token(int tokenIdx)
            {
                return token;
            }

            public Lookup forceToken(int tokenIdx, long token)
            {
                throw new UnsupportedOperationException();
            }

            @Override
            public InetAddressAndPort addr(int idx)
            {
                return null;
            }

            public void reset()
            {
                throw new UnsupportedOperationException();
            }
        };
    }

    public static class DefaultLookup implements Lookup
    {
        protected final Map<Integer, Long> tokenOverrides = new HashMap<>(2);

        public DefaultLookup()
        {
            // A crafty way to introduce a MIN token
            tokenOverrides.put(10, Long.MIN_VALUE);
        }

        public String id(int nodeIdx)
        {
            return String.format("127.0.%d.%d", nodeIdx / 256, nodeIdx % 256);
        }

        public long token(int tokenIdx)
        {
            Long override = tokenOverrides.get(tokenIdx);
            if (override != null)
                return override;
            long token = PCGFastPure.next(tokenIdx, 1L);
            for (Long value : tokenOverrides.values())
            {
                if (token == value)
                    throw new IllegalStateException(String.format("Generated token %d is already used in an override", token));
            }
            return token;
        }

        public Lookup forceToken(int tokenIdx, long token)
        {
            DefaultLookup newLookup = new DefaultLookup();
            newLookup.tokenOverrides.putAll(tokenOverrides);
            newLookup.tokenOverrides.put(tokenIdx, token);
            return newLookup;
        }

        public void reset()
        {
            tokenOverrides.clear();
        }

        public String dc(int dcIdx)
        {
            return String.format("datacenter%d", dcIdx);
        }

        public String rack(int rackIdx)
        {
            return String.format("rack%d", rackIdx);
        }
    }

    public static class HumanReadableTokensLookup extends DefaultLookup {
        @Override
        public long token(int tokenIdx)
        {
            Long override = tokenOverrides.get(tokenIdx);
            if (override != null)
                return override;
            return tokenIdx * 100L;
        }

        public Lookup forceToken(int tokenIdx, long token)
        {
            DefaultLookup lookup = new HumanReadableTokensLookup();
            lookup.tokenOverrides.putAll(tokenOverrides);
            lookup.tokenOverrides.put(tokenIdx, token);
            return lookup;
        }
    }

    public static class Replica implements Comparable<Replica>
    {
        private final Node node;
        private final boolean full;

        public Replica(Node node, boolean full)
        {
            this.node = node;
            this.full = full;
        }

        public Node node()
        {
            return node;
        }

        public boolean isFull()
        {
            return full;
        }

        public boolean isTransient()
        {
            return !full;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) return true;
            if (o == null || !Replica.class.isAssignableFrom(o.getClass())) return false;
            Replica replica = (Replica) o;
            return full == replica.full && Objects.equals(node, replica.node);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(full, node);
        }

        public String toString()
        {
            return String.format("%s(%s)", (full ? "Full" : "Transient"), node.toString());
        }

        @Override
        public int compareTo(Replica o) {
            return node.compareTo(o.node);
        }
    }

    public static NodeFactory nodeFactory()
    {
        return new NodeFactory(new DefaultLookup());
    }

    public static NodeFactory nodeFactoryHumanReadable()
    {
        return new NodeFactory(new HumanReadableTokensLookup());
    }

    public static class NodeFactory implements TokenSupplier
    {
        private final Lookup lookup;

        public NodeFactory(Lookup lookup)
        {
            this.lookup = lookup;
        }

        public Node make(int idx, int dc, int rack)
        {
            return new Node(idx, idx, dc, rack, lookup);
        }

        public Lookup lookup()
        {
            return lookup;
        }

        public Collection<String> tokens(int i)
        {
            return Collections.singletonList(Long.toString(lookup.token(i)));
        }
    }

    public static class Node implements Comparable<Node>
    {
        private final int tokenIdx;
        private final int nodeIdx;
        private final int dcIdx;
        private final int rackIdx;
        private final Lookup lookup;

        public Node(int tokenIdx, int idx, int dcIdx, int rackIdx, Lookup lookup)
        {
            this.tokenIdx = tokenIdx;
            this.nodeIdx = idx;
            this.dcIdx = dcIdx;
            this.rackIdx = rackIdx;
            this.lookup = lookup;
        }

        public String id()
        {
            return lookup.id(nodeIdx);
        }

        public int idx()
        {
            return nodeIdx;
        }

        public int dcIdx()
        {
            return dcIdx;
        }

        public int rackIdx()
        {
            return rackIdx;
        }

        public String dc()
        {
            return lookup.dc(dcIdx);
        }

        public String rack()
        {
            return lookup.rack(rackIdx);
        }

        public long token()
        {
            return lookup.token(tokenIdx);
        }

        public int tokenIdx()
        {
            return tokenIdx;
        }

        public Node withNewToken()
        {
            return new Node(tokenIdx + 100_000, nodeIdx, dcIdx, rackIdx, lookup);
        }

        public Node withToken(int tokenIdx)
        {
            return new Node(tokenIdx, nodeIdx, dcIdx, rackIdx, lookup);
        }

        public Node overrideToken(long override)
        {
            return new Node(tokenIdx, nodeIdx, dcIdx, rackIdx, lookup.forceToken(tokenIdx, override));
        }
        public Murmur3Partitioner.LongToken longToken()
        {
            return new Murmur3Partitioner.LongToken(token());
        }

        public NodeId nodeId()
        {
            return lookup.nodeId(idx());
        }

        public InetAddressAndPort addr()
        {
            return lookup.addr(idx());
        }

        public boolean equals(Object o)
        {
            if (this == o) return true;
            if (o == null || !Node.class.isAssignableFrom(o.getClass())) return false;
            Node node = (Node) o;
            return Objects.equals(nodeIdx, node.nodeIdx);
        }

        public int hashCode()
        {
            return Objects.hash(nodeIdx);
        }

        public int compareTo(Node o)
        {
            return Long.compare(token(), o.token());
        }

        public String toString()
        {
            return String.format("%s-%s@%d", dc(), id(), token());
        }
    }
}
