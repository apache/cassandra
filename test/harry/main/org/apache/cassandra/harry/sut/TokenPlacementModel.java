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

import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.distributed.api.TokenSupplier;
import org.apache.cassandra.harry.gen.rng.PCGFastPure;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.membership.Location;
import org.apache.cassandra.tcm.membership.NodeId;

public class TokenPlacementModel
{
    public abstract static class ReplicationFactor
    {
        private final int nodesTotal;

        public ReplicationFactor(int total)
        {
            this.nodesTotal = total;
        }

        public int total()
        {
            return nodesTotal;
        }

        public abstract int dcs();

        public abstract KeyspaceParams asKeyspaceParams();

        public abstract Map<String, Integer> asMap();

        public ReplicatedRanges replicate(List<Node> nodes)
        {
            return replicate(toRanges(nodes), nodes);
        }
        public abstract ReplicatedRanges replicate(Range[] ranges, List<Node> nodes);
    }

    public static class ReplicatedRanges
    {
        public final Range[] ranges;
        public final NavigableMap<Range, List<Node>> placementsForRange;

        public ReplicatedRanges(Range[] ranges, NavigableMap<Range, List<Node>> placementsForRange)
        {
            this.ranges = ranges;
            this.placementsForRange = placementsForRange;
        }

        public List<Node> replicasFor(long token)
        {
            int idx = indexedBinarySearch(ranges, range -> {
                // exclusive start, so token at the start belongs to a lower range
                if (token <= range.start)
                    return 1;
                // ie token > start && token <= end
                if (token <= range.end ||range.end == Long.MIN_VALUE)
                    return 0;

                return -1;
            });
            assert idx >= 0 : String.format("Somehow ranges %s do not contain token %d", Arrays.toString(ranges), token);
            return placementsForRange.get(ranges[idx]);
        }

        public NavigableMap<Range, List<Node>> asMap()
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

    public static void addIfUnique(List<Node> nodes, Set<Integer> names, Node node)
    {
        if (names.contains(node.idx()))
            return;
        nodes.add(node);
        names.add(node.idx());
    }

    /**
     * Finds a primary replica
     */
    public static int primaryReplica(List<Node> nodes, Range range)
    {
        for (int i = 0; i < nodes.size(); i++)
        {
            if (range.end != Long.MIN_VALUE && nodes.get(i).token() >= range.end)
                return i;
        }
        return -1;
    }


    /**
     * Generates token ranges from the list of nodes
     */
    public static Range[] toRanges(List<Node> nodes)
    {
        List<Long> tokens = new ArrayList<>();
        for (Node node : nodes)
            tokens.add(node.token());
        tokens.add(Long.MIN_VALUE);
        tokens.sort(Long::compareTo);

        Range[] ranges = new Range[nodes.size() + 1];
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
        private final Lookup lookup = new DefaultLookup();
        private KeyspaceParams keyspaceParams;
        private final Map<String, Integer> map;

        public NtsReplicationFactor(int... nodesPerDc)
        {
            super(total(nodesPerDc));
            this.map = toMap(nodesPerDc, lookup);
        }

        public NtsReplicationFactor(int dcs, int nodesPerDc)
        {
            super(dcs * nodesPerDc);
            int[] counts = new int[dcs];
            Arrays.fill(counts, nodesPerDc);
            this.map = toMap(counts, lookup);
        }

        public NtsReplicationFactor(Map<String, Integer> m)
        {
            super(m.values().stream().reduce(0, Integer::sum));
            this.map = m;
        }

        private static int total(int... num)
        {
            int tmp = 0;
            for (int i : num)
                tmp += i;
            return tmp;
        }

        public int dcs()
        {
            return map.size();
        }

        public KeyspaceParams asKeyspaceParams()
        {
            if (this.keyspaceParams == null)
                this.keyspaceParams = toKeyspaceParams();
            return this.keyspaceParams;
        }

        public Map<String, Integer> asMap()
        {
            return this.map;
        }

        public ReplicatedRanges replicate(Range[] ranges, List<Node> nodes)
        {
            return replicate(ranges, nodes, asMap());
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

        public static ReplicatedRanges replicate(Range[] ranges, List<Node> nodes, Map<String, Integer> rfs)
        {
            assertStrictlySorted(nodes);
            Map<String, DatacenterNodes> template = new HashMap<>();

            Map<String, List<Node>> nodesByDC = nodesByDC(nodes);
            Map<String, Set<String>> racksByDC = racksByDC(nodes);

            for (Map.Entry<String, Integer> entry : rfs.entrySet())
            {
                String dc = entry.getKey();
                int rf = entry.getValue();
                List<Node> nodesInThisDC = nodesByDC.get(dc);
                Set<String> racksInThisDC = racksByDC.get(dc);
                int nodeCount = nodesInThisDC == null ? 0 : nodesInThisDC.size();
                int rackCount = racksInThisDC == null ? 0 : racksInThisDC.size();
                if (rf <= 0 || nodeCount == 0)
                    continue;

                template.put(dc, new DatacenterNodes(rf, rackCount, nodeCount));
            }

            NavigableMap<Range, Map<String, List<Node>>> replication = new TreeMap<>();

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

                    replication.put(range, mapValues(nodesInDCs, v -> v.nodes));
                }
                else
                {
                    // if the range end is larger than the highest assigned token, then treat it
                    // as part of the wraparound and replicate it to the same nodes as the first
                    // range. This is most likely caused by a decommission removing the node with
                    // the largest token.
                    replication.put(range, replication.get(ranges[0]));
                }
            }

            return combine(replication);
        }

        /**
         * Replicate ranges to rf nodes.
         */
        private static ReplicatedRanges combine(NavigableMap<Range, Map<String, List<Node>>> orig)
        {

            Range[] ranges = new Range[orig.size()];
            int idx = 0;
            NavigableMap<Range, List<Node>> flattened = new TreeMap<>();
            for (Map.Entry<Range, Map<String, List<Node>>> e : orig.entrySet())
            {
                List<Node> placementsForRange = new ArrayList<>();
                for (List<Node> v : e.getValue().values())
                    placementsForRange.addAll(v);
                ranges[idx++] = e.getKey();
                flattened.put(e.getKey(), placementsForRange);
            }
            return new ReplicatedRanges(ranges, flattened);
        }

        private KeyspaceParams toKeyspaceParams()
        {
            Object[] args = new Object[map.size() * 2];
            int i = 0;
            for (Map.Entry<String, Integer> e : map.entrySet())
            {
                args[i * 2] = e.getKey();
                args[i * 2 + 1] = e.getValue();
                i++;
            }

            return KeyspaceParams.nts(args);
        }

        private Map<String, Integer> toMap(int[] nodesPerDc, Lookup lookup)
        {
            Map<String, Integer> map = new TreeMap<>();
            for (int i = 0; i < nodesPerDc.length; i++)
            {
                map.put(lookup.dc(i + 1), nodesPerDc[i]);
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

        public DatacenterNodes copy()
        {
            return new DatacenterNodes(rfLeft, acceptableRackRepeats);
        }

        DatacenterNodes(int rf,
                        int rackCount,
                        int nodeCount)
        {
            this.rfLeft = Math.min(rf, nodeCount);
            acceptableRackRepeats = rf - rackCount;
        }

        // for copying
        DatacenterNodes(int rfLeft, int acceptableRackRepeats)
        {
            this.rfLeft = rfLeft;
            this.acceptableRackRepeats = acceptableRackRepeats;
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
        private final Lookup lookup = new DefaultLookup();
        public SimpleReplicationFactor(int total)
        {
            super(total);
        }

        public int dcs()
        {
            return 1;
        }

        public KeyspaceParams asKeyspaceParams()
        {
            return KeyspaceParams.simple(total());
        }

        public Map<String, Integer> asMap()
        {
            return Collections.singletonMap(lookup.dc(1), total());
        }

        public ReplicatedRanges replicate(Range[] ranges, List<Node> nodes)
        {
            return replicate(ranges, nodes, total());
        }

        public static ReplicatedRanges replicate(Range[] ranges, List<Node> nodes, int rf)
        {
            NavigableMap<Range, List<Node>> replication = new TreeMap<>();
            for (Range range : ranges)
            {
                Set<Integer> names = new HashSet<>();
                List<Node> replicas = new ArrayList<>();
                int idx = primaryReplica(nodes, range);
                if (idx >= 0)
                {
                    for (int i = idx; i < nodes.size() && replicas.size() < rf; i++)
                        addIfUnique(replicas, names, nodes.get(i));

                    for (int i = 0; replicas.size() < rf && i < idx; i++)
                        addIfUnique(replicas, names, nodes.get(i));
                    if (range.start == Long.MIN_VALUE)
                        replication.put(ranges[ranges.length - 1], replicas);
                    replication.put(range, replicas);
                }
                else
                {
                    // if the range end is larger than the highest assigned token, then treat it
                    // as part of the wraparound and replicate it to the same nodes as the first
                    // range. This is most likely caused by a decommission removing the node with
                    // the largest token.
                    replication.put(range, replication.get(ranges[0]));
                }
            }

            return new ReplicatedRanges(ranges, Collections.unmodifiableNavigableMap(replication));
        }

        public String toString()
        {
            return "SimpleReplicationFactor{" +
                   "rf=" + total() +
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
        protected final Map<Integer, Long> overrides = new HashMap<>(2);

        public String id(int nodeIdx)
        {
            return String.format("127.0.%d.%d", nodeIdx / 256, nodeIdx % 256);
        }

        public long token(int tokenIdx)
        {
            Long override = overrides.get(tokenIdx);
            if (override != null)
                return override;
            return PCGFastPure.next(tokenIdx, 1L);
        }

        public Lookup forceToken(int tokenIdx, long token)
        {
            DefaultLookup newLookup = new DefaultLookup();
            newLookup.overrides.putAll(overrides);
            newLookup.overrides.put(tokenIdx, token);
            return newLookup;
        }

        public void reset()
        {
            overrides.clear();
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
            Long override = overrides.get(tokenIdx);
            if (override != null)
                return override;
            return tokenIdx * 100L;
        }

        public Lookup forceToken(int tokenIdx, long token)
        {
            DefaultLookup lookup = new HumanReadableTokensLookup();
            lookup.overrides.putAll(overrides);
            lookup.overrides.put(tokenIdx, token);
            return lookup;
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
