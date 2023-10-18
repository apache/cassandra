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

package org.apache.cassandra.dht;

import java.math.BigInteger;
import java.util.Collection;
import java.util.Comparator;
import java.util.Set;
import java.util.stream.Collectors;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;

import org.apache.cassandra.locator.EndpointsByRange;
import org.apache.cassandra.locator.EndpointsForRange;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.locator.Replica;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.locator.Replicas;
import org.psjava.algo.graph.flownetwork.FordFulkersonAlgorithm;
import org.psjava.algo.graph.flownetwork.MaximumFlowAlgorithm;
import org.psjava.algo.graph.flownetwork.MaximumFlowAlgorithmResult;
import org.psjava.algo.graph.pathfinder.DFSPathFinder;
import org.psjava.ds.graph.CapacityEdge;
import org.psjava.ds.graph.MutableCapacityGraph;
import org.psjava.ds.numbersystrem.IntegerNumberSystem;
import org.psjava.ds.math.Function;

/**
 * We model the graph like this:
 * * Each range we are about to stream is a vertex in the graph
 * * Each node that can provide a range is a vertex in the graph
 * * We add an edge from each range to the node that can provide the range
 * * Then, to be able to solve the maximum flow problem using Ford-Fulkerson we add a super source with edges to all range vertices
 *   and a super sink with incoming edges from all the node vertices.
 * * The capacity on the edges between the super source and the range-vertices is 1
 * * The capacity on the edges between the range-vertices and the node vertices is infinite
 * * The capacity on the edges between the nodes-vertices and the super sink is ceil(#range-vertices/#node-vertices)
 *   - if we have more machines than ranges to stream the capacity will be 1 (each machine will stream at most 1 range)
 * * Since the sum of the capacity on the edges from the super source to the range-vertices is less or equal to the sum
 *   of the capacities between the node-vertices and super sink we know that to get maximum flow we will use all the
 *   range-vertices. (Say we have x ranges, y machines to provide them, total supersource -> range-vertice capacity will be x,
 *   total node-vertice -> supersink capacity will be (y * ceil(x / y)) which worst case is x if x==y). The capacity between
 *   the range-vertices and node-vertices is infinite.
 * * Then we try to solve the max-flow problem using psjava
 * * If we can't find a solution where the total flow is = number of range-vertices, we bump the capacity between the node-vertices
 *   and the super source and try again.
 *
 *
 */
public class RangeFetchMapCalculator
{
    private static final Logger logger = LoggerFactory.getLogger(RangeFetchMapCalculator.class);
    private static final long TRIVIAL_RANGE_LIMIT = 1000;
    private final EndpointsByRange rangesWithSources;
    private final Predicate<Replica> sourceFilters;
    private final String keyspace;
    //We need two Vertices to act as source and destination in the algorithm
    private final Vertex sourceVertex = OuterVertex.getSourceVertex();
    private final Vertex destinationVertex = OuterVertex.getDestinationVertex();
    private final Set<Range<Token>> trivialRanges;

    public RangeFetchMapCalculator(EndpointsByRange rangesWithSources,
                                   Collection<RangeStreamer.SourceFilter> sourceFilters,
                                   String keyspace)
    {
        this.rangesWithSources = rangesWithSources;
        this.sourceFilters = Predicates.and(sourceFilters);
        this.keyspace = keyspace;
        this.trivialRanges = rangesWithSources.keySet()
                                              .stream()
                                              .filter(RangeFetchMapCalculator::isTrivial)
                                              .collect(Collectors.toSet());
    }

    static boolean isTrivial(Range<Token> range)
    {
        IPartitioner partitioner = DatabaseDescriptor.getPartitioner();
        if (partitioner.splitter().isPresent())
        {
            BigInteger l = partitioner.splitter().get().valueForToken(range.left);
            BigInteger r = partitioner.splitter().get().valueForToken(range.right);
            if (r.compareTo(l) <= 0)
                return false;
            if (r.subtract(l).compareTo(BigInteger.valueOf(TRIVIAL_RANGE_LIMIT)) < 0)
                return true;
        }
        return false;
    }

    public Multimap<InetAddressAndPort, Range<Token>> getRangeFetchMap()
    {
        Multimap<InetAddressAndPort, Range<Token>> fetchMap = HashMultimap.create();
        fetchMap.putAll(getRangeFetchMapForNonTrivialRanges());
        fetchMap.putAll(getRangeFetchMapForTrivialRanges(fetchMap));
        return fetchMap;
    }

    @VisibleForTesting
    Multimap<InetAddressAndPort, Range<Token>> getRangeFetchMapForNonTrivialRanges()
    {
        //Get the graph with edges between ranges and their source endpoints
        MutableCapacityGraph<Vertex, Integer> graph = getGraph();
        //Add source and destination vertex and edges
        addSourceAndDestination(graph, getDestinationLinkCapacity(graph));

        int flow = 0;
        MaximumFlowAlgorithmResult<Integer, CapacityEdge<Vertex, Integer>> result = null;

        //We might not be working on all ranges
        while (flow < getTotalRangeVertices(graph))
        {
            if (flow > 0)
            {
                //We could not find a path with previous graph. Bump the capacity b/w endpoint vertices and destination by 1
                incrementCapacity(graph, 1);
            }

            MaximumFlowAlgorithm fordFulkerson = FordFulkersonAlgorithm.getInstance(DFSPathFinder.getInstance());
            result = fordFulkerson.calc(graph, sourceVertex, destinationVertex, IntegerNumberSystem.getInstance());

            int newFlow = result.calcTotalFlow();
            assert newFlow > flow;   //We are not making progress which should not happen
            flow = newFlow;
        }

        return getRangeFetchMapFromGraphResult(graph, result);
    }

    @VisibleForTesting
    Multimap<InetAddressAndPort, Range<Token>> getRangeFetchMapForTrivialRanges(Multimap<InetAddressAndPort, Range<Token>> optimisedMap)
    {
        Multimap<InetAddressAndPort, Range<Token>> fetchMap = HashMultimap.create();
        for (Range<Token> trivialRange : trivialRanges)
        {
            boolean added = false;
            boolean localDCCheck = true;
            while (!added)
            {
                // sort with the endpoint having the least number of streams first:
                EndpointsForRange replicas = rangesWithSources.get(trivialRange)
                        .sorted(Comparator.comparingInt(o -> optimisedMap.get(o.endpoint()).size()));
                Replicas.temporaryAssertFull(replicas);
                for (Replica replica : replicas)
                {
                    if (passFilters(replica, localDCCheck))
                    {
                        added = true;
                        // if we pass filters, it means that we don't filter away localhost and we can count it as a source,
                        // see RangeFetchMapCalculator#addEndpoints  and RangeStreamer#getRangeFetchMap
                        if (replica.isSelf())
                            continue; // but don't add localhost to avoid streaming locally
                        fetchMap.put(replica.endpoint(), trivialRange);
                        break;
                    }
                }
                if (!added && !localDCCheck)
                    throw new IllegalStateException("Unable to find sufficient sources for streaming range " + trivialRange + " in keyspace " + keyspace);
                if (!added)
                    logger.info("Using other DC endpoints for streaming for range: {} and keyspace {}", trivialRange, keyspace);
                localDCCheck = false;
            }
        }
        return fetchMap;
    }
    /*
        Return the total number of range vertices in the graph
     */
    private int getTotalRangeVertices(MutableCapacityGraph<Vertex, Integer> graph)
    {
        int count = 0;
        for (Vertex vertex : graph.getVertices())
        {
            if (vertex.isRangeVertex())
            {
                count++;
            }
        }

        return count;
    }

    /**
     *  Convert the max flow graph to {@code Multimap<InetAddress, Range<Token>>}
     *      We iterate over all range vertices and find an edge with flow of more than zero connecting to endpoint vertex.
     * @param graph  The graph to convert
     * @param result Flow algorithm result
     * @return  Multi Map of Machine to Ranges
     */
    private Multimap<InetAddressAndPort, Range<Token>> getRangeFetchMapFromGraphResult(MutableCapacityGraph<Vertex, Integer> graph, MaximumFlowAlgorithmResult<Integer, CapacityEdge<Vertex, Integer>> result)
    {
        final Multimap<InetAddressAndPort, Range<Token>> rangeFetchMapMap = HashMultimap.create();
        if(result == null)
            return rangeFetchMapMap;
        final Function<CapacityEdge<Vertex, Integer>, Integer> flowFunction = result.calcFlowFunction();

        for (Vertex vertex : graph.getVertices())
        {
            if (vertex.isRangeVertex())
            {
                boolean sourceFound = false;
                for (CapacityEdge<Vertex, Integer> e : graph.getEdges(vertex))
                {
                    if(flowFunction.get(e) > 0)
                    {
                        assert !sourceFound;
                        sourceFound = true;
                        if(e.to().isEndpointVertex())
                            rangeFetchMapMap.put(((EndpointVertex)e.to()).getEndpoint(), ((RangeVertex)vertex).getRange());
                        else if(e.from().isEndpointVertex())
                            rangeFetchMapMap.put(((EndpointVertex)e.from()).getEndpoint(), ((RangeVertex)vertex).getRange());
                    }
                }

                assert sourceFound;

            }
        }

        return rangeFetchMapMap;
    }

    /**
     * This will increase the capacity from endpoint vertices to destination by incrementalCapacity
     * @param graph The graph to work on
     * @param incrementalCapacity Amount by which to increment capacity
     */
    private void incrementCapacity(MutableCapacityGraph<Vertex, Integer> graph, int incrementalCapacity)
    {
        for (Vertex vertex : graph.getVertices())
        {
            if (vertex.isEndpointVertex())
            {
                graph.addEdge(vertex, destinationVertex, incrementalCapacity);
            }
        }
    }

    /**
     * Add source and destination vertices. Add edges of capacity 1 b/w source and range vertices.
     * Also add edges b/w endpoint vertices and destination vertex with capacity of 'destinationCapacity'
     * @param graph Graph to work on
     * @param destinationCapacity The capacity for edges b/w endpoint vertices and destination
     */
    private void addSourceAndDestination(MutableCapacityGraph<Vertex, Integer> graph, int destinationCapacity)
    {
        graph.insertVertex(sourceVertex);
        graph.insertVertex(destinationVertex);
        for (Vertex vertex : graph.getVertices())
        {
            if (vertex.isRangeVertex())
            {
                graph.addEdge(sourceVertex, vertex, 1);
            }
            else if (vertex.isEndpointVertex())
            {
                graph.addEdge(vertex, destinationVertex, destinationCapacity);
            }
        }
    }

    /**
     * Find the initial capacity which we want to use b/w machine vertices and destination to keep things optimal
     * @param graph Graph to work on
     * @return  The initial capacity
     */
    private int getDestinationLinkCapacity(MutableCapacityGraph<Vertex, Integer> graph)
    {
        //Find total nodes which are endpoints and ranges
        double endpointVertices = 0;
        double rangeVertices = 0;
        for (Vertex vertex : graph.getVertices())
        {
            if (vertex.isEndpointVertex())
            {
                endpointVertices++;
            }
            else if (vertex.isRangeVertex())
            {
                rangeVertices++;
            }
        }

        return (int) Math.ceil(rangeVertices / endpointVertices);
    }

    /**
     *  Generate a graph with all ranges and endpoints as vertices. It will create edges b/w a range and its filtered source endpoints
     *  It will try to use sources from local DC if possible
     * @return  The generated graph
     */
    private MutableCapacityGraph<Vertex, Integer> getGraph()
    {
        MutableCapacityGraph<Vertex, Integer> capacityGraph = MutableCapacityGraph.create();

        //Connect all ranges with all source endpoints
        for (Range<Token> range : rangesWithSources.keySet())
        {
            if (trivialRanges.contains(range))
            {
                logger.debug("Not optimising trivial range {} for keyspace {}", range, keyspace);
                continue;
            }

            final RangeVertex rangeVertex = new RangeVertex(range);

            //Try to only add source endpoints from same DC
            boolean sourceFound = addEndpoints(capacityGraph, rangeVertex, true);

            if (!sourceFound)
            {
                logger.info("Using other DC endpoints for streaming for range: {} and keyspace {}", range, keyspace);
                sourceFound = addEndpoints(capacityGraph, rangeVertex, false);
            }

            if (!sourceFound)
                throw new IllegalStateException("Unable to find sufficient sources for streaming range " + range + " in keyspace " + keyspace);

        }

        return capacityGraph;
    }

    /**
     * Create edges with infinite capacity b/w range vertex and all its source endpoints which clear the filters
     * @param capacityGraph The Capacity graph on which changes are made
     * @param rangeVertex The range for which we need to add all its source endpoints
     * @param localDCCheck Should add source endpoints from local DC only
     * @return If we were able to add atleast one source for this range after applying filters to endpoints
     */
    private boolean addEndpoints(MutableCapacityGraph<Vertex, Integer> capacityGraph, RangeVertex rangeVertex, boolean localDCCheck)
    {
        boolean sourceFound = false;
        Replicas.temporaryAssertFull(rangesWithSources.get(rangeVertex.getRange()));
        for (Replica replica : rangesWithSources.get(rangeVertex.getRange()))
        {
            if (passFilters(replica, localDCCheck))
            {
                sourceFound = true;
                // if we pass filters, it means that we don't filter away localhost and we can count it as a source:
                if (replica.isSelf())
                    continue; // but don't add localhost to the graph to avoid streaming locally
                final Vertex endpointVertex = new EndpointVertex(replica.endpoint());
                capacityGraph.insertVertex(rangeVertex);
                capacityGraph.insertVertex(endpointVertex);
                capacityGraph.addEdge(rangeVertex, endpointVertex, Integer.MAX_VALUE);
            }
        }
        return sourceFound;
    }

    private boolean isInLocalDC(Replica replica)
    {
        return DatabaseDescriptor.getLocalDataCenter().equals(DatabaseDescriptor.getEndpointSnitch().getDatacenter(replica));
    }

    /**
     *
     * @param replica   Replica to check
     * @param localDCCheck Allow endpoints with local DC
     * @return   True if filters pass this endpoint
     */
    private boolean passFilters(final Replica replica, boolean localDCCheck)
    {
        return sourceFilters.apply(replica) && (!localDCCheck || isInLocalDC(replica));
    }

    private static abstract class Vertex
    {
        public enum VERTEX_TYPE
        {
            ENDPOINT, RANGE, SOURCE, DESTINATION
        }

        public abstract VERTEX_TYPE getVertexType();

        public boolean isEndpointVertex()
        {
            return getVertexType() == VERTEX_TYPE.ENDPOINT;
        }

        public boolean isRangeVertex()
        {
            return getVertexType() == VERTEX_TYPE.RANGE;
        }
    }

    /*
       This Vertex will contain the endpoints.
     */
    private static class EndpointVertex extends Vertex
    {
        private final InetAddressAndPort endpoint;

        public EndpointVertex(InetAddressAndPort endpoint)
        {
            assert endpoint != null;
            this.endpoint = endpoint;
        }

        public InetAddressAndPort getEndpoint()
        {
            return endpoint;
        }


        @Override
        public VERTEX_TYPE getVertexType()
        {
            return VERTEX_TYPE.ENDPOINT;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            EndpointVertex that = (EndpointVertex) o;

            return endpoint.equals(that.endpoint);

        }

        @Override
        public int hashCode()
        {
            return endpoint.hashCode();
        }
    }

    /*
       This Vertex will contain the Range
     */
    private static class RangeVertex extends Vertex
    {
        private final Range<Token> range;

        public RangeVertex(Range<Token> range)
        {
            assert range != null;
            this.range = range;
        }

        public Range<Token> getRange()
        {
            return range;
        }

        @Override
        public VERTEX_TYPE getVertexType()
        {
            return VERTEX_TYPE.RANGE;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            RangeVertex that = (RangeVertex) o;

            return range.equals(that.range);

        }

        @Override
        public int hashCode()
        {
            return range.hashCode();
        }
    }

    /*
       This denotes the source and destination Vertex we need for the flow graph
     */
    private static class OuterVertex extends Vertex
    {
        private final boolean source;

        private OuterVertex(boolean source)
        {
            this.source = source;
        }

        public static Vertex getSourceVertex()
        {
            return new OuterVertex(true);
        }

        public static Vertex getDestinationVertex()
        {
            return new OuterVertex(false);
        }

        @Override
        public VERTEX_TYPE getVertexType()
        {
            return source? VERTEX_TYPE.SOURCE : VERTEX_TYPE.DESTINATION;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            OuterVertex that = (OuterVertex) o;

            return source == that.source;

        }

        @Override
        public int hashCode()
        {
            return (source ? 1 : 0);
        }
    }
}
