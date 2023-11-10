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
package org.apache.cassandra.index.sai.disk.v1;

import java.util.Arrays;
import java.util.Map;
import java.util.stream.Collectors;

import io.github.jbellis.jvector.vector.VectorSimilarityFunction;
import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.index.sai.disk.v1.vector.OptimizeFor;
import org.apache.cassandra.index.sai.utils.IndexTermType;

import static org.apache.cassandra.config.CassandraRelevantProperties.SAI_VECTOR_SEARCH_MAX_TOP_K;

/**
 * Per-index config for storage-attached index writers.
 */
public class IndexWriterConfig
{
    public static final String MAXIMUM_NODE_CONNECTIONS = "maximum_node_connections";
    public static final int MAXIMUM_MAXIMUM_NODE_CONNECTIONS = 512;
    public static final int DEFAULT_MAXIMUM_NODE_CONNECTIONS = 16;

    public static final String CONSTRUCTION_BEAM_WIDTH = "construction_beam_width";
    public static final int MAXIMUM_CONSTRUCTION_BEAM_WIDTH = 3200;
    public static final int DEFAULT_CONSTRUCTION_BEAM_WIDTH = 100;

    public static final String SIMILARITY_FUNCTION = "similarity_function";
    public static final VectorSimilarityFunction DEFAULT_SIMILARITY_FUNCTION = VectorSimilarityFunction.COSINE;
    public static final String validSimilarityFunctions = Arrays.stream(VectorSimilarityFunction.values())
                                                                .map(Enum::name)
                                                                .collect(Collectors.joining(", "));

    public static final String OPTIMIZE_FOR = "optimize_for";
    private static final OptimizeFor DEFAULT_OPTIMIZE_FOR = OptimizeFor.LATENCY;
    private static final String validOptimizeFor = Arrays.stream(OptimizeFor.values())
                                                         .map(Enum::name)
                                                         .collect(Collectors.joining(", "));

    public static final int MAX_TOP_K = SAI_VECTOR_SEARCH_MAX_TOP_K.getInt();

    private static final IndexWriterConfig EMPTY_CONFIG = new IndexWriterConfig(-1, -1, null, null);

    // The maximum number of outgoing connections a node can have in a graph.
    private final int maximumNodeConnections;

    // The size of the beam search used when finding nearest neighbours.
    private final int constructionBeamWidth;

    // Used to determine the search to determine the topK results. The score returned is used to order the topK results.
    private final VectorSimilarityFunction similarityFunction;

    private final OptimizeFor optimizeFor;

    public IndexWriterConfig(int maximumNodeConnections,
                             int constructionBeamWidth,
                             VectorSimilarityFunction similarityFunction,
                             OptimizeFor optimizerFor)
    {
        this.maximumNodeConnections = maximumNodeConnections;
        this.constructionBeamWidth = constructionBeamWidth;
        this.similarityFunction = similarityFunction;
        this.optimizeFor = optimizerFor;
    }

    public int getMaximumNodeConnections()
    {
        return maximumNodeConnections;
    }

    public int getConstructionBeamWidth()
    {
        return constructionBeamWidth;
    }

    public VectorSimilarityFunction getSimilarityFunction()
    {
        return similarityFunction;
    }

    public OptimizeFor getOptimizeFor()
    {
        return optimizeFor;
    }

    public static IndexWriterConfig fromOptions(String indexName, IndexTermType indexTermType, Map<String, String> options)
    {
        int maximumNodeConnections = DEFAULT_MAXIMUM_NODE_CONNECTIONS;
        int queueSize = DEFAULT_CONSTRUCTION_BEAM_WIDTH;
        VectorSimilarityFunction similarityFunction = DEFAULT_SIMILARITY_FUNCTION;
        OptimizeFor optimizeFor = DEFAULT_OPTIMIZE_FOR;

        if (options.get(MAXIMUM_NODE_CONNECTIONS) != null ||
            options.get(CONSTRUCTION_BEAM_WIDTH) != null ||
            options.get(SIMILARITY_FUNCTION) != null ||
            options.get(OPTIMIZE_FOR) != null)
        {
            if (!indexTermType.isVector())
                throw new InvalidRequestException(String.format("CQL type %s cannot have vector options", indexTermType.asCQL3Type()));

            if (options.containsKey(MAXIMUM_NODE_CONNECTIONS))
            {
                if (!CassandraRelevantProperties.SAI_VECTOR_ALLOW_CUSTOM_PARAMETERS.getBoolean())
                    throw new InvalidRequestException(String.format("Maximum node connections cannot be set without enabling %s", CassandraRelevantProperties.SAI_VECTOR_ALLOW_CUSTOM_PARAMETERS.name()));

                try
                {
                    maximumNodeConnections = Integer.parseInt(options.get(MAXIMUM_NODE_CONNECTIONS));
                }
                catch (NumberFormatException e)
                {
                    throw new InvalidRequestException(String.format("Maximum number of connections %s is not a valid integer for index %s",
                                                                    options.get(MAXIMUM_NODE_CONNECTIONS), indexName));
                }
                if (maximumNodeConnections <= 0 || maximumNodeConnections > MAXIMUM_MAXIMUM_NODE_CONNECTIONS)
                    throw new InvalidRequestException(String.format("Maximum number of connections for index %s cannot be <= 0 or > %s, was %s", indexName, MAXIMUM_MAXIMUM_NODE_CONNECTIONS, maximumNodeConnections));
            }
            if (options.containsKey(CONSTRUCTION_BEAM_WIDTH))
            {
                if (!CassandraRelevantProperties.SAI_VECTOR_ALLOW_CUSTOM_PARAMETERS.getBoolean())
                    throw new InvalidRequestException(String.format("Construction beam width cannot be set without enabling %s", CassandraRelevantProperties.SAI_VECTOR_ALLOW_CUSTOM_PARAMETERS.name()));

                try
                {
                    queueSize = Integer.parseInt(options.get(CONSTRUCTION_BEAM_WIDTH));
                }
                catch (NumberFormatException e)
                {
                    throw new InvalidRequestException(String.format("Construction beam width %s is not a valid integer for index %s",
                                                                    options.get(CONSTRUCTION_BEAM_WIDTH), indexName));
                }
                if (queueSize <= 0 || queueSize > MAXIMUM_CONSTRUCTION_BEAM_WIDTH)
                    throw new InvalidRequestException(String.format("Construction beam width for index %s cannot be <= 0 or > %s, was %s", indexName, MAXIMUM_CONSTRUCTION_BEAM_WIDTH, queueSize));
            }
            if (options.containsKey(SIMILARITY_FUNCTION))
            {
                String option = options.get(SIMILARITY_FUNCTION).toUpperCase();
                try
                {
                    similarityFunction = VectorSimilarityFunction.valueOf(option);
                }
                catch (IllegalArgumentException e)
                {
                    throw new InvalidRequestException(String.format("Similarity function %s was not recognized for index %s. Valid values are: %s",
                                                                    option, indexName, validSimilarityFunctions));
                }
            }
            if (options.containsKey(OPTIMIZE_FOR))
            {
                String option = options.get(OPTIMIZE_FOR).toUpperCase();
                try
                {
                    optimizeFor = OptimizeFor.valueOf(option);
                }
                catch (IllegalArgumentException e)
                {
                    throw new InvalidRequestException(String.format("optimize_for '%s' was not recognized for index %s. Valid values are: %s",
                                                                    option, indexName, validOptimizeFor));
                }
            }
        }
        return new IndexWriterConfig(maximumNodeConnections, queueSize, similarityFunction, optimizeFor);
    }

    public static IndexWriterConfig emptyConfig()
    {
        return EMPTY_CONFIG;
    }

    @Override
    public String toString()
    {
        return String.format("IndexWriterConfig{%s=%d, %s=%d, %s=%s, %s=%s}",
                             MAXIMUM_NODE_CONNECTIONS, maximumNodeConnections,
                             CONSTRUCTION_BEAM_WIDTH, constructionBeamWidth,
                             SIMILARITY_FUNCTION, similarityFunction,
                             OPTIMIZE_FOR, optimizeFor);
    }
}
