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

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.index.sai.utils.TypeUtil;
import io.github.jbellis.jvector.vector.VectorSimilarityFunction;

import static org.apache.cassandra.config.CassandraRelevantProperties.SAI_VECTOR_SEARCH_MAX_TOP_K;

/**
 * Per-index config for storage-attached index writers.
 */
public class IndexWriterConfig
{
    public static final String POSTING_LIST_LVL_MIN_LEAVES = "bkd_postings_min_leaves";
    public static final String POSTING_LIST_LVL_SKIP_OPTION = "bkd_postings_skip";

    private static final int DEFAULT_POSTING_LIST_MIN_LEAVES = 64;
    private static final int DEFAULT_POSTING_LIST_LVL_SKIP = 3;

    public static final String MAXIMUM_NODE_CONNECTIONS = "maximum_node_connections";
    public static final String CONSTRUCTION_BEAM_WIDTH = "construction_beam_width";
    public static final String SIMILARITY_FUNCTION = "similarity_function";

    public static final int MAXIMUM_MAXIMUM_NODE_CONNECTIONS = 512;
    public static final int MAXIMUM_CONSTRUCTION_BEAM_WIDTH = 3200;

    public static final int DEFAULT_MAXIMUM_NODE_CONNECTIONS = 16;
    public static final int DEFAULT_CONSTRUCTION_BEAM_WIDTH = 100;

    public static final int MAX_TOP_K = SAI_VECTOR_SEARCH_MAX_TOP_K.getInt();

    public static final VectorSimilarityFunction DEFAULT_SIMILARITY_FUNCTION = VectorSimilarityFunction.COSINE;

    public static final String validSimilarityFunctions = Arrays.stream(VectorSimilarityFunction.values())
                                                                .map(e -> e.name())
                                                                .collect(Collectors.joining(", "));

    private static final IndexWriterConfig EMPTY_CONFIG = new IndexWriterConfig(null, -1, -1, -1, -1, null);

    /**
     * Fully qualified index name, in the format "<keyspace>.<table>.<index_name>".
     */
    private final String indexName;

    /**
     * Skip, or the sampling interval, for selecting a bkd tree level that is eligible for an auxiliary posting list.
     * Sampling starts from 0, but bkd tree root node is at level 1. For skip = 4, eligible levels are 4, 8, 12, etc (no
     * level 0, because there is no node at level 0).
     */
    private final int bkdPostingsSkip;

    /**
     * Min. number of reachable leaves for a given node to be eligible for an auxiliary posting list.
     */
    private final int bkdPostingsMinLeaves;

    private final int maximumNodeConnections;

    private final int constructionBeamWidth;

    private final VectorSimilarityFunction similarityFunction;

    public IndexWriterConfig(String indexName,
                             int bkdPostingsSkip,
                             int bkdPostingsMinLeaves)
    {
        this(indexName,
             bkdPostingsSkip,
             bkdPostingsMinLeaves,
             DEFAULT_MAXIMUM_NODE_CONNECTIONS,
             DEFAULT_CONSTRUCTION_BEAM_WIDTH,
             DEFAULT_SIMILARITY_FUNCTION);
    }

    public IndexWriterConfig(String indexName,
                             int bkdPostingsSkip,
                             int bkdPostingsMinLeaves,
                             int maximumNodeConnections,
                             int constructionBeamWidth,
                             VectorSimilarityFunction similarityFunction)
    {
        this.indexName = indexName;
        this.bkdPostingsSkip = bkdPostingsSkip;
        this.bkdPostingsMinLeaves = bkdPostingsMinLeaves;
        this.maximumNodeConnections = maximumNodeConnections;
        this.constructionBeamWidth = constructionBeamWidth;
        this.similarityFunction = similarityFunction;
    }

    public String getIndexName()
    {
        return indexName;
    }

    public int getBkdPostingsMinLeaves()
    {
        return bkdPostingsMinLeaves;
    }

    public int getBkdPostingsSkip()
    {
        return bkdPostingsSkip;
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

    public static IndexWriterConfig fromOptions(String indexName, AbstractType<?> type, Map<String, String> options)
    {
        int minLeaves = DEFAULT_POSTING_LIST_MIN_LEAVES;
        int skip = DEFAULT_POSTING_LIST_LVL_SKIP;
        int maximumNodeConnections = DEFAULT_MAXIMUM_NODE_CONNECTIONS;
        int queueSize = DEFAULT_CONSTRUCTION_BEAM_WIDTH;
        VectorSimilarityFunction similarityFunction = DEFAULT_SIMILARITY_FUNCTION;

        if (options.get(POSTING_LIST_LVL_MIN_LEAVES) != null || options.get(POSTING_LIST_LVL_SKIP_OPTION) != null)
        {
            if (TypeUtil.isLiteral(type))
            {
                throw new InvalidRequestException(String.format("CQL type %s cannot have auxiliary posting lists on index %s.", type.asCQL3Type(), indexName));
            }

            for (Map.Entry<String, String> entry : options.entrySet())
            {
                switch (entry.getKey())
                {
                    case POSTING_LIST_LVL_MIN_LEAVES:
                    {
                        minLeaves = Integer.parseInt(entry.getValue());

                        if (minLeaves < 1)
                        {
                            throw new InvalidRequestException(String.format("Posting list min. leaves count can't be less than 1 on index %s.", indexName));
                        }

                        break;
                    }

                    case POSTING_LIST_LVL_SKIP_OPTION:
                    {
                        skip = Integer.parseInt(entry.getValue());

                        if (skip < 1)
                        {
                            throw new InvalidRequestException(String.format("Posting list skip can't be less than 1 on index %s.", indexName));
                        }

                        break;
                    }
                }
            }
        }
        else if (options.get(MAXIMUM_NODE_CONNECTIONS) != null ||
                 options.get(CONSTRUCTION_BEAM_WIDTH) != null ||
                 options.get(SIMILARITY_FUNCTION) != null)
        {
            if (!type.isVector())
                throw new InvalidRequestException(String.format("CQL type %s cannot have vector options", type.asCQL3Type()));

            if (options.containsKey(MAXIMUM_NODE_CONNECTIONS))
            {
                if (!CassandraRelevantProperties.SAI_HNSW_ALLOW_CUSTOM_PARAMETERS.getBoolean())
                    throw new InvalidRequestException(String.format("Maximum node connections cannot be set without enabling %s", CassandraRelevantProperties.SAI_HNSW_ALLOW_CUSTOM_PARAMETERS.name()));

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
                if (!CassandraRelevantProperties.SAI_HNSW_ALLOW_CUSTOM_PARAMETERS.getBoolean())
                    throw new InvalidRequestException(String.format("Construction beam width cannot be set without enabling %s", CassandraRelevantProperties.SAI_HNSW_ALLOW_CUSTOM_PARAMETERS.name()));

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
        }
        return new IndexWriterConfig(indexName, skip, minLeaves, maximumNodeConnections, queueSize, similarityFunction);
    }

    public static IndexWriterConfig defaultConfig(String indexName)
    {
        return new IndexWriterConfig(indexName,
                                     DEFAULT_POSTING_LIST_LVL_SKIP,
                                     DEFAULT_POSTING_LIST_MIN_LEAVES,
                                     DEFAULT_MAXIMUM_NODE_CONNECTIONS,
                                     DEFAULT_CONSTRUCTION_BEAM_WIDTH,
                                     DEFAULT_SIMILARITY_FUNCTION);
    }

    public static IndexWriterConfig emptyConfig()
    {
        return EMPTY_CONFIG;
    }

    @Override
    public String toString()
    {
        return String.format("IndexWriterConfig{%s=%d, %s=%d, %s=%d, %s=%d, %s=%s}",
                             POSTING_LIST_LVL_SKIP_OPTION, bkdPostingsSkip,
                             POSTING_LIST_LVL_MIN_LEAVES, bkdPostingsMinLeaves,
                             MAXIMUM_NODE_CONNECTIONS, maximumNodeConnections,
                             CONSTRUCTION_BEAM_WIDTH, constructionBeamWidth,
                             SIMILARITY_FUNCTION, similarityFunction);
    }
}
