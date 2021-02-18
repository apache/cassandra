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
package org.apache.cassandra.index.sai.disk;

import java.util.Map;

import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.index.sai.utils.TypeUtil;

/**
 * Per-index config for storage-attached index writers.
 */
public class IndexWriterConfig
{
    public static final String POSTING_LIST_LVL_MIN_LEAVES = "bkd_postings_min_leaves";
    public static final String POSTING_LIST_LVL_SKIP_OPTION = "bkd_postings_skip";

    private static final int DEFAULT_POSTING_LIST_MIN_LEAVES = 64;
    private static final int DEFAULT_POSTING_LIST_LVL_SKIP = 3;

    private static final IndexWriterConfig EMPTY_CONFIG = new IndexWriterConfig(null, -1, -1);

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

    public IndexWriterConfig(String indexName, int bkdPostingsSkip, int bkdPostingsMinLeaves)
    {
        this.indexName = indexName;
        this.bkdPostingsSkip = bkdPostingsSkip;
        this.bkdPostingsMinLeaves = bkdPostingsMinLeaves;
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

    public static IndexWriterConfig fromOptions(String indexName, AbstractType<?> type, Map<String, String> options)
    {
        int minLeaves = DEFAULT_POSTING_LIST_MIN_LEAVES;
        int skip = DEFAULT_POSTING_LIST_LVL_SKIP;

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

        return new IndexWriterConfig(indexName, skip, minLeaves);
    }

    public static IndexWriterConfig defaultConfig(String indexName)
    {
        return new IndexWriterConfig(indexName, DEFAULT_POSTING_LIST_LVL_SKIP, DEFAULT_POSTING_LIST_MIN_LEAVES);
    }

    public static IndexWriterConfig emptyConfig()
    {
        return EMPTY_CONFIG;
    }

    @Override
    public String toString()
    {
        return String.format("IndexWriterConfig{%s=%d, %s=%d}",
                             POSTING_LIST_LVL_SKIP_OPTION, bkdPostingsSkip,
                             POSTING_LIST_LVL_MIN_LEAVES, bkdPostingsMinLeaves);
    }
}
