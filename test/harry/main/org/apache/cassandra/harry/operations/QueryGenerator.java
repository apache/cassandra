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

package org.apache.cassandra.harry.operations;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.harry.core.Run;
import org.apache.cassandra.harry.ddl.SchemaSpec;
import org.apache.cassandra.harry.gen.rng.RngUtils;
import org.apache.cassandra.harry.gen.Surjections;
import org.apache.cassandra.harry.model.OpSelectors;

// TODO: there's a lot of potential to reduce an amount of garbage here.
// TODO: refactor. Currently, this class is a base for both SELECT and DELETE statements. In retrospect,
//       a better way to do the same thing would've been to just inflate bounds, be able to inflate
//       any type of query from the bounds, and leave things like "reverse" up to the last mile / implementation.
public class QueryGenerator
{
    private static final Logger logger = LoggerFactory.getLogger(QueryGenerator.class);

    private static final long GT_STREAM = 0b1;
    private static final long E_STREAM = 0b10;

    private final OpSelectors.PureRng rng;
    private final OpSelectors.PdSelector pdSelector;
    private final OpSelectors.DescriptorSelector descriptorSelector;
    private final SchemaSpec schema;

    public QueryGenerator(Run run)
    {
        this(run.schemaSpec, run.pdSelector, run.descriptorSelector, run.rng);
    }

    public QueryGenerator(SchemaSpec schema,
                          OpSelectors.PdSelector pdSelector,
                          OpSelectors.DescriptorSelector descriptorSelector,
                          OpSelectors.PureRng rng)
    {
        this.pdSelector = pdSelector;
        this.descriptorSelector = descriptorSelector;
        this.schema = schema;
        this.rng = rng;
    }

    public static class TypedQueryGenerator
    {
        private final OpSelectors.PureRng rng;
        private final QueryGenerator queryGenerator;
        private final Surjections.Surjection<Query.QueryKind> queryKindGen;

        public TypedQueryGenerator(Run run)
        {
            this(run.rng, new QueryGenerator(run));
        }

        public TypedQueryGenerator(OpSelectors.PureRng rng,
                                   QueryGenerator queryGenerator)
        {
            this(rng, Surjections.enumValues(Query.QueryKind.class), queryGenerator);
        }

        public TypedQueryGenerator(OpSelectors.PureRng rng,
                                   Surjections.Surjection<Query.QueryKind> queryKindGen,
                                   QueryGenerator queryGenerator)
        {
            this.rng = rng;
            this.queryGenerator = queryGenerator;
            this.queryKindGen = queryKindGen;
        }

        // Queries are inflated from LTS, which identifies the partition, and i, a modifier for the query to
        // be able to generate different queries for the same lts.
        public Query inflate(long lts, long modifier)
        {
            long descriptor = rng.next(modifier, lts);
            Query.QueryKind queryKind = queryKindGen.inflate(descriptor);
            return queryGenerator.inflate(lts, modifier, queryKind);
        }
    }

    public Query inflate(long lts, long modifier, Query.QueryKind queryKind)
    {
        long pd = pdSelector.pd(lts, schema);
        long queryDescriptor = rng.next(modifier, lts);
        boolean reverse = queryDescriptor % 2 == 0;
        switch (queryKind)
        {
            case SINGLE_PARTITION:
                return singlePartition(pd, reverse);
            case SINGLE_CLUSTERING:
            {
                long cd = descriptorSelector.randomCd(pd, queryDescriptor, schema);
                return singleClustering(pd, cd, reverse);
            }
            case CLUSTERING_SLICE:
            {
                long cd = descriptorSelector.randomCd(pd, queryDescriptor, schema);
                try
                {
                    return clusteringSliceQuery(pd, cd, queryDescriptor, reverse);
                }
                catch (IllegalArgumentException retry)
                {
                    return inflate(lts, modifier + 1, queryKind);
                }
            }
            case CLUSTERING_RANGE:
            {
                try
                {
                    long cd1 = descriptorSelector.randomCd(pd, queryDescriptor, schema);
                    long cd2 = descriptorSelector.randomCd(pd, rng.next(queryDescriptor, lts), schema);
                    return clusteringRangeQuery(pd, cd1, cd2, queryDescriptor, reverse);
                }
                catch (IllegalArgumentException retry)
                {
                    return inflate(lts, modifier + 1, queryKind);
                }
            }
            default:
                throw new IllegalArgumentException("Shouldn't happen");
        }
    }

    public Query singlePartition(long pd, boolean reverse)
    {
        return Query.selectAllColumns(schema, pd, reverse);
    }

    public Query singleClustering(long pd, long cd, boolean reverse)
    {
        return Query.singleClustering(schema, pd, cd, reverse);
    }

    public Query clusteringSliceQuery(long pd, long cd, long queryDescriptor, boolean reverse)
    {
        boolean isGt = RngUtils.asBoolean(rng.next(queryDescriptor, GT_STREAM));
        // TODO: make generation of EQ configurable; turn it off and on
        boolean isEquals = RngUtils.asBoolean(rng.next(queryDescriptor, E_STREAM));

        return Query.clusteringSliceQuery(schema, pd, cd, queryDescriptor, isGt, isEquals, reverse);
    }

    public Query clusteringRangeQuery(long pd, long cd1, long cd2, long queryDescriptor, boolean reverse)
    {
        boolean isMinEq = RngUtils.asBoolean(queryDescriptor);
        boolean isMaxEq = RngUtils.asBoolean(rng.next(queryDescriptor, pd));

        return Query.clusteringRangeQuery(schema, pd, cd1, cd2, queryDescriptor, isMinEq, isMaxEq, reverse);
    }

    public static Relation.RelationKind relationKind(boolean isGt, boolean isEquals)
    {
        if (isGt)
            return isEquals ? Relation.RelationKind.GTE : Relation.RelationKind.GT;
        else
            return isEquals ? Relation.RelationKind.LTE : Relation.RelationKind.LT;
    }
}
