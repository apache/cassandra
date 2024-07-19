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

import java.util.*;
import java.util.function.LongSupplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.harry.ddl.ColumnSpec;
import org.apache.cassandra.harry.ddl.SchemaSpec;
import org.apache.cassandra.harry.gen.DataGenerators;
import org.apache.cassandra.harry.gen.rng.RngUtils;
import org.apache.cassandra.harry.model.SelectHelper;
import org.apache.cassandra.harry.util.DescriptorRanges;

import static org.apache.cassandra.harry.operations.QueryGenerator.relationKind;
import static org.apache.cassandra.harry.operations.Relation.FORWARD_COMPARATOR;

/**
 * A class representing relations in the query, essentially what WHERE clause means.
 */
public abstract class Query
{
    private static final Logger logger = LoggerFactory.getLogger(Query.class);
    // TODO: There are queries without PD
    public final long pd;
    public final boolean reverse;
    public final List<Relation> relations;
    public final Map<String, List<Relation>> relationsMap;
    public final SchemaSpec schemaSpec;
    public final QueryKind queryKind;
    public final Selection selection;

    public Query(QueryKind kind, long pd, boolean reverse, List<Relation> relations, SchemaSpec schemaSpec)
    {
        this(kind, pd, reverse, relations, schemaSpec, new Columns(schemaSpec.allColumnsSet, true));
    }

    public Query(QueryKind kind, long pd, boolean reverse, List<Relation> relations, SchemaSpec schemaSpec, Selection selection)
    {
        this.queryKind = kind;
        this.pd = pd;
        this.reverse = reverse;
        this.relations = relations;
        this.relationsMap = new HashMap<>();
        for (Relation relation : relations)
            this.relationsMap.computeIfAbsent(relation.column(), column -> new ArrayList<>()).add(relation);
        this.schemaSpec = schemaSpec;
        this.selection = selection;
    }

    // TODO: pd, values, filtering?
    public boolean matchCd(long cd)
    {
        return simpleMatch(this, cd);
    }

    public static boolean simpleMatch(Query query,
                                      long cd)
    {
        long[] sliced = query.schemaSpec.ckGenerator.slice(cd);
        for (int i = 0; i < query.schemaSpec.clusteringKeys.size(); i++)
        {
            List<Relation> relations = query.relationsMap.get(query.schemaSpec.clusteringKeys.get(i).name);
            if (relations == null)
                continue;

            for (Relation r : relations)
            {
                if (!r.match(sliced[i]))
                    return false;
            }
        }

        return true;
    }

    public static class SinglePartitionQuery extends Query
    {
        public SinglePartitionQuery(QueryKind kind, long pd, boolean reverse, List<Relation> allRelations, SchemaSpec schemaSpec, Selection selection)
        {
            super(kind, pd, reverse, allRelations, schemaSpec, selection);
        }

        public boolean matchCd(long cd)
        {
            return true;
        }

        public DescriptorRanges.DescriptorRange toRange(long ts)
        {
            return new DescriptorRanges.DescriptorRange(Long.MIN_VALUE, Long.MAX_VALUE, true, true, ts);
        }

        public String toString()
        {
            return "SinglePartitionQuery{" +
                   "pd=" + pd +
                   ", reverse=" + reverse +
                   ", relations=" + relations +
                   ", relationsMap=" + relationsMap +
                   ", schemaSpec=" + schemaSpec +
                   '}';
        }
    }

    public static class SingleClusteringQuery extends Query
    {
        private final long cd;

        public SingleClusteringQuery(QueryKind kind, long pd, long cd, boolean reverse, List<Relation> allRelations, SchemaSpec schemaSpec)
        {
            super(kind, pd, reverse, allRelations, schemaSpec);
            this.cd = cd;
        }

        public DescriptorRanges.DescriptorRange toRange(long ts)
        {
            return new DescriptorRanges.DescriptorRange(cd, cd, true, true, ts);
        }

        @Override
        public boolean matchCd(long cd)
        {
            return cd == this.cd;
        }
    }

    public static class ClusteringSliceQuery extends ClusteringRangeQuery
    {
        public ClusteringSliceQuery(QueryKind kind,
                                    long pd,
                                    long cdMin,
                                    long cdMax,
                                    Relation.RelationKind minRelation,
                                    Relation.RelationKind maxRelation,
                                    boolean reverse,
                                    List<Relation> allRelations,
                                    SchemaSpec schemaSpec)
        {
            super(kind, pd, cdMin, cdMax, minRelation, maxRelation, reverse, allRelations, schemaSpec);
        }

        public String toString()
        {
            return "ClusteringSliceQuery{" +
                   "\n" + toSelectStatement() +
                   "\npd=" + pd +
                   "\nreverse=" + reverse +
                   "\nrelations=" + relations +
                   "\nrelationsMap=" + relationsMap +
                   "\nschemaSpec=" + schemaSpec +
                   "\nqueryKind=" + queryKind +
                   "\ncdMin=" + cdMin +
                   "(" + Arrays.toString(schemaSpec.ckGenerator.slice(cdMin)) + ")" +
                   "\ncdMax=" + cdMax +
                   "(" + Arrays.toString(schemaSpec.ckGenerator.slice(cdMax)) + ")" +
                   "\nminRelation=" + minRelation +
                   "\nmaxRelation=" + maxRelation +
                   '}' + "\n" + toSelectStatement().cql();
        }
    }

    public static class ClusteringRangeQuery extends Query
    {
        protected final long cdMin;
        protected final long cdMax;
        protected final Relation.RelationKind minRelation;
        protected final Relation.RelationKind maxRelation;

        public ClusteringRangeQuery(QueryKind kind,
                                    long pd,
                                    long cdMin,
                                    long cdMax,
                                    Relation.RelationKind minRelation,
                                    Relation.RelationKind maxRelation,
                                    boolean reverse,
                                    List<Relation> allRelations,
                                    SchemaSpec schemaSpec)
        {
            super(kind, pd, reverse, allRelations, schemaSpec);
            this.cdMin = cdMin;
            this.cdMax = cdMax;
            this.minRelation = minRelation;
            this.maxRelation = maxRelation;
        }

        public DescriptorRanges.DescriptorRange toRange(long ts)
        {
            return new DescriptorRanges.DescriptorRange(cdMin, cdMax, minRelation.isInclusive(), maxRelation.isInclusive(), ts);
        }

        public boolean matchCd(long cd)
        {
            // TODO: looks like we don't really need comparator here.
            Relation.LongComparator cmp = FORWARD_COMPARATOR;
            boolean res = minRelation.match(cmp, cd, cdMin) && maxRelation.match(cmp, cd, cdMax);
            if (!logger.isDebugEnabled())
                return res;
            boolean superRes = super.matchCd(cd);
            if (res != superRes)
            {
                logger.debug("Query did not pass a sanity check.\n{}\n Super call returned: {}, while current call: {}\n" +
                             "cd         = {} {} ({})\n" +
                             "this.cdMin = {} {} ({})\n" +
                             "this.cdMax = {} {} ({})\n" +
                             "minRelation.match(cmp, cd, this.cdMin) = {}\n" +
                             "maxRelation.match(cmp, cd, this.cdMax) = {}\n",
                             this,
                             superRes, res,
                             cd, Long.toHexString(cd), Arrays.toString(schemaSpec.ckGenerator.slice(cd)),
                             cdMin, Long.toHexString(cdMin), Arrays.toString(schemaSpec.ckGenerator.slice(cdMin)),
                             cdMax, Long.toHexString(cdMax), Arrays.toString(schemaSpec.ckGenerator.slice(cdMax)),
                             minRelation.match(cmp, cd, cdMin),
                             maxRelation.match(cmp, cd, cdMax));
            }
            return res;
        }

        public String toString()
        {
            return "ClusteringRangeQuery{" +
                   "pd=" + pd +
                   ", reverse=" + reverse +
                   ", relations=" + relations +
                   ", relationsMap=" + relationsMap +
                   ", schemaSpec=" + schemaSpec +
                   ", cdMin=" + cdMin +
                   ", cdMax=" + cdMax +
                   ", minRelation=" + minRelation +
                   ", maxRelation=" + maxRelation +
                   '}';
        }
    }

    public CompiledStatement toSelectStatement()
    {
        return SelectHelper.select(schemaSpec, pd, selection.columns(), relations, reverse, selection.includeTimestamp());
    }

    public CompiledStatement toSelectStatement(boolean includeWriteTime)
    {
        return SelectHelper.select(schemaSpec, pd, selection.columns(), relations, reverse, includeWriteTime);
    }

    public CompiledStatement toSelectStatement(Set<ColumnSpec<?>> columns, boolean includeWriteTime)
    {
        return SelectHelper.select(schemaSpec, pd, columns, relations, reverse, includeWriteTime);
    }

    public CompiledStatement toDeleteStatement(long rts)
    {
        return DeleteHelper.delete(schemaSpec, pd, relations, null, null, rts);
    }

    public abstract DescriptorRanges.DescriptorRange toRange(long ts);

    public static Query selectAllColumns(SchemaSpec schemaSpec, long pd, boolean reverse)
    {
        return selectPartition(schemaSpec, pd, reverse, new Columns(schemaSpec.allColumnsSet, true));
    }

    public static Query selectAllColumnsWildcard(SchemaSpec schemaSpec, long pd, boolean reverse)
    {
        return selectPartition(schemaSpec, pd, reverse, Wildcard.instance);
    }

    public static Query selectPartition(SchemaSpec schemaSpec, long pd, boolean reverse, Selection selection)
    {
        return new Query.SinglePartitionQuery(Query.QueryKind.SINGLE_PARTITION,
                                              pd,
                                              reverse,
                                              Collections.emptyList(),
                                              schemaSpec,
                                              selection);
    }

    public static Query singleClustering(SchemaSpec schema, long pd, long cd, boolean reverse)
    {
        return new Query.SingleClusteringQuery(Query.QueryKind.SINGLE_CLUSTERING,
                                               pd,
                                               cd,
                                               reverse,
                                               Relation.eqRelations(schema.ckGenerator.slice(cd), schema.clusteringKeys),
                                               schema);
    }

    public static Query clusteringSliceQuery(SchemaSpec schema, long pd, long cd, long queryDescriptor, boolean isGt, boolean isEquals, boolean reverse)
    {
        List<Relation> relations = new ArrayList<>();

        long[] sliced = schema.ckGenerator.slice(cd);
        long min;
        long max;
        int nonEqFrom = RngUtils.asInt(queryDescriptor, 0, sliced.length - 1);

        long[] minBound = new long[sliced.length];
        long[] maxBound = new long[sliced.length];

        // Algorithm that determines boundaries for a clustering slice.
        //
        // Basic principles are not hard but there are a few edge cases. I haven't figured out how to simplify
        // those, so there might be some room for improvement. In short, what we want to achieve is:
        //
        // 1. Every part that is restricted with an EQ relation goes into the bound verbatim.
        // 2. Every part that is restricted with a non-EQ relation (LT, GT, LTE, GTE) is taken into the bound
        //    if it is required to satisfy the relationship. For example, in `ck1 = 0 AND ck2 < 5`, ck2 will go
        //    to the _max_ boundary, and minimum value will go to the _min_ boundary, since we can select every
        //    descriptor that is prefixed with ck1.
        // 3. Every other part (e.g., ones that are not explicitly mentioned in the query) has to be restricted
        //    according to equality. For example, in `ck1 = 0 AND ck2 < 5`, ck3 that is present in schema but not
        //    mentioned in query, makes sure that any value between [0, min_value, min_value] and [0, 5, min_value]
        //    is matched.
        //
        // One edge case is a query on the first clustering key: `ck1 < 5`. In this case, we have to fixup the lower
        // value to the minimum possible value. We could really just do Long.MIN_VALUE, but in case we forget to
        // adjust entropy elsewhere, it'll be caught correctly here.
        for (int i = 0; i < sliced.length; i++)
        {
            long v = sliced[i];
            DataGenerators.KeyGenerator gen = schema.ckGenerator;
            ColumnSpec column = schema.clusteringKeys.get(i);
            int idx = i;
            LongSupplier maxSupplier = () -> gen.maxValue(idx);
            LongSupplier minSupplier = () -> gen.minValue(idx);

            if (i < nonEqFrom)
            {
                relations.add(Relation.eqRelation(schema.clusteringKeys.get(i), v));
                minBound[i] = v;
                maxBound[i] = v;
            }
            else if (i == nonEqFrom)
            {
                relations.add(Relation.relation(relationKind(isGt, isEquals), schema.clusteringKeys.get(i), v));

                if (column.isReversed())
                {
                    minBound[i] = isGt ? minSupplier.getAsLong() : v;
                    maxBound[i] = isGt ? v : maxSupplier.getAsLong();
                }
                else
                {
                    minBound[i] = isGt ? v : minSupplier.getAsLong();
                    maxBound[i] = isGt ? maxSupplier.getAsLong() : v;
                }
            }
            else
            {
                if (isEquals)
                {
                    minBound[i] = minSupplier.getAsLong();
                    maxBound[i] = maxSupplier.getAsLong();
                }
                // If we have a non-eq case, all subsequent bounds have to correspond to the maximum in normal case,
                // or minimum in case the last bound locked with a relation was reversed.
                //
                // For example, if we have (ck1, ck2, ck3) as (ASC, DESC, ASC), and query ck1 > X, we'll have:
                //  [xxxxx | max_value | max_value]
                //    ck1       ck2         ck3
                // which will exclude xxxx, but take every possible (ck1 > xxxxx) prefixed value.
                //
                // Similarly, if we have (ck1, ck2, ck3) as (ASC, DESC, ASC), and query ck1 <= X, we'll have:
                //  [xxxxx | max_value | max_value]
                // which will include every (ck1 < xxxxx), and any clustering prefixed with xxxxx.
                else if (schema.clusteringKeys.get(nonEqFrom).isReversed())
                    maxBound[i] = minBound[i] = isGt ? minSupplier.getAsLong() : maxSupplier.getAsLong();
                else
                    maxBound[i] = minBound[i] = isGt ? maxSupplier.getAsLong() : minSupplier.getAsLong();
            }
        }

        if (schema.clusteringKeys.get(nonEqFrom).isReversed())
            isGt = !isGt;

        min = schema.ckGenerator.stitch(minBound);
        max = schema.ckGenerator.stitch(maxBound);

        if (nonEqFrom == 0)
        {
            min = isGt ? min : schema.ckGenerator.minValue();
            max = !isGt ? max : schema.ckGenerator.maxValue();
        }

        // if we're about to create an "impossible" query, just bump the modifier and re-generate
        if (min == max && !isEquals)
            throw new IllegalArgumentException("Impossible Query");

        return new Query.ClusteringSliceQuery(Query.QueryKind.CLUSTERING_SLICE,
                                              pd,
                                              min,
                                              max,
                                              relationKind(true, isGt ? isEquals : true),
                                              relationKind(false, !isGt ? isEquals : true),
                                              reverse,
                                              relations,
                                              schema);
    }

    public static Query clusteringRangeQuery(SchemaSpec schema, long pd, long cd1, long cd2, long queryDescriptor, boolean isMinEq, boolean isMaxEq, boolean reverse)
    {
        List<Relation> relations = new ArrayList<>();

        long[] minBound = schema.ckGenerator.slice(cd1);
        long[] maxBound = schema.ckGenerator.slice(cd2);

        int nonEqFrom = RngUtils.asInt(queryDescriptor, 0, schema.clusteringKeys.size() - 1);

        // Logic here is similar to how clustering slices are implemented, except for both lower and upper bound
        // get their values from sliced value in (1) and (2) cases:
        //
        // 1. Every part that is restricted with an EQ relation, takes its value from the min bound.
        //    TODO: this can actually be improved, since in case of hierarchical clustering generation we can
        //          pick out of the keys that are already locked. That said, we'll exercise more cases the way
        //          it is implemented right now.
        // 2. Every part that is restricted with a non-EQ relation is taken into the bound, if it is used in
        //    the query. For example in, `ck1 = 0 AND ck2 > 2 AND ck2 < 5`, ck2 values 2 and 5 will be placed,
        //    correspondingly, to the min and max bound.
        // 3. Every other part has to be restricted according to equality. Similar to clustering slice, we have
        //    to decide whether we use a min or the max value for the bound. Foe example `ck1 = 0 AND ck2 > 2 AND ck2 <= 5`,
        //    assuming we have ck3 that is present in schema but not mentioned in the query, we'll have bounds
        //    created as follows: [0, 2, max_value] and [0, 5, max_value]. Idea here is that since ck2 = 2 is excluded,
        //    we also disallow all ck3 values for [0, 2] prefix. Similarly, since ck2 = 5 is included, we allow every
        //    ck3 value with a prefix of [0, 5].
        for (int i = 0; i < schema.clusteringKeys.size(); i++)
        {
            ColumnSpec<?> col = schema.clusteringKeys.get(i);
            if (i < nonEqFrom)
            {
                relations.add(Relation.eqRelation(col, minBound[i]));
                maxBound[i] = minBound[i];
            }
            else if (i == nonEqFrom)
            {
                long minLocked = Math.min(minBound[nonEqFrom], maxBound[nonEqFrom]);
                long maxLocked = Math.max(minBound[nonEqFrom], maxBound[nonEqFrom]);
                relations.add(Relation.relation(relationKind(true, col.isReversed() ? isMaxEq : isMinEq), col,
                                                col.isReversed() ? maxLocked : minLocked));
                relations.add(Relation.relation(relationKind(false, col.isReversed() ? isMinEq : isMaxEq), col,
                                                col.isReversed() ? minLocked : maxLocked));
                minBound[i] = minLocked;
                maxBound[i] = maxLocked;
            }
            else
            {
                minBound[i] = isMinEq ? schema.ckGenerator.minValue(i) : schema.ckGenerator.maxValue(i);
                maxBound[i] = isMaxEq ? schema.ckGenerator.maxValue(i) : schema.ckGenerator.minValue(i);
            }
        }

        long stitchedMin = schema.ckGenerator.stitch(minBound);
        long stitchedMax = schema.ckGenerator.stitch(maxBound);

        // TODO: one of the ways to get rid of garbage here, and potentially even simplify the code is to
        //       simply return bounds here. After bounds are created, we slice them and generate query right
        //       from the bounds. In this case, we can even say that things like -inf/+inf are special values,
        //       and use them as placeholders. Also, it'll be easier to manipulate relations.
        return new Query.ClusteringRangeQuery(Query.QueryKind.CLUSTERING_RANGE,
                                              pd,
                                              stitchedMin,
                                              stitchedMax,
                                              relationKind(true, isMinEq),
                                              relationKind(false, isMaxEq),
                                              reverse,
                                              relations,
                                              schema);
    }

    public enum QueryKind
    {
        SINGLE_PARTITION,
        SINGLE_CLUSTERING,
        // Not sure if that's the best way to name these, but the difference is as follows:
        //
        // For a single clustering, clustering slice is essentially [x; ∞) or (-∞; x].
        //
        // However, in case of multiple clusterings, such as (ck1, ck2, ck3), can result into a range query with locked prefix:
        //    ck1 = x and ck2 = y and ck3 > z
        //
        // This would translate into bounds such as:
        //    ( (x, y, z); (x, y, max_ck3) )
        //
        // Logic here is that when we're "locking" x and y, and allow third slice to be in the range [z; ∞).
        // Implementation is a bit more involved than that, since we have to make sure it works for all clustering sizes
        // and for reversed type.
        CLUSTERING_SLICE,
        // For a single clustering, clustering slice is essentially [x; y].
        //
        // For multiple-clusterings case, for example (ck1, ck2, ck3), we select how many clusterings we "lock" with EQ
        // relation, and do a range slice for the rest. For example:
        //    ck1 = x and ck2 = y and ck3 > z1 and ck3 < z2
        //
        // Such queries only make sense if written partition actually has clusterings that have intersecting parts.
        CLUSTERING_RANGE
    }

    public interface Selection
    {
        Set<ColumnSpec<?>> columns();
        boolean includeTimestamp();
    }

    public static class Wildcard implements Selection
    {
        public static final Wildcard instance = new Wildcard();

        public Set<ColumnSpec<?>> columns()
        {
            return null;
        }

        public boolean includeTimestamp()
        {
            return false;
        }
    }

    public static class Columns implements Selection
    {
        private Set<ColumnSpec<?>> columns;
        private boolean includeTimestamp;

        public Columns(Set<ColumnSpec<?>> columns, boolean includeTimestamp)
        {
            this.columns = columns;
            this.includeTimestamp = includeTimestamp;
        }

        public Set<ColumnSpec<?>> columns()
        {
            return columns;
        }

        public boolean includeTimestamp()
        {
            return includeTimestamp;
        }
    }
}