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

package org.apache.cassandra.harry.ddl;

import java.util.*;
import java.util.function.Consumer;

import org.apache.cassandra.harry.gen.DataGenerators;
import org.apache.cassandra.harry.sut.SystemUnderTest;
import org.apache.cassandra.harry.operations.CompiledStatement;
import org.apache.cassandra.harry.operations.Relation;
import org.apache.cassandra.harry.util.BitSet;

public class SchemaSpec
{
    public interface SchemaSpecFactory
    {
        SchemaSpec make(long seed, SystemUnderTest sut);
    }

    public final DataGenerators.KeyGenerator pkGenerator;
    public final DataGenerators.KeyGenerator ckGenerator;

    private final boolean isCompactStorage;
    private final boolean disableReadRepair;
    private final String compactionStrategy;
    public final boolean trackLts;

    // These fields are immutable, and are safe as public
    public final String keyspace;
    public final String table;

    public final List<ColumnSpec<?>> partitionKeys;
    public final List<ColumnSpec<?>> clusteringKeys;
    public final List<ColumnSpec<?>> regularColumns;
    public final List<ColumnSpec<?>> staticColumns;
    public final List<ColumnSpec<?>> allColumns;
    public final Set<ColumnSpec<?>> allColumnsSet;

    public final BitSet ALL_COLUMNS_BITSET;
    public final int regularColumnsOffset;
    public final int staticColumnsOffset;
    public final BitSet regularColumnsMask;
    public final BitSet regularAndStaticColumnsMask;
    public final BitSet staticColumnsMask;

    public SchemaSpec(String keyspace,
                      String table,
                      List<ColumnSpec<?>> partitionKeys,
                      List<ColumnSpec<?>> clusteringKeys,
                      List<ColumnSpec<?>> regularColumns,
                      List<ColumnSpec<?>> staticColumns)
    {
        this(keyspace, table, partitionKeys, clusteringKeys, regularColumns, staticColumns, DataGenerators.createKeyGenerator(clusteringKeys), false, false, null, false);
    }

    public SchemaSpec cloneWithName(String ks,
                                    String table)
    {
        return new SchemaSpec(ks, table, partitionKeys, clusteringKeys, regularColumns, staticColumns, ckGenerator, isCompactStorage, disableReadRepair, compactionStrategy, trackLts);
    }

    public SchemaSpec trackLts()
    {
        return new SchemaSpec(keyspace, table, partitionKeys, clusteringKeys, regularColumns, staticColumns, ckGenerator, isCompactStorage, disableReadRepair, compactionStrategy, true);
    }

    public SchemaSpec withCompactStorage()
    {
        return new SchemaSpec(keyspace, table, partitionKeys, clusteringKeys, regularColumns, staticColumns, ckGenerator, true, disableReadRepair, compactionStrategy, trackLts);
    }

    public SchemaSpec withCompactionStrategy(String compactionStrategy)
    {
        return new SchemaSpec(keyspace, table, partitionKeys, clusteringKeys, regularColumns, staticColumns, ckGenerator, false, disableReadRepair, compactionStrategy, trackLts);
    }

    public SchemaSpec withCkGenerator(DataGenerators.KeyGenerator ckGeneratorOverride, List<ColumnSpec<?>> clusteringKeys)
    {
        return new SchemaSpec(keyspace, table, partitionKeys, clusteringKeys, regularColumns, staticColumns, ckGeneratorOverride, isCompactStorage, disableReadRepair, compactionStrategy, trackLts);
    }

    public SchemaSpec withColumns(List<ColumnSpec<?>> regularColumns, List<ColumnSpec<?>> staticColumns)
    {
        return new SchemaSpec(keyspace, table, partitionKeys, clusteringKeys, regularColumns, staticColumns, ckGenerator, isCompactStorage, disableReadRepair, compactionStrategy, trackLts);
    }

    public SchemaSpec(String keyspace,
                      String table,
                      List<ColumnSpec<?>> partitionKeys,
                      List<ColumnSpec<?>> clusteringKeys,
                      List<ColumnSpec<?>> regularColumns,
                      List<ColumnSpec<?>> staticColumns,
                      DataGenerators.KeyGenerator ckGenerator,
                      boolean isCompactStorage,
                      boolean disableReadRepair,
                      String compactionStrategy,
                      boolean trackLts)
    {
        assert !isCompactStorage || clusteringKeys.isEmpty() || regularColumns.size() <= 1 :
        String.format("Compact storage %s. Clustering keys: %d. Regular columns: %d", isCompactStorage, clusteringKeys.size(), regularColumns.size());

        this.keyspace = keyspace;
        this.table = table;
        this.isCompactStorage = isCompactStorage;
        this.disableReadRepair = disableReadRepair;
        this.compactionStrategy = compactionStrategy;

        this.partitionKeys = Collections.unmodifiableList(new ArrayList<>(partitionKeys));
        for (int i = 0; i < partitionKeys.size(); i++)
            partitionKeys.get(i).setColumnIndex(i);
        this.clusteringKeys = Collections.unmodifiableList(new ArrayList<>(clusteringKeys));
        for (int i = 0; i < clusteringKeys.size(); i++)
            clusteringKeys.get(i).setColumnIndex(i);
        this.staticColumns = Collections.unmodifiableList(new ArrayList<>(staticColumns));
        for (int i = 0; i < staticColumns.size(); i++)
            staticColumns.get(i).setColumnIndex(i);
        this.regularColumns = Collections.unmodifiableList(new ArrayList<>(regularColumns));
        for (int i = 0; i < regularColumns.size(); i++)
            regularColumns.get(i).setColumnIndex(i);

        List<ColumnSpec<?>> all = new ArrayList<>();
        for (ColumnSpec<?> columnSpec : concat(partitionKeys,
                                               clusteringKeys,
                                               staticColumns,
                                               regularColumns))
        {
            all.add(columnSpec);
        }
        this.allColumns = Collections.unmodifiableList(all);
        this.allColumnsSet = Collections.unmodifiableSet(new LinkedHashSet<>(all));

        this.pkGenerator = DataGenerators.createKeyGenerator(partitionKeys);
        if (ckGenerator == null)
            ckGenerator = DataGenerators.createKeyGenerator(clusteringKeys);
        this.ckGenerator = ckGenerator;

        this.ALL_COLUMNS_BITSET = BitSet.allSet(regularColumns.size());

        this.staticColumnsOffset = partitionKeys.size() + clusteringKeys.size();
        this.regularColumnsOffset = staticColumnsOffset + staticColumns.size();

        this.regularColumnsMask = regularColumnsMask(this);
        this.regularAndStaticColumnsMask = regularAndStaticColumnsMask(this);
        this.staticColumnsMask = staticColumnsMask(this);
        this.trackLts = trackLts;
    }



    public static BitSet allColumnsMask(SchemaSpec schema)
    {
        return BitSet.allSet(schema.allColumns.size());
    }

    public BitSet regularColumnsMask()
    {
        return this.regularColumnsMask;
    }

    public BitSet regularAndStaticColumnsMask()
    {
        return this.regularAndStaticColumnsMask;
    }

    public BitSet staticColumnsMask()
    {
        return this.staticColumnsMask;
    }

    private static BitSet regularColumnsMask(SchemaSpec schema)
    {
        BitSet mask = BitSet.allUnset(schema.allColumns.size());
        for (int i = 0; i < schema.regularColumns.size(); i++)
            mask.set(schema.regularColumnsOffset + i);
        return mask;
    }

    private static BitSet regularAndStaticColumnsMask(SchemaSpec schema)
    {
        BitSet mask = BitSet.allUnset(schema.allColumns.size());
        for (int i = 0; i < schema.staticColumns.size() + schema.regularColumns.size(); i++)
            mask.set(schema.staticColumnsOffset + i);
        return mask;
    }

    private static BitSet staticColumnsMask(SchemaSpec schema)
    {
        BitSet mask = BitSet.allUnset(schema.allColumns.size());
        for (int i = 0; i < schema.staticColumns.size(); i++)
            mask.set(schema.staticColumnsOffset + i);
        return mask;
    }

    public void validate()
    {
        assert pkGenerator.byteSize() == Long.BYTES : partitionKeys.toString();
    }

    public interface AddRelationCallback
    {
        void accept(ColumnSpec<?> spec, Relation.RelationKind kind, Object value);
    }

    public void inflateRelations(long pd,
                                 List<Relation> clusteringRelations,
                                 AddRelationCallback consumer)
    {
        Object[] pk = inflatePartitionKey(pd);
        for (int i = 0; i < pk.length; i++)
            consumer.accept(partitionKeys.get(i), Relation.RelationKind.EQ, pk[i]);

        inflateRelations(clusteringRelations, consumer);
    }

    public void inflateRelations(List<Relation> clusteringRelations,
                                 AddRelationCallback consumer)
    {
        for (Relation r : clusteringRelations)
            consumer.accept(r.columnSpec, r.kind, r.value());
    }

    public Object[] inflatePartitionKey(long pd)
    {
        return pkGenerator.inflate(pd);
    }

    public Object[] inflateClusteringKey(long cd)
    {
        return ckGenerator.inflate(cd);
    }

    public Object[] inflateRegularColumns(long[] vds)
    {
        return DataGenerators.inflateData(regularColumns, vds);
    }

    public Object[] inflateStaticColumns(long[] sds)
    {
        return DataGenerators.inflateData(staticColumns, sds);
    }

    public long adjustPdEntropy(long descriptor)
    {
        return pkGenerator.adjustEntropyDomain(descriptor);
    }

    public long adjustCdEntropy(long descriptor)
    {
        return ckGenerator.adjustEntropyDomain(descriptor);
    }

    public long deflatePartitionKey(Object[] pk)
    {
        return pkGenerator.deflate(pk);
    }

    public long deflateClusteringKey(Object[] ck)
    {
        return ckGenerator.deflate(ck);
    }

    public long[] deflateStaticColumns(Object[] statics)
    {
        return DataGenerators.deflateData(staticColumns, statics);
    }

    public long[] deflateRegularColumns(Object[] regulars)
    {
        return DataGenerators.deflateData(regularColumns, regulars);
    }

    public CompiledStatement compile()
    {
        StringBuilder sb = new StringBuilder();

        sb.append("CREATE TABLE IF NOT EXISTS ");
        sb.append(keyspace)
          .append(".")
          .append(table)
          .append(" (");

        SeparatorAppender commaAppender = new SeparatorAppender();
        for (ColumnSpec<?> cd : partitionKeys)
        {
            commaAppender.accept(sb);
            sb.append(cd.toCQL());
            if (partitionKeys.size() == 1 && clusteringKeys.size() == 0)
                sb.append(" PRIMARY KEY");
        }

        for (ColumnSpec<?> cd : concat(clusteringKeys,
                                       staticColumns,
                                       regularColumns))
        {
            commaAppender.accept(sb);
            sb.append(cd.toCQL());
        }

        if (clusteringKeys.size() > 0 || partitionKeys.size() > 1)
        {
            sb.append(", ").append(getPrimaryKeyCql());
        }

        if (trackLts)
            sb.append(", ").append("visited_lts list<bigint> static");

        sb.append(')');

        Runnable appendWith = doOnce(() -> sb.append(" WITH"));

        if (isCompactStorage)
        {
            appendWith.run();
            sb.append(" COMPACT STORAGE AND");
        }

        if (disableReadRepair)
        {
            appendWith.run();
            sb.append(" read_repair = 'NONE' AND");
        }

        if (compactionStrategy != null)
        {
            appendWith.run();
            sb.append(" compaction = {'class': '").append(compactionStrategy).append("'} AND");
        }

        if (clusteringKeys.size() > 0)
        {
            appendWith.run();
            sb.append(getClusteringOrderCql())
              .append(';');
        }

        return new CompiledStatement(sb.toString());
    }

    private String getClusteringOrderCql()
    {
        StringBuilder sb = new StringBuilder();
        if (clusteringKeys.size() > 0)
        {
            sb.append(" CLUSTERING ORDER BY (");

            SeparatorAppender commaAppender = new SeparatorAppender();
            for (ColumnSpec<?> column : clusteringKeys)
            {
                commaAppender.accept(sb);
                sb.append(column.name).append(' ').append(column.isReversed() ? "DESC" : "ASC");
            }

            sb.append(")");
        }

        return sb.toString();
    }

    private String getPrimaryKeyCql()
    {
        StringBuilder sb = new StringBuilder();
        sb.append("PRIMARY KEY (");
        if (partitionKeys.size() > 1)
        {
            sb.append('(');
            SeparatorAppender commaAppender = new SeparatorAppender();
            for (ColumnSpec<?> cd : partitionKeys)
            {
                commaAppender.accept(sb);
                sb.append(cd.name);
            }
            sb.append(')');
        }
        else
        {
            sb.append(partitionKeys.get(0).name);
        }

        for (ColumnSpec<?> cd : clusteringKeys)
            sb.append(", ").append(cd.name);

        return sb.append(')').toString();
    }

    public String toString()
    {
        return String.format("schema {cql=%s, columns=%s}", compile().toString(), allColumns);
    }

    private static Runnable doOnce(Runnable r)
    {
        return new Runnable()
        {
            boolean executed = false;

            public void run()
            {
                if (executed)
                    return;

                executed = true;
                r.run();
            }
        };
    }

    public static class SeparatorAppender implements Consumer<StringBuilder>
    {
        boolean isFirst = true;
        private final String separator;

        public SeparatorAppender()
        {
            this(",");
        }

        public SeparatorAppender(String separator)
        {
            this.separator = separator;
        }

        public void accept(StringBuilder stringBuilder)
        {
            if (isFirst)
                isFirst = false;
            else
                stringBuilder.append(separator);
        }

        public void accept(StringBuilder stringBuilder, String s)
        {
            accept(stringBuilder);
            stringBuilder.append(s);
        }


        public void reset()
        {
            isFirst = true;
        }
    }

    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SchemaSpec that = (SchemaSpec) o;
        return Objects.equals(keyspace, that.keyspace) &&
               Objects.equals(table, that.table) &&
               Objects.equals(partitionKeys, that.partitionKeys) &&
               Objects.equals(clusteringKeys, that.clusteringKeys) &&
               Objects.equals(regularColumns, that.regularColumns);
    }

    public int hashCode()
    {
        return Objects.hash(keyspace, table, partitionKeys, clusteringKeys, regularColumns);
    }

    public static <T> Iterable<T> concat(Iterable<T>... iterables)
    {
        assert iterables != null && iterables.length > 0;
        if (iterables.length == 1)
            return iterables[0];

        return () -> {
            return new Iterator<T>()
            {
                int idx;
                Iterator<T> current;
                boolean hasNext;

                {
                    idx = 0;
                    prepareNext();
                }

                private void prepareNext()
                {
                    if (current != null && current.hasNext())
                    {
                        hasNext = true;
                        return;
                    }

                    while (idx < iterables.length)
                    {
                        current = iterables[idx].iterator();
                        idx++;
                        if (current.hasNext())
                        {
                            hasNext = true;
                            return;
                        }
                    }

                    hasNext = false;
                }

                public boolean hasNext()
                {
                    return hasNext;
                }

                public T next()
                {
                    T next = current.next();
                    prepareNext();
                    return next;
                }
            };
        };
    }
}
