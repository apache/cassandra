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

package org.apache.cassandra.harry;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;

import org.apache.cassandra.cql3.ast.Symbol;
import org.apache.cassandra.harry.dsl.HistoryBuilder;
import org.apache.cassandra.harry.gen.Generator;
import org.apache.cassandra.harry.gen.Generators;
import org.apache.cassandra.harry.gen.ValueGenerators;
import org.apache.cassandra.harry.util.IteratorsUtil;
import org.apache.cassandra.service.consensus.TransactionalMode;
import org.apache.cassandra.utils.ByteArrayUtil;

import static org.apache.cassandra.harry.gen.InvertibleGenerator.MAX_ENTROPY;

public class SchemaSpec
{
    public final String keyspace;
    public final String table;

    public final List<ColumnSpec<?>> partitionKeys;
    public final List<ColumnSpec<?>> clusteringKeys;
    public final List<ColumnSpec<?>> regularColumns;
    public final List<ColumnSpec<?>> staticColumns;

    public final List<ColumnSpec<?>> allColumnInSelectOrder;
    public final ValueGenerators valueGenerators;
    public final Options options;

    public SchemaSpec(long seed,
                      int populationPerColumn,
                      String keyspace,
                      String table,
                      List<ColumnSpec<?>> partitionKeys,
                      List<ColumnSpec<?>> clusteringKeys,
                      List<ColumnSpec<?>> regularColumns,
                      List<ColumnSpec<?>> staticColumns)
    {
        this(seed, populationPerColumn, keyspace, table, partitionKeys, clusteringKeys, regularColumns, staticColumns, optionsBuilder());
    }

    @SuppressWarnings({ "unchecked" })
    public SchemaSpec(long seed,
                      int populationPerColumn,
                      String keyspace,
                      String table,
                      List<ColumnSpec<?>> partitionKeys,
                      List<ColumnSpec<?>> clusteringKeys,
                      List<ColumnSpec<?>> regularColumns,
                      List<ColumnSpec<?>> staticColumns,
                      Options options)
    {
        this.keyspace = keyspace;
        this.table = table;
        this.options = options;

        this.partitionKeys = Collections.unmodifiableList(new ArrayList<>(partitionKeys));
        this.clusteringKeys = Collections.unmodifiableList(new ArrayList<>(clusteringKeys));
        this.staticColumns = Collections.unmodifiableList(new ArrayList<>(staticColumns));
        this.regularColumns = Collections.unmodifiableList(new ArrayList<>(regularColumns));

        List<ColumnSpec<?>> staticSelectOrder = new ArrayList<>(staticColumns);
        staticSelectOrder.sort((s1, s2) -> ByteArrayUtil.compareUnsigned(s1.name.getBytes(), s2.name.getBytes()));
        List<ColumnSpec<?>> regularSelectOrder = new ArrayList<>(regularColumns);
        regularSelectOrder.sort((s1, s2) -> ByteArrayUtil.compareUnsigned(s1.name.getBytes(), s2.name.getBytes()));

        List<ColumnSpec<?>> selectOrder = new ArrayList<>();
        for (ColumnSpec<?> column : IteratorsUtil.concat(partitionKeys,
                                                         clusteringKeys,
                                                         staticSelectOrder,
                                                         regularSelectOrder))
            selectOrder.add(column);
        this.allColumnInSelectOrder = Collections.unmodifiableList(selectOrder);

        // TODO: empty gen
        this.valueGenerators = HistoryBuilder.valueGenerators(this, seed, populationPerColumn);
    }

    public static /* unsigned */ long cumulativeEntropy(List<ColumnSpec<?>> columns)
    {
        if (columns.isEmpty())
            return 0;

        long entropy = 1;
        for (ColumnSpec<?> column : columns)
        {
            if (Long.compareUnsigned(column.type.typeEntropy(), MAX_ENTROPY) == 0)
                return MAX_ENTROPY;

            long next = entropy * column.type.typeEntropy();
            if (Long.compareUnsigned(next, entropy) < 0 || Long.compareUnsigned(next, column.type.typeEntropy()) < 0)
                return MAX_ENTROPY;

            entropy = next;
        }

        return entropy;
    }

    public static Generator<Object[]> forKeys(List<ColumnSpec<?>> columns)
    {
        Generator<?>[] gens = new Generator[columns.size()];
        for (int i = 0; i < gens.length; i++)
            gens[i] = columns.get(i).gen;
        return Generators.zipArray(gens);
    }

    public String compile()
    {
        StringBuilder sb = new StringBuilder();

        sb.append("CREATE TABLE ");
        if (options.ifNotExists())
            sb.append("IF NOT EXISTS ");

        sb.append(Symbol.maybeQuote(keyspace))
          .append(".")
          .append(Symbol.maybeQuote(table))
          .append(" (");

        SeparatorAppender commaAppender = new SeparatorAppender();
        for (ColumnSpec<?> cd : partitionKeys)
        {
            commaAppender.accept(sb);
            sb.append(cd.toCQL());
            if (partitionKeys.size() == 1 && clusteringKeys.isEmpty())
                sb.append(" PRIMARY KEY");
        }

        for (ColumnSpec<?> cd : IteratorsUtil.concat(clusteringKeys,
                                                     staticColumns,
                                                     regularColumns))
        {
            commaAppender.accept(sb);
            sb.append(cd.toCQL());
        }

        if (!clusteringKeys.isEmpty() || partitionKeys.size() > 1)
        {
            sb.append(", ").append(getPrimaryKeyCql());
        }

        // TODO: test
        if (options.trackLts())
            sb.append(", ").append("visited_lts list<bigint> static");

        sb.append(')');

        Runnable appendWith = doOnce(() -> sb.append(" WITH"));
        boolean shouldAppendAnd = false;

        if (options.compactStorage())
        {
            appendWith.run();
            sb.append(" COMPACT STORAGE");
            shouldAppendAnd = true;
        }

        if (options.transactionalMode() != null)
        {
            appendWith.run();
            if (shouldAppendAnd)
                sb.append(" AND");
            sb.append(" ").append(options.transactionalMode().asCqlParam());
            shouldAppendAnd = true;
        }

        if (options.disableReadRepair())
        {
            appendWith.run();
            if (shouldAppendAnd)
                sb.append(" AND");
            sb.append(" read_repair = 'NONE' ");
            shouldAppendAnd = true;
        }

        if (options.compactionStrategy() != null)
        {
            appendWith.run();
            if (shouldAppendAnd)
                sb.append(" AND");
            sb.append(" compaction = {'class': '").append(options.compactionStrategy()).append("'}");
            shouldAppendAnd = true;
        }

        if (!clusteringKeys.isEmpty())
        {
            appendWith.run();
            if (shouldAppendAnd)
            {
                sb.append(" AND");
                shouldAppendAnd = false;
            }
            sb.append(getClusteringOrderCql());
        }

        if (shouldAppendAnd)
        {
            sb.append(" AND");
            shouldAppendAnd = false;
        }

        sb.append(';');
        return sb.toString();
    }

    private String getClusteringOrderCql()
    {
        StringBuilder sb = new StringBuilder();
        if (!clusteringKeys.isEmpty())
        {
            sb.append(" CLUSTERING ORDER BY (");

            SeparatorAppender commaAppender = new SeparatorAppender();
            for (ColumnSpec<?> column : clusteringKeys)
            {
                commaAppender.accept(sb);
                sb.append(column.name).append(' ').append(column.isReversed() ? "DESC" : "ASC");
            }

            sb.append(')');
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
        return String.format("schema {cql=%s}", compile());
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

    public interface Options
    {
        TransactionalMode transactionalMode();
        boolean addWriteTimestamps();
        boolean disableReadRepair();
        String compactionStrategy();
        boolean compactStorage();
        boolean ifNotExists();
        boolean trackLts();
    }

    public static OptionsBuilder optionsBuilder()
    {
        return new OptionsBuilder();
    }

    public static class OptionsBuilder implements Options
    {
        private TransactionalMode transactionalMode = null;
        private boolean addWriteTimestamps = true;
        private boolean disableReadRepair = false;
        private String compactionStrategy = null;
        private boolean ifNotExists = false;
        private boolean trackLts = false;
        private boolean compactStorage = false;

        private OptionsBuilder()
        {
        }

        public OptionsBuilder withTransactionalMode(TransactionalMode mode)
        {
            this.transactionalMode = mode;
            return this;
        }

        @Override
        public TransactionalMode transactionalMode()
        {
            return transactionalMode;
        }

        public OptionsBuilder addWriteTimestamps(boolean newValue)
        {
            this.addWriteTimestamps = newValue;
            return this;
        }

        @Override
        public boolean addWriteTimestamps()
        {
            return addWriteTimestamps;
        }

        public OptionsBuilder disableReadRepair(boolean newValue)
        {
            this.disableReadRepair = newValue;
            return this;
        }

        @Override
        public boolean disableReadRepair()
        {
            return disableReadRepair;
        }

        public OptionsBuilder compactionStrategy(String compactionStrategy)
        {
            this.compactionStrategy = compactionStrategy;
            return this;
        }

        @Override
        public String compactionStrategy()
        {
            return compactionStrategy;
        }

        public OptionsBuilder withCompactStorage()
        {
            this.compactStorage = true;
            return this;
        }

        @Override
        public boolean compactStorage()
        {
            return compactStorage;
        }

        public OptionsBuilder ifNotExists(boolean v)
        {
            this.ifNotExists = v;
            return this;
        }

        @Override
        public boolean ifNotExists()
        {
            return ifNotExists;
        }

        public OptionsBuilder trackLts(boolean v)
        {
            this.trackLts = v;
            return this;
        }

        @Override
        public boolean trackLts()
        {
            return trackLts;
        }
    }
}