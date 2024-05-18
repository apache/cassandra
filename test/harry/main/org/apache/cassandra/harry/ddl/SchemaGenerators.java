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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.function.Supplier;

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.harry.gen.Generator;
import org.apache.cassandra.harry.gen.Surjections;

public class SchemaGenerators
{
    private final static long SCHEMAGEN_STREAM_ID = 0x6264593273L;

    public static Builder schema(String ks)
    {
        return new Builder(ks);
    }

    public static final Map<String, ColumnSpec.DataType<?>> nameToTypeMap;
    public static final Collection<ColumnSpec.DataType<?>> columnTypes;
    public static final Collection<ColumnSpec.DataType<?>> partitionKeyTypes;
    public static final Collection<ColumnSpec.DataType<?>> clusteringKeyTypes;

    static
    {
        partitionKeyTypes = Collections.unmodifiableList(Arrays.asList(ColumnSpec.int8Type,
                                                                       ColumnSpec.int16Type,
                                                                       ColumnSpec.int32Type,
                                                                       ColumnSpec.int64Type,
                                                                       ColumnSpec.floatType,
                                                                       ColumnSpec.doubleType,
                                                                       ColumnSpec.asciiType,
                                                                       ColumnSpec.textType));

        columnTypes = Collections.unmodifiableList(Arrays.asList(ColumnSpec.int8Type,
                                                                 ColumnSpec.int16Type,
                                                                 ColumnSpec.int32Type,
                                                                 ColumnSpec.int64Type,
                                                                 ColumnSpec.floatType,
                                                                 ColumnSpec.doubleType,
                                                                 ColumnSpec.asciiType,
                                                                 ColumnSpec.textType));


        List<ColumnSpec.DataType<?>> builder = new ArrayList<>(partitionKeyTypes);
        Map<String, ColumnSpec.DataType<?>> mapBuilder = new HashMap<>();

        for (ColumnSpec.DataType<?> columnType : partitionKeyTypes)
        {
            ColumnSpec.DataType<?> reversedType = ColumnSpec.ReversedType.getInstance(columnType);
            builder.add(reversedType);

            mapBuilder.put(columnType.nameForParser(), columnType);
            mapBuilder.put(String.format("desc(%s)", columnType.nameForParser()), columnType);
        }

        builder.add(ColumnSpec.floatType);
        builder.add(ColumnSpec.doubleType);

        clusteringKeyTypes = Collections.unmodifiableList(builder);
        nameToTypeMap = Collections.unmodifiableMap(mapBuilder);
    }

    @SuppressWarnings("unchecked")
    public static <T> Generator<T> fromValues(Collection<T> allValues)
    {
        return fromValues((T[]) allValues.toArray());
    }

    public static <T> Generator<T> fromValues(T[] allValues)
    {
        return (rng) -> {
            return allValues[rng.nextInt(allValues.length - 1)];
        };
    }

    @SuppressWarnings("unchecked")
    public static Generator<ColumnSpec<?>> columnSpecGenerator(String prefix, ColumnSpec.Kind kind)
    {
        return fromValues(columnTypes)
               .map(new Function<ColumnSpec.DataType<?>, ColumnSpec<?>>()
               {
                   private int counter = 0;

                   public ColumnSpec<?> apply(ColumnSpec.DataType<?> type)
                   {
                       return new ColumnSpec<>(prefix + (counter++),
                                               type,
                                               kind);
                   }
               });
    }

    @SuppressWarnings("unchecked")
    public static Generator<ColumnSpec<?>> columnSpecGenerator(Collection<ColumnSpec.DataType<?>> columnTypes, String prefix, ColumnSpec.Kind kind)
    {
        return fromValues(columnTypes)
               .map(new Function<ColumnSpec.DataType<?>, ColumnSpec<?>>()
               {
                   private int counter = 0;

                   public ColumnSpec<?> apply(ColumnSpec.DataType<?> type)
                   {
                       return new ColumnSpec<>(String.format("%s%04d", prefix, counter++),
                                               type,
                                               kind);
                   }
               });
    }

    @SuppressWarnings("unchecked")
    public static Generator<ColumnSpec<?>> clusteringColumnSpecGenerator(String prefix)
    {
        return fromValues(clusteringKeyTypes)
               .map(new Function<ColumnSpec.DataType<?>, ColumnSpec<?>>()
               {
                   private int counter = 0;

                   public ColumnSpec<?> apply(ColumnSpec.DataType<?> type)
                   {
                       return ColumnSpec.ck(String.format("%s%04d", prefix, counter++), type);
                   }
               });
    }

    @SuppressWarnings("unchecked")
    public static Generator<ColumnSpec<?>> partitionColumnSpecGenerator(String prefix)
    {
        return fromValues(partitionKeyTypes)
               .map(new Function<ColumnSpec.DataType<?>, ColumnSpec<?>>()
               {
                   private int counter = 0;

                   public ColumnSpec<?> apply(ColumnSpec.DataType<?> type)
                   {

                       return ColumnSpec.pk(String.format("%s%04d", prefix, counter++),
                                            type);
                   }
               });
    }

    private static AtomicInteger tableCounter = new AtomicInteger(1);

    public static class Builder
    {
        private final String keyspace;
        private final Supplier<String> tableNameSupplier;

        private Generator<ColumnSpec<?>> pkGenerator = partitionColumnSpecGenerator("pk");
        private Generator<ColumnSpec<?>> ckGenerator = clusteringColumnSpecGenerator("ck");
        private Generator<ColumnSpec<?>> regularGenerator = columnSpecGenerator("regular", ColumnSpec.Kind.REGULAR);
        private Generator<ColumnSpec<?>> staticGenerator = columnSpecGenerator("static", ColumnSpec.Kind.STATIC);

        private int minPks = 1;
        private int maxPks = 1;
        private int minCks = 0;
        private int maxCks = 0;
        private int minRegular = 0;
        private int maxRegular = 0;
        private int minStatic = 0;
        private int maxStatic = 0;

        public Builder(String keyspace)
        {
            this(keyspace, () -> "table_" + tableCounter.getAndIncrement());
        }

        public Builder(String keyspace, Supplier<String> tableNameSupplier)
        {
            this.keyspace = keyspace;
            this.tableNameSupplier = tableNameSupplier;
        }

        public Builder partitionKeyColumnCount(int numCols)
        {
            return partitionKeyColumnCount(numCols, numCols);
        }

        public Builder partitionKeyColumnCount(int minCols, int maxCols)
        {
            this.minPks = minCols;
            this.maxPks = maxCols;
            return this;
        }

        public Builder partitionKeySpec(int minCols, int maxCols, ColumnSpec.DataType<?>... columnTypes)
        {
            return partitionKeySpec(minCols, maxCols, Arrays.asList(columnTypes));
        }

        public Builder partitionKeySpec(int minCols, int maxCols, Collection<ColumnSpec.DataType<?>> columnTypes)
        {
            this.minPks = minCols;
            this.maxPks = maxCols;
            this.pkGenerator = columnSpecGenerator(columnTypes, "pk", ColumnSpec.Kind.PARTITION_KEY);
            return this;
        }

        public Builder clusteringColumnCount(int numCols)
        {
            return clusteringColumnCount(numCols, numCols);
        }

        public Builder clusteringColumnCount(int minCols, int maxCols)
        {
            this.minCks = minCols;
            this.maxCks = maxCols;
            return this;
        }

        public Builder clusteringKeySpec(int minCols, int maxCols, ColumnSpec.DataType<?>... columnTypes)
        {
            return clusteringKeySpec(minCols, maxCols, Arrays.asList(columnTypes));
        }

        public Builder clusteringKeySpec(int minCols, int maxCols, Collection<ColumnSpec.DataType<?>> columnTypes)
        {
            this.minCks = minCols;
            this.maxCks = maxCols;
            this.ckGenerator = columnSpecGenerator(columnTypes, "ck", ColumnSpec.Kind.CLUSTERING);
            return this;
        }

        public Builder regularColumnCount(int minCols, int maxCols)
        {
            this.minRegular = minCols;
            this.maxRegular = maxCols;
            return this;
        }

        public Builder regularColumnCount(int numCols)
        {
            return regularColumnCount(numCols, numCols);
        }

        public Builder regularColumnSpec(int minCols, int maxCols, ColumnSpec.DataType<?>... columnTypes)
        {
            return this.regularColumnSpec(minCols, maxCols, Arrays.asList(columnTypes));
        }

        public Builder regularColumnSpec(int minCols, int maxCols, Collection<ColumnSpec.DataType<?>> columnTypes)
        {
            this.minRegular = minCols;
            this.maxRegular = maxCols;
            this.regularGenerator = columnSpecGenerator(columnTypes, "regular", ColumnSpec.Kind.REGULAR);
            return this;
        }

        public Builder staticColumnCount(int minCols, int maxCols)
        {
            this.minStatic = minCols;
            this.maxStatic = maxCols;
            return this;
        }

        public Builder staticColumnCount(int numCols)
        {
            return staticColumnCount(numCols, numCols);
        }

        public Builder staticColumnSpec(int minCols, int maxCols, ColumnSpec.DataType<?>... columnTypes)
        {
            return this.staticColumnSpec(minCols, maxCols, Arrays.asList(columnTypes));
        }

        public Builder staticColumnSpec(int minCols, int maxCols, Collection<ColumnSpec.DataType<?>> columnTypes)
        {
            this.minStatic = minCols;
            this.maxStatic = maxCols;
            this.staticGenerator = columnSpecGenerator(columnTypes, "static", ColumnSpec.Kind.STATIC);
            return this;
        }

        private static class ColumnCounts
        {
            private final int pks;
            private final int cks;
            private final int regulars;
            private final int statics;

            private ColumnCounts(int pks, int cks, int regulars, int statics)
            {
                this.pks = pks;
                this.cks = cks;
                this.regulars = regulars;
                this.statics = statics;
            }
        }

        public Generator<ColumnCounts> columnCountsGenerator()
        {
            return (rand) -> {
                int pks = rand.nextInt(minPks, maxPks);
                int cks = rand.nextInt(minCks, maxCks);
                int regulars = rand.nextInt(minRegular, maxRegular);
                int statics = rand.nextInt(minStatic, maxStatic);

                return new ColumnCounts(pks, cks, regulars, statics);
            };
        }

        public Generator<SchemaSpec> generator()
        {
            Generator<ColumnCounts> columnCountsGenerator = columnCountsGenerator();

            return columnCountsGenerator.flatMap(counts -> {
                return rand -> {
                    List<ColumnSpec<?>> pk = pkGenerator.generate(rand, counts.pks);
                    List<ColumnSpec<?>> ck = ckGenerator.generate(rand, counts.cks);
                    return new SchemaSpec(keyspace,
                                          tableNameSupplier.get(),
                                          pk,
                                          ck,
                                          regularGenerator.generate(rand, counts.regulars),
                                          staticGenerator.generate(rand, counts.statics));
                };
            });
        }

        public Surjections.Surjection<SchemaSpec> surjection()
        {
            return generator().toSurjection(SCHEMAGEN_STREAM_ID);
        }
    }

    public static Surjections.Surjection<SchemaSpec> defaultSchemaSpecGen(String table)
    {
        return new SchemaGenerators.Builder(DEFAULT_KEYSPACE_NAME, () -> table)
               .partitionKeySpec(1, 3,
                                 partitionKeyTypes)
               .clusteringKeySpec(1, 3,
                                  clusteringKeyTypes)
               .regularColumnSpec(3, 5,
                                  ColumnSpec.int8Type,
                                  ColumnSpec.int16Type,
                                  ColumnSpec.int32Type,
                                  ColumnSpec.int64Type,
                                  ColumnSpec.floatType,
                                  ColumnSpec.doubleType,
                                  ColumnSpec.asciiType(5, 256))
               .staticColumnSpec(3, 5,
                                 ColumnSpec.int8Type,
                                 ColumnSpec.int16Type,
                                 ColumnSpec.int32Type,
                                 ColumnSpec.int64Type,
                                 ColumnSpec.floatType,
                                 ColumnSpec.doubleType,
                                 ColumnSpec.asciiType(4, 512),
                                 ColumnSpec.asciiType(4, 2048))
               .surjection();
    }

    public static String DEFAULT_KEYSPACE_NAME = "harry";

    private static final String DEFAULT_PREFIX = "table_";
    private static final AtomicInteger counter = new AtomicInteger();
    private static final Supplier<String> tableNameSupplier = () -> DEFAULT_PREFIX + counter.getAndIncrement();

    // simplest schema gen, nothing can go wrong with it
    public static final Surjections.Surjection<SchemaSpec> longOnlySpecBuilder = new Builder(DEFAULT_KEYSPACE_NAME, tableNameSupplier)
                                                                                 .partitionKeySpec(1, 1, ColumnSpec.int64Type)
                                                                                 .clusteringKeySpec(1, 1, ColumnSpec.int64Type)
                                                                                 .regularColumnSpec(1, 10, ColumnSpec.int64Type)
                                                                                 .staticColumnSpec(1, 10, ColumnSpec.int64Type)
                                                                                 .surjection();

    private static final ColumnSpec.DataType<String> simpleStringType = ColumnSpec.asciiType(4, 10);
    private static final Surjections.Surjection<SchemaSpec> longAndStringSpecBuilder = new SchemaGenerators.Builder(DEFAULT_KEYSPACE_NAME, tableNameSupplier)
                                                                                       .partitionKeySpec(2, 2, ColumnSpec.int64Type, simpleStringType)
                                                                                       .clusteringKeySpec(2, 2, ColumnSpec.int64Type, simpleStringType)
                                                                                       .regularColumnSpec(1, 10, ColumnSpec.int64Type, simpleStringType)
                                                                                       .staticColumnSpec(1, 10, ColumnSpec.int64Type)
                                                                                       .surjection();

    public static final Surjections.Surjection<SchemaSpec> longOnlyWithReverseSpecBuilder = new SchemaGenerators.Builder(DEFAULT_KEYSPACE_NAME, tableNameSupplier)
                                                                                            .partitionKeySpec(1, 1, ColumnSpec.int64Type)
                                                                                            .clusteringKeySpec(1, 1, ColumnSpec.ReversedType.getInstance(ColumnSpec.int64Type))
                                                                                            .regularColumnSpec(1, 10, ColumnSpec.int64Type)
                                                                                            .staticColumnSpec(1, 10, ColumnSpec.int64Type)
                                                                                            .surjection();

    public static final Surjections.Surjection<SchemaSpec> longAndStringSpecWithReversedLongBuilder = new SchemaGenerators.Builder(DEFAULT_KEYSPACE_NAME, tableNameSupplier)
                                                                                                      .partitionKeySpec(2, 2, ColumnSpec.int64Type, simpleStringType)
                                                                                                      .clusteringKeySpec(2, 2, ColumnSpec.ReversedType.getInstance(ColumnSpec.int64Type), simpleStringType)
                                                                                                      .regularColumnSpec(1, 10, ColumnSpec.int64Type, simpleStringType)
                                                                                                      .staticColumnSpec(1, 10, ColumnSpec.int64Type)
                                                                                                      .surjection();

    public static final Surjections.Surjection<SchemaSpec> longAndStringSpecWithReversedStringBuilder = new SchemaGenerators.Builder(DEFAULT_KEYSPACE_NAME, tableNameSupplier)
                                                                                                        .partitionKeySpec(2, 2, ColumnSpec.int64Type, simpleStringType)
                                                                                                        .clusteringKeySpec(2, 2, ColumnSpec.int64Type, ColumnSpec.ReversedType.getInstance(simpleStringType))
                                                                                                        .regularColumnSpec(1, 10, ColumnSpec.int64Type, simpleStringType)
                                                                                                        .staticColumnSpec(1, 10, ColumnSpec.int64Type)
                                                                                                        .surjection();

    public static final Surjections.Surjection<SchemaSpec> longAndStringSpecWithReversedBothBuilder = new SchemaGenerators.Builder(DEFAULT_KEYSPACE_NAME, tableNameSupplier)
                                                                                                      .partitionKeySpec(2, 2, ColumnSpec.int64Type, simpleStringType)
                                                                                                      .clusteringKeySpec(2, 2, ColumnSpec.ReversedType.getInstance(ColumnSpec.int64Type), ColumnSpec.ReversedType.getInstance(simpleStringType))
                                                                                                      .regularColumnSpec(1, 10, ColumnSpec.int64Type, simpleStringType)
                                                                                                      .staticColumnSpec(1, 10, ColumnSpec.int64Type)
                                                                                                      .surjection();

    public static final Surjections.Surjection<SchemaSpec> withAllFeaturesEnabled = new SchemaGenerators.Builder(DEFAULT_KEYSPACE_NAME, tableNameSupplier)
                                                                                    .partitionKeySpec(1, 4, columnTypes)
                                                                                    .clusteringKeySpec(1, 4, clusteringKeyTypes)
                                                                                    .regularColumnSpec(1, 10, columnTypes)
                                                                                    .surjection();

    public static final Surjections.Surjection<SchemaSpec>[] PROGRESSIVE_GENERATORS = new Surjections.Surjection[]{
    longOnlySpecBuilder,
    longAndStringSpecBuilder,
    longOnlyWithReverseSpecBuilder,
    longAndStringSpecWithReversedLongBuilder,
    longAndStringSpecWithReversedStringBuilder,
    longAndStringSpecWithReversedBothBuilder,
    withAllFeaturesEnabled
    };

    // Create schema generators that would produce tables starting with just a few features, progressing to use more
    public static Supplier<SchemaSpec> progression(int switchAfter)
    {
        Supplier<SchemaSpec>[] generators = new Supplier[PROGRESSIVE_GENERATORS.length];
        for (int i = 0; i < generators.length; i++)
            generators[i] = PROGRESSIVE_GENERATORS[i].toSupplier();

        return new Supplier<SchemaSpec>()
        {
            private int counter = 0;
            public SchemaSpec get()
            {
                int idx = (counter / switchAfter) % generators.length;
                counter++;
                SchemaSpec spec = generators[idx].get();
                int tries = 100;
                while ((spec.pkGenerator.byteSize() != Long.BYTES) && tries > 0)
                {
                    System.out.println("Skipping schema, since it doesn't have enough entropy bits available: " + spec.compile().cql());
                    spec = generators[idx].get();
                    tries--;
                }

                spec.validate();

                assert tries > 0 : String.format("Max number of tries exceeded on generator %d, can't generate a needed schema", idx);
                return spec;
            }
        };
    }

    public static List<ColumnSpec<?>> toColumns(Map<String, String> config, ColumnSpec.Kind kind, boolean allowReverse)
    {
        if (config == null)
            return Collections.EMPTY_LIST;

        List<ColumnSpec<?>> columns = new ArrayList<>(config.size());

        for (Map.Entry<String, String> e : config.entrySet())
        {
            ColumnSpec.DataType<?> type = nameToTypeMap.get(e.getValue());
            assert type != null : "Can't parse the type";
            assert allowReverse || !type.isReversed() : String.format("%s columns aren't allowed to be reversed", type);
            columns.add(new ColumnSpec<>(e.getKey(), type, kind));
        }

        return columns;
    }

    public static SchemaSpec parse(String keyspace,
                                   String table,
                                   Map<String, String> pks,
                                   Map<String, String> cks,
                                   Map<String, String> regulars,
                                   Map<String, String> statics)
    {
        return new SchemaSpec(keyspace, table,
                              toColumns(pks, ColumnSpec.Kind.PARTITION_KEY, false),
                              toColumns(cks, ColumnSpec.Kind.CLUSTERING, false),
                              toColumns(regulars, ColumnSpec.Kind.REGULAR, false),
                              toColumns(statics, ColumnSpec.Kind.STATIC, false));
    }

    public static int DEFAULT_SWITCH_AFTER = CassandraRelevantProperties.TEST_HARRY_SWITCH_AFTER.getInt();
    public static int GENERATORS_COUNT = PROGRESSIVE_GENERATORS.length;
    public static int DEFAULT_RUNS = DEFAULT_SWITCH_AFTER * GENERATORS_COUNT;
}
