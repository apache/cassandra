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
package org.apache.cassandra.harry.stress.config;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.cassandra.harry.gen.TypeAdapters;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.Constructor;

import org.apache.cassandra.config.YamlConfigurationLoader;
import org.apache.cassandra.cql3.statements.schema.CreateTableStatement;
import org.apache.cassandra.db.marshal.*;
import org.apache.cassandra.harry.ColumnSpec;
import org.apache.cassandra.harry.SchemaSpec;
import org.apache.cassandra.harry.gen.SchemaGenerators;
import org.apache.cassandra.harry.gen.EntropySource;
import org.apache.cassandra.harry.gen.rng.JdkRandomEntropySource;
import org.apache.cassandra.harry.stress.RotationStrategy;
import org.apache.cassandra.harry.stress.VisitGenerator;
import org.apache.cassandra.harry.stress.distribution.Distribution;
import org.apache.cassandra.harry.stress.distribution.Distributions;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;

import static org.apache.cassandra.utils.LocalizeString.toLowerCaseLocalized;

public class StressSchemaConfig
{
    private final SchemaSpec schema;
    private final Distribution rowPopulation;
    private final Map<String, Distribution> columnPopulations;
    private final Map<String, Distribution> valueSizeDistribution;
    private final RotationConfig rotation;

    private StressSchemaConfig(SchemaSpec schema, Distribution rowPopulation, Map<String, Distribution> columnPopulations, Map<String, Distribution> valueSizeDistribution, RotationConfig rotation)
    {
        this.schema = schema;
        this.rowPopulation = rowPopulation;
        this.columnPopulations = columnPopulations;
        this.valueSizeDistribution = valueSizeDistribution;
        this.rotation = rotation;
    }

    public SchemaSpec schema()
    {
        return schema;
    }

    public Distribution rowPopulation()
    {
        return rowPopulation;
    }

    public VisitGenerator.ColumnPopulation columnPopulation()
    {
        return column -> columnPopulations.getOrDefault(column, Distributions.fixed(100));
    }

    /**
     * A fresh {@link RotationStrategy} built from the {@code rotation:} section of the config. Returns a new instance
     * on every call because rotation strategies are stateful, so a single config can hand identical-but-independent
     * strategies to, e.g., offline generation and a validation replay.
     */
    public RotationStrategy rotationStrategy()
    {
        return rotation.build();
    }

    public static StressSchemaConfig load(Path yamlFile) throws IOException
    {
        try (InputStream is = Files.newInputStream(yamlFile))
        {
            return load(is);
        }
    }

    public static StressSchemaConfig load(URI uri) throws IOException
    {
        try (InputStream is = uri.toURL().openStream())
        {
            return load(is);
        }
    }

    public static StressSchemaConfig load(InputStream is)
    {
        Constructor constructor = new Constructor(StressSchemaYaml.class, YamlConfigurationLoader.getDefaultLoaderOptions());
        Yaml yaml = new Yaml(constructor);
        StressSchemaYaml parsed = yaml.loadAs(is, StressSchemaYaml.class);
        return fromYaml(parsed);
    }

    static StressSchemaConfig fromYaml(StressSchemaYaml yaml)
    {
        if (yaml.keyspace == null)
            throw new IllegalArgumentException("keyspace is required");
        if (yaml.table == null)
            throw new IllegalArgumentException("table is required");

        Map<String, ColumnConfig> populationSpecs = parseColumnSpecs(yaml.columnspec);

        Map<String, Distribution> populations = new HashMap<>();
        Map<String, Distribution> valueSizeDistributions = new HashMap<>();
        for (Map.Entry<String, ColumnConfig> e : populationSpecs.entrySet())
        {
            populations.put(e.getKey(), parseDistribution(e.getValue().population, Distributions.uniformRandom(1, 10)));
            valueSizeDistributions.put(e.getKey(), parseDistribution(e.getValue().size, null));
        }
        Distribution rowPopulation = parseDistribution(yaml.rows, Distributions.uniformRandom(1, 10));

        SchemaSpec schema;
        if (yaml.table_definition != null)
            schema = parseTableDefinition(yaml.keyspace, yaml.table, yaml.table_definition, valueSizeDistributions);
        else
            schema = generateRandomSchema(yaml.keyspace, yaml.table, yaml.seed);
        return new StressSchemaConfig(schema, rowPopulation, populations, valueSizeDistributions, RotationConfig.fromYaml(yaml.rotation));
    }

    private static Map<String, ColumnConfig> parseColumnSpecs(List<Map<String, Object>> columnspec)
    {
        Map<String, ColumnConfig> columnConfigs = new HashMap<>();
        if (columnspec != null)
        {
            for (Map<String, Object> spec : columnspec)
            {
                Map<String, Object> entry = new HashMap<>(spec);
                lowerCaseKeys(entry);
                String name = (String) entry.get("name");
                if (name == null)
                    throw new IllegalArgumentException("Missing 'name' in columnspec entry");
                String population = (String) entry.get("population");
                String size = (String) entry.get("size");
                if (population != null || size != null)
                    columnConfigs.put(name, new ColumnConfig(size, population));
            }
        }
        return columnConfigs;
    }

    private static class ColumnConfig
    {
        final String size;
        final String population;

        ColumnConfig(String size, String population)
        {
            this.size = size;
            this.population = population;
        }
    }

    private static SchemaSpec parseTableDefinition(String keyspace, String table, String tableCql, Map<String, Distribution> valueSizeDistribution)
    {
        TableMetadata metadata = CreateTableStatement.parse(tableCql, keyspace).build();

        List<ColumnSpec<?>> pks = new ArrayList<>();
        for (ColumnMetadata cm : metadata.partitionKeyColumns())
        {
            pks.add(ColumnSpec.pk(cm.name.toString(),
                                  resolveServerType(cm.type),
                                  TypeAdapters.forValues(cm.type, valueSizeDistribution.get(cm.name.toString()))));
        }

        List<ColumnSpec<?>> cks = new ArrayList<>();
        for (ColumnMetadata cm : metadata.clusteringColumns())
        {
            cks.add(ColumnSpec.ck(cm.name.toString(),
                                  resolveServerType(cm.type.unwrap()),
                                  TypeAdapters.forValues(cm.type.unwrap(), valueSizeDistribution.get(cm.name.toString())),
                                  cm.type.isReversed()));
        }

        List<ColumnSpec<?>> regulars = new ArrayList<>();
        List<ColumnSpec<?>> statics = new ArrayList<>();
        Iterator<ColumnMetadata> it = metadata.allColumnsInSelectOrder();
        while (it.hasNext())
        {
            ColumnMetadata cm = it.next();
            if (cm.isRegular())
            {
                regulars.add(ColumnSpec.regularColumn(cm.name.toString(),
                                                      resolveServerType(cm.type),
                                                      TypeAdapters.forValues(cm.type, valueSizeDistribution.get(cm.name.toString()))));
            }
            else if (cm.isStatic())
            {
                statics.add(ColumnSpec.staticColumn(cm.name.toString(),
                                                    resolveServerType(cm.type),
                                                    TypeAdapters.forValues(cm.type, valueSizeDistribution.get(cm.name.toString()))));
            }
        }

        SchemaSpec.Options options = SchemaSpec.optionsBuilder()
                                               .ifNotExists(true)
                                               .addWriteTimestamps(true)
                                               // carry the compaction strategy (e.g. LeveledCompactionStrategy) through from the CQL
                                               .compactionStrategy(metadata.params.compaction.klass().getSimpleName());

        return new SchemaSpec(keyspace, table, pks, cks, regulars, statics, options);
    }

    private static SchemaSpec generateRandomSchema(String keyspace, String table, Long seed)
    {
        long s = seed != null ? seed : System.nanoTime();
        EntropySource rng = new JdkRandomEntropySource(s);
        return SchemaGenerators.schemaSpecGen(keyspace, table, 100).generate(rng);
    }

    @SuppressWarnings("rawtypes")
    static ColumnSpec.DataType resolveServerType(AbstractType<?> serverType)
    {
        if (serverType instanceof ReversedType)
            serverType = ((ReversedType<?>) serverType).baseType;

        if (serverType instanceof AsciiType)       return ColumnSpec.asciiType;
        if (serverType instanceof UTF8Type)        return ColumnSpec.textType;
        if (serverType instanceof LongType)        return ColumnSpec.int64Type;
        if (serverType instanceof Int32Type)       return ColumnSpec.int32Type;
        if (serverType instanceof ShortType)       return ColumnSpec.int16Type;
        if (serverType instanceof ByteType)        return ColumnSpec.int8Type;
        if (serverType instanceof BooleanType)     return ColumnSpec.booleanType;
        if (serverType instanceof FloatType)       return ColumnSpec.floatType;
        if (serverType instanceof DoubleType)      return ColumnSpec.doubleType;
        if (serverType instanceof BytesType)       return ColumnSpec.blobType;
        if (serverType instanceof UUIDType)        return ColumnSpec.uuidType;
        if (serverType instanceof TimeUUIDType)    return ColumnSpec.timeUuidType;
        if (serverType instanceof TimestampType)   return ColumnSpec.timestampType;
        if (serverType instanceof IntegerType)     return ColumnSpec.varintType;
        if (serverType instanceof DecimalType)     return ColumnSpec.decimalType;
        if (serverType instanceof InetAddressType) return ColumnSpec.inetType;
        if (serverType instanceof TimeType)        return ColumnSpec.timeType;

        throw new IllegalArgumentException("Unsupported column type: " + serverType.asCQL3Type());
    }

    public static Distribution parseDistribution(String spec, Distribution onNull)
    {
        if (spec == null) return onNull;
        spec = spec.trim();
        String lower = toLowerCaseLocalized(spec);

        int parenOpen = lower.indexOf('(');
        if (parenOpen < 0 || !lower.endsWith(")"))
            throw new IllegalArgumentException("Invalid distribution spec: " + spec);

        String name = lower.substring(0, parenOpen);
        String args = spec.substring(parenOpen + 1, spec.length() - 1).trim();

        switch (name)
        {
            case "fixed":
                return Distributions.fixed(parseLong(args));
            case "uniform":
            {
                long[] range = parseRange(args);
                return Distributions.uniformRandom(range[0], range[1]);
            }
            case "cdf":
            {
                String[] vs = args.split(",");
                float[] chances = new float[vs.length - 1];
                long[] bounds = new long[vs.length];
                for (int i = 0 ; i < vs.length ; ++i)
                {
                    String[] v = vs[i].split(":");
                    if (i == 0 && chances[i] != 0f)
                        throw new IllegalArgumentException("Must specify a mapping for 0; first is " + vs[i]);

                    if (i < vs.length - 1) chances[i] = Float.parseFloat(v[0]);
                    else if (Float.parseFloat(v[0]) != 1f)
                        throw new IllegalArgumentException("Must specify a mapping for 1; last is " + vs[i]);


                    bounds[i] = Long.parseLong(v[1]);
                }
                return new Distributions.CDF(chances, bounds);
            }
            case "gaussian":
            case "gauss":
            case "normal":
            case "norm":
            {
                String[] parts = args.split(",");
                long[] range = parseRange(parts[0].trim());
                // TODO: use a proper gaussian distribution once available
                return Distributions.fixed(range[0] + (range[1] - range[0]) / 2);
            }
            default:
                throw new IllegalArgumentException("Unsupported distribution type: " + name +
                                                   ". Supported: fixed, uniform, gaussian");
        }
    }

    static long[] parseRange(String rangeSpec)
    {
        String[] bounds = rangeSpec.split("\\.\\.+");
        if (bounds.length != 2)
            throw new IllegalArgumentException("Expected range in form min..max, got: " + rangeSpec);
        return new long[]{ parseLong(bounds[0].trim()), parseLong(bounds[1].trim()) };
    }

    static long parseLong(String value)
    {
        value = toLowerCaseLocalized(value.trim());
        long multiplier = 1;
        if (value.endsWith("b"))
        {
            multiplier = 1_000_000_000L;
            value = value.substring(0, value.length() - 1);
        }
        else if (value.endsWith("m"))
        {
            multiplier = 1_000_000L;
            value = value.substring(0, value.length() - 1);
        }
        else if (value.endsWith("k"))
        {
            multiplier = 1_000L;
            value = value.substring(0, value.length() - 1);
        }
        return Long.parseLong(value) * multiplier;
    }

    private static void lowerCaseKeys(Map<String, Object> map)
    {
        List<String> keys = new ArrayList<>(map.keySet());
        for (String key : keys)
        {
            String lower = toLowerCaseLocalized(key);
            if (!lower.equals(key))
            {
                Object val = map.remove(key);
                map.put(lower, val);
            }
        }
    }

    /** Parsed, default-applied rotation parameters; {@link #build()} yields a fresh (stateful) strategy per call. */
    static final class RotationConfig
    {
        final String strategy;
        final int target;
        final int replaceWithNew;
        final int replaceWithVisited;
        final int switchInterval;

        RotationConfig(String strategy, int target, int replaceWithNew, int replaceWithVisited, int switchInterval)
        {
            this.strategy = strategy;
            this.target = target;
            this.replaceWithNew = replaceWithNew;
            this.replaceWithVisited = replaceWithVisited;
            this.switchInterval = switchInterval;
        }

        static RotationConfig fromYaml(RotationYaml yaml)
        {
            if (yaml == null)
                return new RotationConfig("fixed", 2000, 0, 0, 500);
            return new RotationConfig(yaml.strategy != null ? toLowerCaseLocalized(yaml.strategy.trim()) : "fixed",
                                      yaml.target != null ? yaml.target : 2000,
                                      yaml.replace_with_new != null ? yaml.replace_with_new : 0,
                                      yaml.replace_with_visited != null ? yaml.replace_with_visited : 0,
                                      yaml.partition_switch_interval != null ? yaml.partition_switch_interval : 500);
        }

        RotationStrategy build()
        {
            switch (strategy)
            {
                case "fixed":
                    return new RotationStrategy.FixedRotationStrategy(target, replaceWithNew, replaceWithVisited, switchInterval);
                case "random":
                    return new RotationStrategy.RandomRotationStrategy(target, switchInterval);
                default:
                    throw new IllegalArgumentException("Unknown rotation strategy: " + strategy + ". Expected 'fixed' or 'random'.");
            }
        }
    }

    public static class StressSchemaYaml
    {
        public String keyspace;
        public String table;
        public String table_definition;
        public Long seed;
        public List<Map<String, Object>> columnspec;
        public String rows;
        public RotationYaml rotation;
    }

    public static class RotationYaml
    {
        public String strategy;
        public Integer target;
        public Integer replace_with_new;
        public Integer replace_with_visited;
        public Integer partition_switch_interval;
    }
}
