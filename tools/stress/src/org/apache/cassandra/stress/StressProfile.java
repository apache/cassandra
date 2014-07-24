/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */
package org.apache.cassandra.stress;


import com.datastax.driver.core.*;
import com.datastax.driver.core.exceptions.AlreadyExistsException;

import com.google.common.util.concurrent.Uninterruptibles;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.statements.CreateKeyspaceStatement;
import org.apache.cassandra.exceptions.RequestValidationException;

import org.apache.cassandra.stress.generate.DistributionFactory;
import org.apache.cassandra.stress.generate.PartitionGenerator;
import org.apache.cassandra.stress.generate.RatioDistributionFactory;
import org.apache.cassandra.stress.generate.values.Booleans;
import org.apache.cassandra.stress.generate.values.Bytes;
import org.apache.cassandra.stress.generate.values.Generator;
import org.apache.cassandra.stress.generate.values.Dates;
import org.apache.cassandra.stress.generate.values.Doubles;
import org.apache.cassandra.stress.generate.values.Floats;
import org.apache.cassandra.stress.generate.values.GeneratorConfig;
import org.apache.cassandra.stress.generate.values.Inets;
import org.apache.cassandra.stress.generate.values.Integers;
import org.apache.cassandra.stress.generate.values.Lists;
import org.apache.cassandra.stress.generate.values.Longs;
import org.apache.cassandra.stress.generate.values.Sets;
import org.apache.cassandra.stress.generate.values.Strings;
import org.apache.cassandra.stress.generate.values.TimeUUIDs;
import org.apache.cassandra.stress.generate.values.UUIDs;
import org.apache.cassandra.stress.operations.userdefined.SchemaInsert;
import org.apache.cassandra.stress.operations.userdefined.SchemaQuery;
import org.apache.cassandra.stress.settings.OptionDistribution;
import org.apache.cassandra.stress.settings.OptionRatioDistribution;
import org.apache.cassandra.stress.settings.StressSettings;
import org.apache.cassandra.stress.settings.ValidationType;
import org.apache.cassandra.stress.util.JavaDriverClient;
import org.apache.cassandra.stress.util.ThriftClient;
import org.apache.cassandra.stress.util.Timer;
import org.apache.cassandra.thrift.Compression;
import org.apache.cassandra.thrift.ThriftConversion;
import org.apache.thrift.TException;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.Constructor;
import org.yaml.snakeyaml.error.YAMLException;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOError;
import java.io.IOException;
import java.io.Serializable;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

public class StressProfile implements Serializable
{
    private String keyspaceCql;
    private String tableCql;
    private String seedStr;

    public String keyspaceName;
    public String tableName;
    private Map<String, GeneratorConfig> columnConfigs;
    private Map<String, String> queries;
    private Map<String, String> insert;

    transient volatile TableMetadata tableMetaData;

    transient volatile GeneratorFactory generatorFactory;

    transient volatile BatchStatement.Type batchType;
    transient volatile DistributionFactory partitions;
    transient volatile RatioDistributionFactory pervisit;
    transient volatile RatioDistributionFactory perbatch;
    transient volatile PreparedStatement insertStatement;
    transient volatile Integer thriftInsertId;

    transient volatile Map<String, PreparedStatement> queryStatements;
    transient volatile Map<String, Integer> thriftQueryIds;

    private void init(StressYaml yaml) throws RequestValidationException
    {
        keyspaceName = yaml.keyspace;
        keyspaceCql = yaml.keyspace_definition;
        tableName = yaml.table;
        tableCql = yaml.table_definition;
        seedStr = yaml.seed;
        queries = yaml.queries;
        insert = yaml.insert;

        assert keyspaceName != null : "keyspace name is required in yaml file";
        assert tableName != null : "table name is required in yaml file";
        assert queries != null : "queries map is required in yaml file";

        if (keyspaceCql != null && keyspaceCql.length() > 0)
        {
            String name = ((CreateKeyspaceStatement) QueryProcessor.parseStatement(keyspaceCql)).keyspace();
            assert name.equalsIgnoreCase(keyspaceName) : "Name in keyspace_definition doesn't match keyspace property: '" + name + "' != '" + keyspaceName + "'";
        }
        else
        {
            keyspaceCql = null;
        }

        if (tableCql != null && tableCql.length() > 0)
        {
            String name = CFMetaData.compile(tableCql, keyspaceName).cfName;
            assert name.equalsIgnoreCase(tableName) : "Name in table_definition doesn't match table property: '" + name + "' != '" + tableName + "'";
        }
        else
        {
            tableCql = null;
        }

        columnConfigs = new HashMap<>();
        for (Map<String,Object> spec : yaml.columnspec)
        {
            lowerCase(spec);
            String name = (String) spec.remove("name");
            DistributionFactory population = !spec.containsKey("population") ? null : OptionDistribution.get((String) spec.remove("population"));
            DistributionFactory size = !spec.containsKey("size") ? null : OptionDistribution.get((String) spec.remove("size"));
            DistributionFactory clustering = !spec.containsKey("cluster") ? null : OptionDistribution.get((String) spec.remove("cluster"));

            if (!spec.isEmpty())
                throw new IllegalArgumentException("Unrecognised option(s) in column spec: " + spec);
            if (name == null)
                throw new IllegalArgumentException("Missing name argument in column spec");

            GeneratorConfig config = new GeneratorConfig(yaml.seed + name, clustering, size, population);
            columnConfigs.put(name, config);
        }
    }

    public void maybeCreateSchema(StressSettings settings)
    {
        JavaDriverClient client = settings.getJavaDriverClient(false);

        if (keyspaceCql != null)
        {
            try
            {
                client.execute(keyspaceCql, org.apache.cassandra.db.ConsistencyLevel.ONE);
            }
            catch (AlreadyExistsException e)
            {
            }
        }

        client.execute("use "+keyspaceName, org.apache.cassandra.db.ConsistencyLevel.ONE);

        if (tableCql != null)
        {
            try
            {
                client.execute(tableCql, org.apache.cassandra.db.ConsistencyLevel.ONE);
            }
            catch (AlreadyExistsException e)
            {
            }

            System.out.println(String.format("Created schema. Sleeping %ss for propagation.", settings.node.nodes.size()));
            Uninterruptibles.sleepUninterruptibly(settings.node.nodes.size(), TimeUnit.SECONDS);
        }

        maybeLoadSchemaInfo(settings);
    }


    private void maybeLoadSchemaInfo(StressSettings settings)
    {
        if (tableMetaData == null)
        {
            JavaDriverClient client = settings.getJavaDriverClient();

            synchronized (client)
            {

                if (tableMetaData != null)
                    return;

                TableMetadata metadata = client.getCluster()
                                               .getMetadata()
                                               .getKeyspace(keyspaceName)
                                               .getTable(tableName);

                //Fill in missing column configs
                for (ColumnMetadata col : metadata.getColumns())
                {
                    if (columnConfigs.containsKey(col.getName()))
                        continue;

                    columnConfigs.put(col.getName(), new GeneratorConfig(seedStr + col.getName(), null, null, null));
                }

                tableMetaData = metadata;
            }
        }
    }

    public SchemaQuery getQuery(String name, Timer timer, PartitionGenerator generator, StressSettings settings)
    {
        if (queryStatements == null)
        {
            synchronized (this)
            {
                if (queryStatements == null)
                {
                    try
                    {
                        JavaDriverClient jclient = settings.getJavaDriverClient();
                        ThriftClient tclient = settings.getThriftClient();
                        Map<String, PreparedStatement> stmts = new HashMap<>();
                        Map<String, Integer> tids = new HashMap<>();
                        for (Map.Entry<String, String> e : queries.entrySet())
                        {
                            stmts.put(e.getKey().toLowerCase(), jclient.prepare(e.getValue()));
                            tids.put(e.getKey().toLowerCase(), tclient.prepare_cql3_query(e.getValue(), Compression.NONE));
                        }
                        thriftQueryIds = tids;
                        queryStatements = stmts;
                    }
                    catch (TException e)
                    {
                        throw new RuntimeException(e);
                    }
                }
            }
        }

        // TODO validation
        name = name.toLowerCase();
        return new SchemaQuery(timer, generator, settings, thriftQueryIds.get(name), queryStatements.get(name), ThriftConversion.fromThrift(settings.command.consistencyLevel), ValidationType.NOT_FAIL);
    }

    public SchemaInsert getInsert(Timer timer, PartitionGenerator generator, StressSettings settings)
    {
        if (insertStatement == null)
        {
            synchronized (this)
            {
                if (insertStatement == null)
                {
                    maybeLoadSchemaInfo(settings);

                    Set<ColumnMetadata> keyColumns = com.google.common.collect.Sets.newHashSet(tableMetaData.getPrimaryKey());

                    //Non PK Columns
                    StringBuilder sb = new StringBuilder();

                    sb.append("UPDATE \"").append(tableName).append("\" SET ");

                    //PK Columns
                    StringBuilder pred = new StringBuilder();
                    pred.append(" WHERE ");

                    boolean firstCol = true;
                    boolean firstPred = true;
                    for (ColumnMetadata c : tableMetaData.getColumns())
                    {

                        if (keyColumns.contains(c))
                        {
                            if (firstPred)
                                firstPred = false;
                            else
                                pred.append(" AND ");

                            pred.append(c.getName()).append(" = ?");
                        }
                        else
                        {
                            if (firstCol)
                                firstCol = false;
                            else
                                sb.append(",");

                            sb.append(c.getName()).append(" = ");

                            switch (c.getType().getName())
                            {
                                case SET:
                                case LIST:
                                case COUNTER:
                                    sb.append(c.getName()).append(" + ?");
                                    break;
                                default:
                                    sb.append("?");
                                    break;
                            }
                        }
                    }

                    //Put PK predicates at the end
                    sb.append(pred);

                    if (insert == null)
                        insert = new HashMap<>();
                    lowerCase(insert);

                    partitions = OptionDistribution.get(!insert.containsKey("partitions") ? "fixed(1)" : insert.remove("partitions"));
                    pervisit = OptionRatioDistribution.get(!insert.containsKey("pervisit") ? "fixed(1)/1" : insert.remove("pervisit"));
                    perbatch = OptionRatioDistribution.get(!insert.containsKey("perbatch") ? "fixed(1)/1" : insert.remove("perbatch"));
                    batchType = !insert.containsKey("batchtype") ? BatchStatement.Type.UNLOGGED : BatchStatement.Type.valueOf(insert.remove("batchtype"));
                    if (!insert.isEmpty())
                        throw new IllegalArgumentException("Unrecognised insert option(s): " + insert);

                    if (generator.maxRowCount > 100 * 1000 * 1000)
                        System.err.printf("WARNING: You have defined a schema that permits very large partitions (%.0f max rows (>100M))\n", generator.maxRowCount);
                    if (perbatch.get().max() * pervisit.get().max() * partitions.get().maxValue() * generator.maxRowCount > 100000)
                        System.err.printf("WARNING: You have defined a schema that permits very large batches (%.0f max rows (>100K)). This may OOM this stress client, or the server.\n",
                                           perbatch.get().max() * pervisit.get().max() * partitions.get().maxValue() * generator.maxRowCount);

                    JavaDriverClient client = settings.getJavaDriverClient();
                    String query = sb.toString();
                    try
                    {
                        thriftInsertId = settings.getThriftClient().prepare_cql3_query(query, Compression.NONE);
                    }
                    catch (TException e)
                    {
                        throw new RuntimeException(e);
                    }
                    insertStatement = client.prepare(query);
                }
            }
        }

        return new SchemaInsert(timer, generator, settings, partitions.get(), pervisit.get(), perbatch.get(), thriftInsertId, insertStatement, ThriftConversion.fromThrift(settings.command.consistencyLevel), batchType);
    }

    public PartitionGenerator newGenerator(StressSettings settings)
    {
        if (generatorFactory == null)
        {
            synchronized (this)
            {
                maybeLoadSchemaInfo(settings);
                if (generatorFactory == null)
                    generatorFactory = new GeneratorFactory();
            }
        }

        return generatorFactory.newGenerator();
    }

    private class GeneratorFactory
    {
        final List<ColumnInfo> partitionKeys = new ArrayList<>();
        final List<ColumnInfo> clusteringColumns = new ArrayList<>();
        final List<ColumnInfo> valueColumns = new ArrayList<>();

        private GeneratorFactory()
        {
            Set<ColumnMetadata> keyColumns = com.google.common.collect.Sets.newHashSet(tableMetaData.getPrimaryKey());

            for (ColumnMetadata metadata : tableMetaData.getPartitionKey())
                partitionKeys.add(new ColumnInfo(metadata.getName(), metadata.getType(), columnConfigs.get(metadata.getName())));
            for (ColumnMetadata metadata : tableMetaData.getClusteringColumns())
                clusteringColumns.add(new ColumnInfo(metadata.getName(), metadata.getType(), columnConfigs.get(metadata.getName())));
            for (ColumnMetadata metadata : tableMetaData.getColumns())
                if (!keyColumns.contains(metadata))
                    valueColumns.add(new ColumnInfo(metadata.getName(), metadata.getType(), columnConfigs.get(metadata.getName())));
        }

        PartitionGenerator newGenerator()
        {
            return new PartitionGenerator(get(partitionKeys), get(clusteringColumns), get(valueColumns));
        }

        List<Generator> get(List<ColumnInfo> columnInfos)
        {
            List<Generator> result = new ArrayList<>();
            for (ColumnInfo columnInfo : columnInfos)
                result.add(columnInfo.getGenerator());
            return result;
        }
    }

    static class ColumnInfo
    {
        final String name;
        final DataType type;
        final GeneratorConfig config;

        ColumnInfo(String name, DataType type, GeneratorConfig config)
        {
            this.name = name;
            this.type = type;
            this.config = config;
        }

        Generator getGenerator()
        {
            return getGenerator(name, type, config);
        }

        static Generator getGenerator(final String name, final DataType type, GeneratorConfig config)
        {
            switch (type.getName())
            {
                case ASCII:
                case TEXT:
                case VARCHAR:
                    return new Strings(name, config);
                case BIGINT:
                case COUNTER:
                    return new Longs(name, config);
                case BLOB:
                    return new Bytes(name, config);
                case BOOLEAN:
                    return new Booleans(name, config);
                case DECIMAL:
                case DOUBLE:
                    return new Doubles(name, config);
                case FLOAT:
                    return new Floats(name, config);
                case INET:
                    return new Inets(name, config);
                case INT:
                case VARINT:
                    return new Integers(name, config);
                case TIMESTAMP:
                    return new Dates(name, config);
                case UUID:
                    return new UUIDs(name, config);
                case TIMEUUID:
                    return new TimeUUIDs(name, config);
                case SET:
                    return new Sets(name, getGenerator(name, type.getTypeArguments().get(0), config), config);
                case LIST:
                    return new Lists(name, getGenerator(name, type.getTypeArguments().get(0), config), config);
                default:
                    throw new UnsupportedOperationException();
            }
        }
    }

    public static StressProfile load(File file) throws IOError
    {
        try
        {
            byte[] profileBytes = Files.readAllBytes(Paths.get(file.toURI()));

            Constructor constructor = new Constructor(StressYaml.class);

            Yaml yaml = new Yaml(constructor);

            StressYaml profileYaml = yaml.loadAs(new ByteArrayInputStream(profileBytes), StressYaml.class);

            StressProfile profile = new StressProfile();
            profile.init(profileYaml);

            return profile;
        }
        catch (YAMLException | IOException | RequestValidationException e)
        {
            throw new IOError(e);
        }
    }

    static <V> void lowerCase(Map<String, V> map)
    {
        List<Map.Entry<String, V>> reinsert = new ArrayList<>();
        Iterator<Map.Entry<String, V>> iter = map.entrySet().iterator();
        while (iter.hasNext())
        {
            Map.Entry<String, V> e = iter.next();
            if (!e.getKey().equalsIgnoreCase(e.getKey()))
            {
                reinsert.add(e);
                iter.remove();
            }
        }
        for (Map.Entry<String, V> e : reinsert)
            map.put(e.getKey().toLowerCase(), e.getValue());
    }
}
