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


import java.io.IOError;
import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.net.URI;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import com.google.common.base.Function;
import com.google.common.util.concurrent.Uninterruptibles;

import com.datastax.driver.core.*;
import com.datastax.driver.core.exceptions.AlreadyExistsException;
import org.antlr.runtime.RecognitionException;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.cql3.CQLFragmentParser;
import org.apache.cassandra.cql3.CqlParser;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.statements.CreateTableStatement;
import org.apache.cassandra.exceptions.RequestValidationException;
import org.apache.cassandra.exceptions.SyntaxException;
import org.apache.cassandra.stress.generate.*;
import org.apache.cassandra.stress.generate.values.*;
import org.apache.cassandra.stress.operations.userdefined.TokenRangeQuery;
import org.apache.cassandra.stress.operations.userdefined.SchemaInsert;
import org.apache.cassandra.stress.operations.userdefined.SchemaQuery;
import org.apache.cassandra.stress.operations.userdefined.ValidatingSchemaQuery;
import org.apache.cassandra.stress.report.Timer;
import org.apache.cassandra.stress.settings.*;
import org.apache.cassandra.stress.util.JavaDriverClient;
import org.apache.cassandra.stress.util.ResultLogger;
import org.apache.cassandra.stress.util.ThriftClient;
import org.apache.cassandra.thrift.Compression;
import org.apache.cassandra.thrift.ThriftConversion;
import org.apache.thrift.TException;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.Constructor;
import org.yaml.snakeyaml.error.YAMLException;

public class StressProfile implements Serializable
{
    private String keyspaceCql;
    private String tableCql;
    private List<String> extraSchemaDefinitions;
    public final String seedStr = "seed for stress";

    public String keyspaceName;
    public String tableName;
    private Map<String, GeneratorConfig> columnConfigs;
    private Map<String, StressYaml.QueryDef> queries;
    public Map<String, StressYaml.TokenRangeQueryDef> tokenRangeQueries;
    private Map<String, String> insert;
    private boolean schemaCreated=false;

    transient volatile TableMetadata tableMetaData;
    transient volatile Set<TokenRange> tokenRanges;

    transient volatile GeneratorFactory generatorFactory;

    transient volatile BatchStatement.Type batchType;
    transient volatile DistributionFactory partitions;
    transient volatile RatioDistributionFactory selectchance;
    transient volatile RatioDistributionFactory rowPopulation;
    transient volatile PreparedStatement insertStatement;
    transient volatile Integer thriftInsertId;
    transient volatile List<ValidatingSchemaQuery.Factory> validationFactories;

    transient volatile Map<String, SchemaQuery.ArgSelect> argSelects;
    transient volatile Map<String, PreparedStatement> queryStatements;
    transient volatile Map<String, Integer> thriftQueryIds;

    private static final Pattern lowercaseAlphanumeric = Pattern.compile("[a-z0-9_]+");


    public void printSettings(ResultLogger out, StressSettings stressSettings)
    {
        out.printf("  Keyspace Name: %s%n", keyspaceName);
        out.printf("  Keyspace CQL: %n***%n%s***%n%n", keyspaceCql);
        out.printf("  Table Name: %s%n", tableName);
        out.printf("  Table CQL: %n***%n%s***%n%n", tableCql);
        out.printf("  Extra Schema Definitions: %s%n", extraSchemaDefinitions);
        if (columnConfigs != null)
        {
            out.printf("  Generator Configs:%n");
            columnConfigs.forEach((k, v) -> out.printf("    %s: %s%n", k, v.getConfigAsString()));
        }
        if(queries != null)
        {
            out.printf("  Query Definitions:%n");
            queries.forEach((k, v) -> out.printf("    %s: %s%n", k, v.getConfigAsString()));
        }
        if (tokenRangeQueries != null)
        {
            out.printf("  Token Range Queries:%n");
            tokenRangeQueries.forEach((k, v) -> out.printf("    %s: %s%n", k, v.getConfigAsString()));
        }
        if (insert != null)
        {
            out.printf("  Insert Settings:%n");
            insert.forEach((k, v) -> out.printf("    %s: %s%n", k, v));
        }

        PartitionGenerator generator = newGenerator(stressSettings);
        Distribution visits = stressSettings.insert.visits.get();
        SchemaInsert tmp = getInsert(null, generator, null, stressSettings); //just calling this to initialize selectchance and partitions vals for calc below

        double minBatchSize = selectchance.get().min() * partitions.get().minValue() * generator.minRowCount * (1d / visits.maxValue());
        double maxBatchSize = selectchance.get().max() * partitions.get().maxValue() * generator.maxRowCount * (1d / visits.minValue());
        out.printf("Generating batches with [%d..%d] partitions and [%.0f..%.0f] rows (of [%.0f..%.0f] total rows in the partitions)%n",
                          partitions.get().minValue(), partitions.get().maxValue(),
                          minBatchSize, maxBatchSize,
                          partitions.get().minValue() * generator.minRowCount,
                          partitions.get().maxValue() * generator.maxRowCount);

    }


    private void init(StressYaml yaml) throws RequestValidationException
    {
        keyspaceName = yaml.keyspace;
        keyspaceCql = yaml.keyspace_definition;
        tableName = yaml.table;
        tableCql = yaml.table_definition;
        queries = yaml.queries;
        tokenRangeQueries = yaml.token_range_queries;
        insert = yaml.insert;

        extraSchemaDefinitions = yaml.extra_definitions;

        assert keyspaceName != null : "keyspace name is required in yaml file";
        assert tableName != null : "table name is required in yaml file";
        assert queries != null : "queries map is required in yaml file";

        for (String query : queries.keySet())
            assert !tokenRangeQueries.containsKey(query) :
                String.format("Found %s in both queries and token_range_queries, please use different names", query);

        if (keyspaceCql != null && keyspaceCql.length() > 0)
        {
            try
            {
                String name = CQLFragmentParser.parseAnyUnhandled(CqlParser::createKeyspaceStatement, keyspaceCql).keyspace();
                assert name.equalsIgnoreCase(keyspaceName) : "Name in keyspace_definition doesn't match keyspace property: '" + name + "' != '" + keyspaceName + "'";
            }
            catch (RecognitionException | SyntaxException e)
            {
                throw new IllegalArgumentException("There was a problem parsing the keyspace cql: " + e.getMessage());
            }
        }
        else
        {
            keyspaceCql = null;
        }

        if (tableCql != null && tableCql.length() > 0)
        {
            try
            {
                String name = CQLFragmentParser.parseAnyUnhandled(CqlParser::createTableStatement, tableCql).columnFamily();
                assert name.equalsIgnoreCase(tableName) : "Name in table_definition doesn't match table property: '" + name + "' != '" + tableName + "'";
            }
            catch (RecognitionException | RuntimeException e)
            {
                throw new IllegalArgumentException("There was a problem parsing the table cql: " + e.getMessage());
            }
        }
        else
        {
            tableCql = null;
        }

        columnConfigs = new HashMap<>();

        if (yaml.columnspec != null)
        {
            for (Map<String, Object> spec : yaml.columnspec)
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

                GeneratorConfig config = new GeneratorConfig(seedStr + name, clustering, size, population);
                columnConfigs.put(name, config);
            }
        }
    }

    public void maybeCreateSchema(StressSettings settings)
    {
        if (!schemaCreated)
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

            client.execute("use " + keyspaceName, org.apache.cassandra.db.ConsistencyLevel.ONE);

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

            if (extraSchemaDefinitions != null)
            {
                for (String extraCql : extraSchemaDefinitions)
                {

                    try
                    {
                        client.execute(extraCql, org.apache.cassandra.db.ConsistencyLevel.ONE);
                    }
                    catch (AlreadyExistsException e)
                    {
                    }
                }

                System.out.println(String.format("Created extra schema. Sleeping %ss for propagation.", settings.node.nodes.size()));
                Uninterruptibles.sleepUninterruptibly(settings.node.nodes.size(), TimeUnit.SECONDS);
            }
        schemaCreated = true;
        }
        maybeLoadSchemaInfo(settings);

    }

    public void truncateTable(StressSettings settings)
    {
        JavaDriverClient client = settings.getJavaDriverClient(false);
        assert settings.command.truncate != SettingsCommand.TruncateWhen.NEVER;
        String cql = String.format("TRUNCATE %s.%s", keyspaceName, tableName);
        client.execute(cql, org.apache.cassandra.db.ConsistencyLevel.ONE);
        System.out.println(String.format("Truncated %s.%s. Sleeping %ss for propagation.",
                                         keyspaceName, tableName, settings.node.nodes.size()));
        Uninterruptibles.sleepUninterruptibly(settings.node.nodes.size(), TimeUnit.SECONDS);
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
                                               .getTable(quoteIdentifier(tableName));

                if (metadata == null)
                    throw new RuntimeException("Unable to find table " + keyspaceName + "." + tableName);

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

    public Set<TokenRange> maybeLoadTokenRanges(StressSettings settings)
    {
        maybeLoadSchemaInfo(settings); // ensure table metadata is available

        JavaDriverClient client = settings.getJavaDriverClient();
        synchronized (client)
        {
            if (tokenRanges != null)
                return tokenRanges;

            Cluster cluster = client.getCluster();
            Metadata metadata = cluster.getMetadata();
            if (metadata == null)
                throw new RuntimeException("Unable to get metadata");

            List<TokenRange> sortedRanges = new ArrayList<>(metadata.getTokenRanges().size() + 1);
            for (TokenRange range : metadata.getTokenRanges())
            {
                //if we don't unwrap we miss the partitions between ring min and smallest range start value
                if (range.isWrappedAround())
                    sortedRanges.addAll(range.unwrap());
                else
                    sortedRanges.add(range);
            }

            Collections.sort(sortedRanges);
            tokenRanges = new LinkedHashSet<>(sortedRanges);
            return tokenRanges;
        }
    }

    public Operation getQuery(String name,
                              Timer timer,
                              PartitionGenerator generator,
                              SeedManager seeds,
                              StressSettings settings,
                              boolean isWarmup)
    {
        name = name.toLowerCase();
        if (!queries.containsKey(name))
            throw new IllegalArgumentException("No query defined with name " + name);

        if (queryStatements == null)
        {
            synchronized (this)
            {
                if (queryStatements == null)
                {
                    try
                    {
                        JavaDriverClient jclient = settings.getJavaDriverClient();
                        ThriftClient tclient = null;

                        if (settings.mode.api != ConnectionAPI.JAVA_DRIVER_NATIVE)
                            tclient = settings.getThriftClient();

                        Map<String, PreparedStatement> stmts = new HashMap<>();
                        Map<String, Integer> tids = new HashMap<>();
                        Map<String, SchemaQuery.ArgSelect> args = new HashMap<>();
                        for (Map.Entry<String, StressYaml.QueryDef> e : queries.entrySet())
                        {
                            stmts.put(e.getKey().toLowerCase(), jclient.prepare(e.getValue().cql));

                            if (tclient != null)
                                tids.put(e.getKey().toLowerCase(), tclient.prepare_cql3_query(e.getValue().cql, Compression.NONE));

                            args.put(e.getKey().toLowerCase(), e.getValue().fields == null
                                                                     ? SchemaQuery.ArgSelect.MULTIROW
                                                                     : SchemaQuery.ArgSelect.valueOf(e.getValue().fields.toUpperCase()));
                        }
                        thriftQueryIds = tids;
                        queryStatements = stmts;
                        argSelects = args;
                    }
                    catch (TException e)
                    {
                        throw new RuntimeException(e);
                    }
                }
            }
        }

        return new SchemaQuery(timer, settings, generator, seeds, thriftQueryIds.get(name), queryStatements.get(name),
                               ThriftConversion.fromThrift(settings.command.consistencyLevel), argSelects.get(name));
    }

    public Operation getBulkReadQueries(String name, Timer timer, StressSettings settings, TokenRangeIterator tokenRangeIterator, boolean isWarmup)
    {
        StressYaml.TokenRangeQueryDef def = tokenRangeQueries.get(name);
        if (def == null)
            throw new IllegalArgumentException("No bulk read query defined with name " + name);

        return new TokenRangeQuery(timer, settings, tableMetaData, tokenRangeIterator, def, isWarmup);
    }


    public PartitionGenerator getOfflineGenerator()
    {
        CFMetaData cfMetaData = CFMetaData.compile(tableCql, keyspaceName);

        //Add missing column configs
        Iterator<ColumnDefinition> it = cfMetaData.allColumnsInSelectOrder();
        while (it.hasNext())
        {
            ColumnDefinition c = it.next();
            if (!columnConfigs.containsKey(c.name.toString()))
                columnConfigs.put(c.name.toString(), new GeneratorConfig(seedStr + c.name.toString(), null, null, null));
        }

        List<Generator> partitionColumns = cfMetaData.partitionKeyColumns().stream()
                                                     .map(c -> new ColumnInfo(c.name.toString(), c.type.asCQL3Type().toString(), "", columnConfigs.get(c.name.toString())))
                                                     .map(c -> c.getGenerator())
                                                     .collect(Collectors.toList());

        List<Generator> clusteringColumns = cfMetaData.clusteringColumns().stream()
                                                             .map(c -> new ColumnInfo(c.name.toString(), c.type.asCQL3Type().toString(), "", columnConfigs.get(c.name.toString())))
                                                             .map(c -> c.getGenerator())
                                                             .collect(Collectors.toList());

        List<Generator> regularColumns = com.google.common.collect.Lists.newArrayList(cfMetaData.partitionColumns().selectOrderIterator()).stream()
                                                                                                             .map(c -> new ColumnInfo(c.name.toString(), c.type.asCQL3Type().toString(), "", columnConfigs.get(c.name.toString())))
                                                                                                             .map(c -> c.getGenerator())
                                                                                                             .collect(Collectors.toList());

        return new PartitionGenerator(partitionColumns, clusteringColumns, regularColumns, PartitionGenerator.Order.ARBITRARY);
    }

    public CreateTableStatement.RawStatement getCreateStatement()
    {
        CreateTableStatement.RawStatement createStatement = QueryProcessor.parseStatement(tableCql, CreateTableStatement.RawStatement.class, "CREATE TABLE");
        createStatement.prepareKeyspace(keyspaceName);

        return createStatement;
    }

    public SchemaInsert getOfflineInsert(Timer timer, PartitionGenerator generator, SeedManager seedManager, StressSettings settings)
    {
        assert tableCql != null;

        CFMetaData cfMetaData = CFMetaData.compile(tableCql, keyspaceName);

        List<ColumnDefinition> allColumns = com.google.common.collect.Lists.newArrayList(cfMetaData.allColumnsInSelectOrder());

        StringBuilder sb = new StringBuilder();
        sb.append("INSERT INTO ").append(quoteIdentifier(keyspaceName)).append(".").append(quoteIdentifier(tableName)).append(" (");
        StringBuilder value = new StringBuilder();
        for (ColumnDefinition c : allColumns)
        {
            sb.append(quoteIdentifier(c.name.toString())).append(", ");
            value.append("?, ");
        }
        sb.delete(sb.lastIndexOf(","), sb.length());
        value.delete(value.lastIndexOf(","), value.length());
        sb.append(") ").append("values(").append(value).append(')');


        if (insert == null)
            insert = new HashMap<>();
        lowerCase(insert);

        partitions = select(settings.insert.batchsize, "partitions", "fixed(1)", insert, OptionDistribution.BUILDER);
        selectchance = select(settings.insert.selectRatio, "select", "fixed(1)/1", insert, OptionRatioDistribution.BUILDER);
        rowPopulation = select(settings.insert.rowPopulationRatio, "row-population", "fixed(1)/1", insert, OptionRatioDistribution.BUILDER);

        if (generator.maxRowCount > 100 * 1000 * 1000)
            System.err.printf("WARNING: You have defined a schema that permits very large partitions (%.0f max rows (>100M))%n", generator.maxRowCount);

        String statement = sb.toString();

        //CQLTableWriter requires the keyspace name be in the create statement
        String tableCreate = tableCql.replaceFirst("\\s+\"?"+tableName+"\"?\\s+", " \""+keyspaceName+"\".\""+tableName+"\" ");


        return new SchemaInsert(timer, settings, generator, seedManager, selectchance.get(), rowPopulation.get(), thriftInsertId, statement, tableCreate);
    }

    public SchemaInsert getInsert(Timer timer, PartitionGenerator generator, SeedManager seedManager, StressSettings settings)
    {
        if (insertStatement == null)
        {
            synchronized (this)
            {
                if (insertStatement == null)
                {
                    maybeLoadSchemaInfo(settings);

                    Set<ColumnMetadata> keyColumns = com.google.common.collect.Sets.newHashSet(tableMetaData.getPrimaryKey());
                    Set<ColumnMetadata> allColumns = com.google.common.collect.Sets.newHashSet(tableMetaData.getColumns());
                    boolean isKeyOnlyTable = (keyColumns.size() == allColumns.size());
                    //With compact storage
                    if (!isKeyOnlyTable && (keyColumns.size() == (allColumns.size() - 1)))
                    {
                        com.google.common.collect.Sets.SetView diff = com.google.common.collect.Sets.difference(allColumns, keyColumns);
                        for (Object obj : diff)
                        {
                            ColumnMetadata col = (ColumnMetadata)obj;
                            isKeyOnlyTable = col.getName().isEmpty();
                            break;
                        }
                    }

                    //Non PK Columns
                    StringBuilder sb = new StringBuilder();
                    if (!isKeyOnlyTable)
                    {
                        sb.append("UPDATE ").append(quoteIdentifier(tableName)).append(" SET ");
                        //PK Columns
                        StringBuilder pred = new StringBuilder();
                        pred.append(" WHERE ");

                        boolean firstCol = true;
                        boolean firstPred = true;
                        for (ColumnMetadata c : tableMetaData.getColumns()) {

                            if (keyColumns.contains(c)) {
                                if (firstPred)
                                    firstPred = false;
                                else
                                    pred.append(" AND ");

                                pred.append(quoteIdentifier(c.getName())).append(" = ?");
                            } else {
                                if (firstCol)
                                    firstCol = false;
                                else
                                    sb.append(',');

                                sb.append(quoteIdentifier(c.getName())).append(" = ");

                                switch (c.getType().getName())
                                {
                                case SET:
                                case LIST:
                                case COUNTER:
                                    sb.append(quoteIdentifier(c.getName())).append(" + ?");
                                    break;
                                default:
                                    sb.append("?");
                                    break;
                                }
                            }
                        }

                        //Put PK predicates at the end
                        sb.append(pred);
                    }
                    else
                    {
                        sb.append("INSERT INTO ").append(quoteIdentifier(tableName)).append(" (");
                        StringBuilder value = new StringBuilder();
                        for (ColumnMetadata c : tableMetaData.getPrimaryKey())
                        {
                            sb.append(quoteIdentifier(c.getName())).append(", ");
                            value.append("?, ");
                        }
                        sb.delete(sb.lastIndexOf(","), sb.length());
                        value.delete(value.lastIndexOf(","), value.length());
                        sb.append(") ").append("values(").append(value).append(')');
                    }

                    if (insert == null)
                        insert = new HashMap<>();
                    lowerCase(insert);

                    partitions = select(settings.insert.batchsize, "partitions", "fixed(1)", insert, OptionDistribution.BUILDER);
                    selectchance = select(settings.insert.selectRatio, "select", "fixed(1)/1", insert, OptionRatioDistribution.BUILDER);
                    rowPopulation = select(settings.insert.rowPopulationRatio, "row-population", "fixed(1)/1", insert, OptionRatioDistribution.BUILDER);
                    batchType = settings.insert.batchType != null
                                ? settings.insert.batchType
                                : !insert.containsKey("batchtype")
                                  ? BatchStatement.Type.LOGGED
                                  : BatchStatement.Type.valueOf(insert.remove("batchtype"));
                    if (!insert.isEmpty())
                        throw new IllegalArgumentException("Unrecognised insert option(s): " + insert);

                    Distribution visits = settings.insert.visits.get();
                    // these min/max are not absolutely accurate if selectchance < 1, but they're close enough to
                    // guarantee the vast majority of actions occur in these bounds
                    double minBatchSize = selectchance.get().min() * partitions.get().minValue() * generator.minRowCount * (1d / visits.maxValue());
                    double maxBatchSize = selectchance.get().max() * partitions.get().maxValue() * generator.maxRowCount * (1d / visits.minValue());

                    if (generator.maxRowCount > 100 * 1000 * 1000)
                        System.err.printf("WARNING: You have defined a schema that permits very large partitions (%.0f max rows (>100M))%n", generator.maxRowCount);
                    if (batchType == BatchStatement.Type.LOGGED && maxBatchSize > 65535)
                    {
                        System.err.printf("ERROR: You have defined a workload that generates batches with more than 65k rows (%.0f), but have required the use of LOGGED batches. There is a 65k row limit on a single batch.%n",
                                          selectchance.get().max() * partitions.get().maxValue() * generator.maxRowCount);
                        System.exit(1);
                    }
                    if (maxBatchSize > 100000)
                        System.err.printf("WARNING: You have defined a schema that permits very large batches (%.0f max rows (>100K)). This may OOM this stress client, or the server.%n",
                                          selectchance.get().max() * partitions.get().maxValue() * generator.maxRowCount);

                    JavaDriverClient client = settings.getJavaDriverClient();
                    String query = sb.toString();

                    if (settings.mode.api != ConnectionAPI.JAVA_DRIVER_NATIVE)
                    {
                        try
                        {
                            thriftInsertId = settings.getThriftClient().prepare_cql3_query(query, Compression.NONE);
                        }
                        catch (TException e)
                        {
                            throw new RuntimeException(e);
                        }
                    }

                    insertStatement = client.prepare(query);
                }
            }
        }

        return new SchemaInsert(timer, settings, generator, seedManager, partitions.get(), selectchance.get(), rowPopulation.get(), thriftInsertId, insertStatement, ThriftConversion.fromThrift(settings.command.consistencyLevel), batchType);
    }

    public List<ValidatingSchemaQuery> getValidate(Timer timer, PartitionGenerator generator, SeedManager seedManager, StressSettings settings)
    {
        if (validationFactories == null)
        {
            synchronized (this)
            {
                if (validationFactories == null)
                {
                    maybeLoadSchemaInfo(settings);
                    validationFactories = ValidatingSchemaQuery.create(tableMetaData, settings);
                }
            }
        }

        List<ValidatingSchemaQuery> queries = new ArrayList<>();
        for (ValidatingSchemaQuery.Factory factory : validationFactories)
            queries.add(factory.create(timer, settings, generator, seedManager, ThriftConversion.fromThrift(settings.command.consistencyLevel)));
        return queries;
    }

    private static <E> E select(E first, String key, String defValue, Map<String, String> map, Function<String, E> builder)
    {
        String val = map.remove(key);

        if (first != null)
            return first;
        if (val != null && val.trim().length() > 0)
            return builder.apply(val);

        return builder.apply(defValue);
    }

    public PartitionGenerator newGenerator(StressSettings settings)
    {
        if (generatorFactory == null)
        {
            synchronized (this)
            {
                maybeCreateSchema(settings);
                maybeLoadSchemaInfo(settings);
                if (generatorFactory == null)
                    generatorFactory = new GeneratorFactory();
            }
        }

        return generatorFactory.newGenerator(settings);
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
                partitionKeys.add(new ColumnInfo(metadata.getName(), metadata.getType().getName().toString(),
                                                 metadata.getType().isCollection() ? metadata.getType().getTypeArguments().get(0).getName().toString() : "",
                                                 columnConfigs.get(metadata.getName())));
            for (ColumnMetadata metadata : tableMetaData.getClusteringColumns())
                clusteringColumns.add(new ColumnInfo(metadata.getName(), metadata.getType().getName().toString(),
                                                     metadata.getType().isCollection() ? metadata.getType().getTypeArguments().get(0).getName().toString() : "",
                                                     columnConfigs.get(metadata.getName())));
            for (ColumnMetadata metadata : tableMetaData.getColumns())
                if (!keyColumns.contains(metadata))
                    valueColumns.add(new ColumnInfo(metadata.getName(), metadata.getType().getName().toString(),
                                                    metadata.getType().isCollection() ? metadata.getType().getTypeArguments().get(0).getName().toString() : "",
                                                    columnConfigs.get(metadata.getName())));
        }

        PartitionGenerator newGenerator(StressSettings settings)
        {
            return new PartitionGenerator(get(partitionKeys), get(clusteringColumns), get(valueColumns), settings.generate.order);
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
        final String type;
        final String collectionType;
        final GeneratorConfig config;

        ColumnInfo(String name, String type, String collectionType, GeneratorConfig config)
        {
            this.name = name;
            this.type = type;
            this.collectionType = collectionType;
            this.config = config;
        }

        Generator getGenerator()
        {
            return getGenerator(name, type, collectionType, config);
        }

        static Generator getGenerator(final String name, final String type, final String collectionType, GeneratorConfig config)
        {
            switch (type.toUpperCase())
            {
                case "ASCII":
                case "TEXT":
                case "VARCHAR":
                    return new Strings(name, config);
                case "BIGINT":
                case "COUNTER":
                    return new Longs(name, config);
                case "BLOB":
                    return new Bytes(name, config);
                case "BOOLEAN":
                    return new Booleans(name, config);
                case "DECIMAL":
                    return new BigDecimals(name, config);
                case "DOUBLE":
                    return new Doubles(name, config);
                case "FLOAT":
                    return new Floats(name, config);
                case "INET":
                    return new Inets(name, config);
                case "INT":
                    return new Integers(name, config);
                case "VARINT":
                    return new BigIntegers(name, config);
                case "TIMESTAMP":
                    return new Dates(name, config);
                case "UUID":
                    return new UUIDs(name, config);
                case "TIMEUUID":
                    return new TimeUUIDs(name, config);
                case "TINYINT":
                    return new TinyInts(name, config);
                case "SMALLINT":
                    return new SmallInts(name, config);
                case "TIME":
                    return new Times(name, config);
                case "DATE":
                    return new LocalDates(name, config);
                case "SET":
                    return new Sets(name, getGenerator(name, collectionType, null, config), config);
                case "LIST":
                    return new Lists(name, getGenerator(name, collectionType, null, config), config);
                default:
                    throw new UnsupportedOperationException("Because of this name: "+name+" if you removed it from the yaml and are still seeing this, make sure to drop table");
            }
        }
    }

    public static StressProfile load(URI file) throws IOError
    {
        try
        {
            Constructor constructor = new Constructor(StressYaml.class);

            Yaml yaml = new Yaml(constructor);

            InputStream yamlStream = file.toURL().openStream();

            if (yamlStream.available() == 0)
                throw new IOException("Unable to load yaml file from: "+file);

            StressYaml profileYaml = yaml.loadAs(yamlStream, StressYaml.class);

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

    /* Quote a identifier if it contains uppercase letters */
    private static String quoteIdentifier(String identifier)
    {
        return lowercaseAlphanumeric.matcher(identifier).matches() ? identifier : '\"'+identifier+ '\"';
    }
}
