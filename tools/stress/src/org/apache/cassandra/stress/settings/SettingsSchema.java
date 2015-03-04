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
package org.apache.cassandra.stress.settings;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.*;

import org.apache.cassandra.thrift.*;
import org.apache.cassandra.utils.ByteBufferUtil;

public class SettingsSchema implements Serializable
{

    public static final String DEFAULT_VALIDATOR  = "BytesType";

    private final String replicationStrategy;
    private final Map<String, String> replicationStrategyOptions;

    private final String compression;
    private final String compactionStrategy;
    private final Map<String, String> compactionStrategyOptions;
    public final String keyspace;

    public SettingsSchema(Options options, SettingsCommand command)
    {
        if (command instanceof SettingsCommandUser)
            keyspace = ((SettingsCommandUser) command).profile.keyspaceName;
        else
            keyspace = options.keyspace.value();

        replicationStrategy = options.replication.getStrategy();
        replicationStrategyOptions = options.replication.getOptions();
        compression = options.compression.value();
        compactionStrategy = options.compaction.getStrategy();
        compactionStrategyOptions = options.compaction.getOptions();
    }

    public void createKeySpaces(StressSettings settings)
    {
        createKeySpacesThrift(settings);
    }


    /**
     * Create Keyspace with Standard and Super/Counter column families
     */
    public void createKeySpacesThrift(StressSettings settings)
    {
        KsDef ksdef = new KsDef();

        // column family for standard columns
        CfDef standardCfDef = new CfDef(keyspace, "standard1");
        Map<String, String> compressionOptions = new HashMap<>();
        if (compression != null)
            compressionOptions.put("sstable_compression", compression);

        String comparator = settings.columns.comparator;
        standardCfDef.setComparator_type(comparator)
                .setDefault_validation_class(DEFAULT_VALIDATOR)
                .setCompression_options(compressionOptions);

        for (int i = 0; i < settings.columns.names.size(); i++)
            standardCfDef.addToColumn_metadata(new ColumnDef(settings.columns.names.get(i), "BytesType"));

        // column family for standard counters
        CfDef counterCfDef = new CfDef(keyspace, "counter1")
                .setComparator_type(comparator)
                .setDefault_validation_class("CounterColumnType")
                .setCompression_options(compressionOptions);

        ksdef.setName(keyspace);
        ksdef.setStrategy_class(replicationStrategy);

        if (!replicationStrategyOptions.isEmpty())
        {
            ksdef.setStrategy_options(replicationStrategyOptions);
        }

        if (compactionStrategy != null)
        {
            standardCfDef.setCompaction_strategy(compactionStrategy);
            counterCfDef.setCompaction_strategy(compactionStrategy);
            if (!compactionStrategyOptions.isEmpty())
            {
                standardCfDef.setCompaction_strategy_options(compactionStrategyOptions);
                counterCfDef.setCompaction_strategy_options(compactionStrategyOptions);
            }
        }

        ksdef.setCf_defs(new ArrayList<>(Arrays.asList(standardCfDef, counterCfDef)));

        Cassandra.Client client = settings.getRawThriftClient(false);

        try
        {
            client.system_add_keyspace(ksdef);

            /* CQL3 counter cf */
            client.set_cql_version("3.0.0"); // just to create counter cf for cql3

            client.set_keyspace(keyspace);
            client.execute_cql3_query(createCounterCFStatementForCQL3(settings), Compression.NONE, ConsistencyLevel.ONE);

            if (settings.mode.cqlVersion.isCql())
                client.set_cql_version(settings.mode.cqlVersion.connectVersion);
            /* end */

            System.out.println(String.format("Created keyspaces. Sleeping %ss for propagation.", settings.node.nodes.size()));
            Thread.sleep(settings.node.nodes.size() * 1000L); // seconds
        }
        catch (InvalidRequestException e)
        {
            System.err.println("Unable to create stress keyspace: " + e.getWhy());
        }
        catch (Exception e)
        {
            System.err.println("!!!! " + e.getMessage());
        }
    }

    private ByteBuffer createCounterCFStatementForCQL3(StressSettings options)
    {
        StringBuilder counter3 = new StringBuilder("CREATE TABLE \"Counter3\" (KEY blob PRIMARY KEY, ");

        for (int i = 0; i < options.columns.maxColumnsPerKey; i++)
        {
            counter3.append("c").append(i).append(" counter");
            if (i != options.columns.maxColumnsPerKey - 1)
                counter3.append(", ");
        }
        counter3.append(");");

        return ByteBufferUtil.bytes(counter3.toString());
    }

    // Option Declarations

    private static final class Options extends GroupedOptions
    {
        final OptionReplication replication = new OptionReplication();
        final OptionCompaction compaction = new OptionCompaction();
        final OptionSimple keyspace = new OptionSimple("keyspace=", ".*", "keyspace1", "The keyspace name to use", false);
        final OptionSimple compression = new OptionSimple("compression=", ".*", null, "Specify the compression to use for sstable, default:no compression", false);

        @Override
        public List<? extends Option> options()
        {
            return Arrays.asList(replication, keyspace, compaction, compression);
        }
    }

    // CLI Utility Methods

    public static SettingsSchema get(Map<String, String[]> clArgs, SettingsCommand command)
    {
        String[] params = clArgs.remove("-schema");
        if (params == null)
            return new SettingsSchema(new Options(), command);

        if (command instanceof SettingsCommandUser)
            throw new IllegalArgumentException("-schema can only be provided with predefined operations insert, read, etc.; the 'user' command requires a schema yaml instead");

        GroupedOptions options = GroupedOptions.select(params, new Options());
        if (options == null)
        {
            printHelp();
            System.out.println("Invalid -schema options provided, see output for valid options");
            System.exit(1);
        }
        return new SettingsSchema((Options) options, command);
    }

    public static void printHelp()
    {
        GroupedOptions.printOptions(System.out, "-schema", new Options());
    }

    public static Runnable helpPrinter()
    {
        return new Runnable()
        {
            @Override
            public void run()
            {
                printHelp();
            }
        };
    }

}
