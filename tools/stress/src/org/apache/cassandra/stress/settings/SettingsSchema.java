package org.apache.cassandra.stress.settings;
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


import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.*;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.thrift.*;
import org.apache.cassandra.utils.ByteBufferUtil;

public class SettingsSchema implements Serializable
{

    public static final String DEFAULT_COMPARATOR = "AsciiType";
    public static final String DEFAULT_VALIDATOR  = "BytesType";

    private final String replicationStrategy;
    private final Map<String, String> replicationStrategyOptions;

    private final IndexType indexType;
    private final boolean replicateOnWrite;
    private final String compression;
    private final String compactionStrategy;
    public final String keyspace;
    public final String columnFamily;

    public SettingsSchema(Options options)
    {
        replicateOnWrite = !options.noReplicateOnWrite.setByUser();
        replicationStrategy = options.replication.getStrategy();
        replicationStrategyOptions = options.replication.getOptions();
        if (options.index.setByUser())
            indexType = IndexType.valueOf(options.index.value().toUpperCase());
        else
            indexType = null;
        compression = options.compression.value();
        compactionStrategy = options.compactionStrategy.value();
        if (compactionStrategy != null)
        {
            try
            {
                CFMetaData.createCompactionStrategy(compactionStrategy);
            } catch (ConfigurationException e)
            {
                throw new IllegalArgumentException("Invalid compaction strategy: " + compactionStrategy);
            }
        }
        keyspace = options.keyspace.value();
        columnFamily = options.columnFamily.value();
    }

    private void createKeyspacesCql3(StressSettings settings)
    {
//        settings.getJavaDriverClient().execute("create table Standard1")
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
        CfDef standardCfDef = new CfDef(keyspace, columnFamily);
        Map<String, String> compressionOptions = new HashMap<String, String>();
        if (compression != null)
            compressionOptions.put("sstable_compression", compression);

        String comparator = settings.columns.comparator;
        standardCfDef.setComparator_type(comparator)
                .setDefault_validation_class(DEFAULT_VALIDATOR)
                .setCompression_options(compressionOptions);

        if (!settings.columns.useTimeUUIDComparator)
        {
            for (int i = 0; i < settings.columns.maxColumnsPerKey; i++)
            {
                standardCfDef.addToColumn_metadata(new ColumnDef(ByteBufferUtil.bytes("C" + i), "BytesType"));
            }
        }

        if (indexType != null)
        {
            ColumnDef standardColumn = new ColumnDef(ByteBufferUtil.bytes("C1"), "BytesType");
            standardColumn.setIndex_type(indexType).setIndex_name("Idx1");
            standardCfDef.setColumn_metadata(Arrays.asList(standardColumn));
        }

        // column family with super columns
        CfDef superCfDef = new CfDef(keyspace, "Super1")
                .setColumn_type("Super");
        superCfDef.setComparator_type(DEFAULT_COMPARATOR)
                .setSubcomparator_type(comparator)
                .setDefault_validation_class(DEFAULT_VALIDATOR)
                .setCompression_options(compressionOptions);

        // column family for standard counters
        CfDef counterCfDef = new CfDef(keyspace, "Counter1")
                .setComparator_type(comparator)
                .setDefault_validation_class("CounterColumnType")
                .setReplicate_on_write(replicateOnWrite)
                .setCompression_options(compressionOptions);

        // column family with counter super columns
        CfDef counterSuperCfDef = new CfDef(keyspace, "SuperCounter1")
                .setComparator_type(comparator)
                .setDefault_validation_class("CounterColumnType")
                .setReplicate_on_write(replicateOnWrite)
                .setColumn_type("Super")
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
            superCfDef.setCompaction_strategy(compactionStrategy);
            counterCfDef.setCompaction_strategy(compactionStrategy);
            counterSuperCfDef.setCompaction_strategy(compactionStrategy);
        }

        ksdef.setCf_defs(new ArrayList<CfDef>(Arrays.asList(standardCfDef, superCfDef, counterCfDef, counterSuperCfDef)));

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
            Thread.sleep(settings.node.nodes.size() * 1000); // seconds
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
        final OptionSimple index = new OptionSimple("index=", "KEYS|CUSTOM|COMPOSITES", null, "Type of index to create on needed column families (KEYS)", false);
        final OptionSimple keyspace = new OptionSimple("keyspace=", ".*", "Keyspace1", "The keyspace name to use", false);
        final OptionSimple columnFamily = new OptionSimple("columnfamily=", ".*", "Standard1", "The column family name to use", false);
        final OptionSimple compactionStrategy = new OptionSimple("compaction=", ".*", null, "The compaction strategy to use", false);
        final OptionSimple noReplicateOnWrite = new OptionSimple("no-replicate-on-write", "", null, "Set replicate_on_write to false for counters. Only counter add with CL=ONE will work", false);
        final OptionSimple compression = new OptionSimple("compression=", ".*", null, "Specify the compression to use for sstable, default:no compression", false);

        @Override
        public List<? extends Option> options()
        {
            return Arrays.asList(replication, index, keyspace, columnFamily, compactionStrategy, noReplicateOnWrite, compression);
        }
    }

    // CLI Utility Methods

    public static SettingsSchema get(Map<String, String[]> clArgs)
    {
        String[] params = clArgs.remove("-schema");
        if (params == null)
            return new SettingsSchema(new Options());

        GroupedOptions options = GroupedOptions.select(params, new Options());
        if (options == null)
        {
            printHelp();
            System.out.println("Invalid -schema options provided, see output for valid options");
            System.exit(1);
        }
        return new SettingsSchema((Options) options);
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
