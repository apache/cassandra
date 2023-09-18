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
import java.nio.charset.CharacterCodingException;
import java.util.*;

import com.datastax.driver.core.exceptions.AlreadyExistsException;
import org.apache.cassandra.stress.util.JavaDriverClient;
import org.apache.cassandra.stress.util.ResultLogger;
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
            keyspace = null; //this should never be used - StressProfile passes keyspace name directly
        else
            keyspace = options.keyspace.value();

        replicationStrategy = options.replication.getStrategy();
        replicationStrategyOptions = options.replication.getOptions();
        compression = options.compression.value();
        compactionStrategy = options.compaction.getStrategy();
        compactionStrategyOptions = options.compaction.getOptions();
    }

    /**
     * Create Keyspace with Standard and Super/Counter column families
     */
    public void createKeySpaces(StressSettings settings)
    {

        JavaDriverClient client  = settings.getJavaDriverClient(false);

        try
        {
            //Keyspace
            client.execute(createKeyspaceStatementCQL3(), org.apache.cassandra.db.ConsistencyLevel.LOCAL_ONE);

            client.execute("USE \""+keyspace+"\"", org.apache.cassandra.db.ConsistencyLevel.LOCAL_ONE);

            //Add standard1 and counter1
            client.execute(createStandard1StatementCQL3(settings), org.apache.cassandra.db.ConsistencyLevel.LOCAL_ONE);
            client.execute(createCounter1StatementCQL3(settings), org.apache.cassandra.db.ConsistencyLevel.LOCAL_ONE);

            System.out.println(String.format("Created keyspaces. Sleeping %ss for propagation.", settings.node.nodes.size()));
            Thread.sleep(settings.node.nodes.size() * 1000L); // seconds
        }
        catch (AlreadyExistsException e)
        {
            //Ok.
        }
        catch (Exception e)
        {
            throw new RuntimeException("Encountered exception creating schema", e);
        }
    }

    String createKeyspaceStatementCQL3()
    {
        StringBuilder b = new StringBuilder();

        //Create Keyspace
        b.append("CREATE KEYSPACE IF NOT EXISTS \"")
         .append(keyspace)
         .append("\" WITH replication = {'class': '")
         .append(replicationStrategy)
         .append("'");

        if (replicationStrategyOptions.isEmpty())
        {
            b.append(", 'replication_factor': '1'}");
        }
        else
        {
            for(Map.Entry<String, String> entry : replicationStrategyOptions.entrySet())
            {
                b.append(", '").append(entry.getKey()).append("' : '").append(entry.getValue()).append("'");
            }

            b.append("}");
        }

        b.append(" AND durable_writes = true;\n");

        return b.toString();
    }

    String createStandard1StatementCQL3(StressSettings settings)
    {

        StringBuilder b = new StringBuilder();

        b.append("CREATE TABLE IF NOT EXISTS ")
         .append("standard1 (key blob PRIMARY KEY ");

        try
        {
            for (ByteBuffer name : settings.columns.names)
                b.append("\n, \"").append(ByteBufferUtil.string(name)).append("\" blob");
        }
        catch (CharacterCodingException e)
        {
            throw new RuntimeException(e);
        }

        //Compression
        b.append(") WITH compression = {");
        if (compression != null)
            b.append("'class' : '").append(compression).append("'");

        b.append("}");

        //Compaction
        if (compactionStrategy != null)
        {
            b.append(" AND compaction = { 'class' : '").append(compactionStrategy).append("'");

            for (Map.Entry<String, String> entry : compactionStrategyOptions.entrySet())
                b.append(", '").append(entry.getKey()).append("' : '").append(entry.getValue()).append("'");

            b.append("}");
        }

        b.append(";\n");

        return b.toString();
    }

    String createCounter1StatementCQL3(StressSettings settings)
    {

        StringBuilder b = new StringBuilder();

        b.append("CREATE TABLE IF NOT EXISTS ")
         .append("counter1 (key blob PRIMARY KEY,");

        try
        {
            for (ByteBuffer name : settings.columns.names)
                b.append("\n, \"").append(ByteBufferUtil.string(name)).append("\" counter");
        }
        catch (CharacterCodingException e)
        {
            throw new RuntimeException(e);
        }

        //Compression
        b.append(") WITH compression = {");
        if (compression != null)
            b.append("'class' : '").append(compression).append("'");

        b.append("}");

        //Compaction
        if (compactionStrategy != null)
        {
            b.append(" AND compaction = { 'class' : '").append(compactionStrategy).append("'");

            for (Map.Entry<String, String> entry : compactionStrategyOptions.entrySet())
                b.append(", '").append(entry.getKey()).append("' : '").append(entry.getValue()).append("'");

            b.append("}");
        }

        b.append(";\n");

        return b.toString();
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
    public void printSettings(ResultLogger out)
    {
        out.println("  Keyspace: " + keyspace);
        out.println("  Replication Strategy: " + replicationStrategy);
        out.println("  Replication Strategy Options: " + replicationStrategyOptions);

        out.println("  Table Compression: " + compression);
        out.println("  Table Compaction Strategy: " + compactionStrategy);
        out.println("  Table Compaction Strategy Options: " + compactionStrategyOptions);
    }


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
