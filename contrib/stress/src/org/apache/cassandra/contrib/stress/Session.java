/**
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
package org.apache.cassandra.contrib.stress;

import org.apache.cassandra.db.ColumnFamilyType;
import org.apache.cassandra.thrift.*;
import org.apache.commons.cli.*;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicIntegerArray;
import java.util.concurrent.atomic.AtomicLongArray;

public class Session
{
    // command line options
    public static final Options availableOptions = new Options();

    public final AtomicIntegerArray operationCount;
    public final AtomicIntegerArray keyCount;
    public final AtomicLongArray latencies;

    static
    {
        availableOptions.addOption("h",  "help",                 false,  "Show this help message and exit");
        availableOptions.addOption("n",  "num-keys",             true,   "Number of keys, default:1000000");
        availableOptions.addOption("N",  "skip-keys",            true,   "Fraction of keys to skip initially, default:0");
        availableOptions.addOption("t",  "threads",              true,   "Number of threads to use, default:50");
        availableOptions.addOption("c",  "columns",              true,   "Number of columns per key, default:5");
        availableOptions.addOption("S",  "column-size",          true,   "Size of column values in bytes, default:34");
        availableOptions.addOption("C",  "cardinality",          true,   "Number of unique values stored in columns, default:50");
        availableOptions.addOption("d",  "nodes",                true,   "Host nodes (comma separated), default:locahost");
        availableOptions.addOption("s",  "stdev",                true,   "Standard Deviation Factor, default:0.1");
        availableOptions.addOption("r",  "random",               false,  "Use random key generator (STDEV will have no effect), default:false");
        availableOptions.addOption("f",  "file",                 true,   "Write output to given file");
        availableOptions.addOption("p",  "port",                 true,   "Thrift port, default:9160");
        availableOptions.addOption("m",  "unframed",             false,  "Use unframed transport, default:false");
        availableOptions.addOption("o",  "operation",            true,   "Operation to perform (INSERT, READ, RANGE_SLICE, INDEXED_RANGE_SLICE, MULTI_GET), default:INSERT");
        availableOptions.addOption("u",  "supercolumns",         true,   "Number of super columns per key, default:1");
        availableOptions.addOption("y",  "family-type",          true,   "Column Family Type (Super, Standard), default:Standard");
        availableOptions.addOption("k",  "keep-going",           false,  "Ignore errors inserting or reading, default:false");
        availableOptions.addOption("i",  "progress-interval",    true,   "Progress Report Interval (seconds), default:10");
        availableOptions.addOption("g",  "keys-per-call",        true,   "Number of keys to get_range_slices or multiget per call, default:1000");
        availableOptions.addOption("l",  "replication-factor",   true,   "Replication Factor to use when creating needed column families, default:1");
        availableOptions.addOption("e",  "consistency-level",    true,   "Consistency Level to use (ONE, QUORUM, LOCAL_QUORUM, EACH_QUORUM, ALL, ANY), default:ONE");
        availableOptions.addOption("x",  "create-index",         true,   "Type of index to create on needed column families (KEYS)");
    }

    private int numKeys          = 1000 * 1000;
    private float skipKeys       = 0;
    private int threads          = 50;
    private int columns          = 5;
    private int columnSize       = 34;
    private int cardinality      = 50;
    private String[] nodes       = new String[] { "127.0.0.1" };
    private boolean random       = false;
    private boolean unframed     = false;
    private boolean ignoreErrors = false;
    private int port             = 9160;
    private int superColumns     = 1;

    private int progressInterval  = 10;
    private int keysPerCall       = 1000;
    private int replicationFactor = 1;

    private PrintStream out = System.out;

    private IndexType indexType = null;
    private Stress.Operation operation = Stress.Operation.INSERT;
    private ColumnFamilyType columnFamilyType = ColumnFamilyType.Standard;
    private ConsistencyLevel consistencyLevel = ConsistencyLevel.ONE;

    // required by Gaussian distribution.
    protected int   mean;
    protected float sigma;

    public Session(String[] arguments) throws IllegalArgumentException
    {
        float STDev = 0.1f;
        CommandLineParser parser = new PosixParser();

        try
        {
            CommandLine cmd = parser.parse(availableOptions, arguments);

            if (cmd.hasOption("h"))
                throw new IllegalArgumentException("help");

            if (cmd.hasOption("n"))
                numKeys = Integer.parseInt(cmd.getOptionValue("n"));

            if (cmd.hasOption("N"))
                skipKeys = Float.parseFloat(cmd.getOptionValue("N"));

            if (cmd.hasOption("t"))
                threads = Integer.parseInt(cmd.getOptionValue("t"));

            if (cmd.hasOption("c"))
                columns = Integer.parseInt(cmd.getOptionValue("c"));

            if (cmd.hasOption("S"))
                columnSize = Integer.parseInt(cmd.getOptionValue("S"));

            if (cmd.hasOption("C"))
                cardinality = Integer.parseInt(cmd.getOptionValue("t"));

            if (cmd.hasOption("d"))
                nodes = cmd.getOptionValue("d").split(",");

            if (cmd.hasOption("s"))
                STDev = Float.parseFloat(cmd.getOptionValue("s"));

            if (cmd.hasOption("r"))
                random = Boolean.parseBoolean(cmd.getOptionValue("r"));

            if (cmd.hasOption("f"))
            {
                try
                {
                    out = new PrintStream(new FileOutputStream(cmd.getOptionValue("f")));
                }
                catch (FileNotFoundException e)
                {
                    System.out.println(e.getMessage());
                }
            }

            if (cmd.hasOption("p"))
                port = Integer.parseInt(cmd.getOptionValue("p"));

            if (cmd.hasOption("m"))
                unframed = Boolean.parseBoolean(cmd.getOptionValue("m"));

            if (cmd.hasOption("o"))
                operation = Stress.Operation.valueOf(cmd.getOptionValue("o").toUpperCase());

            if (cmd.hasOption("u"))
                superColumns = Integer.parseInt(cmd.getOptionValue("u"));

            if (cmd.hasOption("y"))
                columnFamilyType = ColumnFamilyType.valueOf(cmd.getOptionValue("y"));

            if (cmd.hasOption("k"))
                ignoreErrors = true;

            if (cmd.hasOption("i"))
                progressInterval = Integer.parseInt(cmd.getOptionValue("i"));

            if (cmd.hasOption("g"))
                keysPerCall = Integer.parseInt(cmd.getOptionValue("g"));

            if (cmd.hasOption("l"))
                replicationFactor = Integer.parseInt(cmd.getOptionValue("l"));

            if (cmd.hasOption("e"))
                consistencyLevel = ConsistencyLevel.valueOf(cmd.getOptionValue("e").toUpperCase());

            if (cmd.hasOption("x"))
                indexType = IndexType.valueOf(cmd.getOptionValue("x").toUpperCase());
        }
        catch (ParseException e)
        {
            throw new IllegalArgumentException(e.getMessage(), e);
        }

        mean  = numKeys / 2;
        sigma = numKeys * STDev;

        operationCount = new AtomicIntegerArray(threads);
        keyCount = new AtomicIntegerArray(threads);
        latencies = new AtomicLongArray(threads);
    }

    public int getCardinality()
    {
        return cardinality;
    }

    public int getColumnSize()
    {
        return columnSize;
    }

    public boolean isUnframed()
    {
        return unframed;
    }

    public int getColumnsPerKey()
    {
        return columns;
    }

    public ColumnFamilyType getColumnFamilyType()
    {
        return columnFamilyType;
    }

    public int getNumKeys()
    {
        return numKeys;
    }

    public int getThreads()
    {
        return threads;
    }

    public float getSkipKeys()
    {
        return skipKeys;
    }

    public int getSuperColumns()
    {
        return superColumns;
    }

    public int getKeysPerThread()
    {
        return numKeys / threads;
    }

    public int getTotalKeysLength()
    {
        return Integer.toString(numKeys).length();
    }

    public ConsistencyLevel getConsistencyLevel()
    {
        return consistencyLevel;
    }

    public boolean ignoreErrors()
    {
        return ignoreErrors;
    }

    public Stress.Operation getOperation()
    {
        return operation;
    }

    public PrintStream getOutputStream()
    {
        return out;
    }

    public int getProgressInterval()
    {
        return progressInterval;
    }

    public boolean useRandomGenerator()
    {
        return random;
    }

    public int getKeysPerCall()
    {
        return keysPerCall;
    }

    // required by Gaussian distribution
    public int getMean()
    {
        return mean;
    }

    // required by Gaussian distribution
    public float getSigma()
    {
        return sigma;
    }

    /**
     * Create Keyspace1 with Standard1 and Super1 column families
     */
    public void createKeySpaces()
    {
        KsDef keyspace = new KsDef();
        ColumnDef standardColumn = new ColumnDef(ByteBuffer.wrap("C1".getBytes()), "UTF8Type");
        ColumnDef superSubColumn = new ColumnDef(ByteBuffer.wrap("S1".getBytes()), "UTF8Type");

        if (indexType != null)
            standardColumn.setIndex_type(indexType).setIndex_name("Idx1");

        // column family for standard columns
        CfDef standardCfDef = new CfDef("Keyspace1", "Standard1").setColumn_metadata(Arrays.asList(standardColumn));

        // column family with super columns
        CfDef superCfDef = new CfDef("Keyspace1", "Super1").setColumn_metadata(Arrays.asList(superSubColumn)).setColumn_type("Super");

        keyspace.setName("Keyspace1");
        keyspace.setStrategy_class("org.apache.cassandra.locator.SimpleStrategy");
        keyspace.setReplication_factor(replicationFactor);
        keyspace.setCf_defs(new ArrayList<CfDef>(Arrays.asList(standardCfDef, superCfDef)));

        Cassandra.Client client = getClient(false);

        try
        {
            client.system_add_keyspace(keyspace);
            out.println(String.format("Created keyspaces. Sleeping %ss for propagation.", nodes.length));

            Thread.sleep(nodes.length * 1000); // seconds
        }
        catch (InvalidRequestException e)
        {
            out.println(e.getWhy());
        }
        catch (Exception e)
        {
            out.println(e.getMessage());
        }
    }

    /**
     * Thrift client connection with Keyspace1 set.
     * @return cassandra client connection
     */
    public Cassandra.Client getClient()
    {
        return getClient(true);
    }
    /**
     * Thrift client connection
     * @param setKeyspace - should we set keyspace for client or not
     * @return cassandra client connection
     */
    public Cassandra.Client getClient(boolean setKeyspace)
    {
        // random node selection for fake load balancing
        String currentNode = nodes[Stress.randomizer.nextInt(nodes.length)];

        TSocket socket = new TSocket(currentNode, port);
        TTransport transport = (isUnframed()) ? socket : new TFramedTransport(socket);
        Cassandra.Client client = new Cassandra.Client(new TBinaryProtocol(transport));

        try
        {
            transport.open();

            if (setKeyspace)
            {
                client.set_keyspace("Keyspace1");
            }
        }
        catch (InvalidRequestException e)
        {
            throw new RuntimeException(e.getWhy());
        }
        catch (Exception e)
        {
            throw new RuntimeException(e.getMessage());
        }

        return client;
    }

}
