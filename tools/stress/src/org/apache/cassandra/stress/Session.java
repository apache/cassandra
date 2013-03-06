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
package org.apache.cassandra.stress;

import java.io.*;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Histogram;
import org.apache.cassandra.cli.transport.FramedTransportFactory;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.EncryptionOptions;
import org.apache.cassandra.config.EncryptionOptions.ClientEncryptionOptions;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.exceptions.SyntaxException;
import org.apache.cassandra.db.marshal.*;
import org.apache.commons.cli.*;

import org.apache.cassandra.db.ColumnFamilyType;
import org.apache.cassandra.stress.util.CassandraClient;
import org.apache.cassandra.transport.SimpleClient;
import org.apache.cassandra.thrift.*;
import org.apache.commons.lang.StringUtils;

import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportFactory;

public class Session implements Serializable
{
    // command line options
    public static final Options availableOptions = new Options();

    public static final String KEYSPACE_NAME = "Keyspace1";
    public static final String DEFAULT_COMPARATOR = "AsciiType";
    public static final String DEFAULT_VALIDATOR  = "BytesType";

    private static InetAddress localInetAddress;

    public final AtomicInteger operations = new AtomicInteger();
    public final AtomicInteger keys = new AtomicInteger();
    public final com.yammer.metrics.core.Timer latency = Metrics.newTimer(Session.class, "latency");

    private static final String SSL_TRUSTSTORE = "truststore";
    private static final String SSL_TRUSTSTORE_PW = "truststore-password";
    private static final String SSL_PROTOCOL = "ssl-protocol";
    private static final String SSL_ALGORITHM = "ssl-alg";
    private static final String SSL_STORE_TYPE = "store-type";
    private static final String SSL_CIPHER_SUITES = "ssl-ciphers";

    static
    {
        availableOptions.addOption("h",  "help",                 false,  "Show this help message and exit");
        availableOptions.addOption("n",  "num-keys",             true,   "Number of keys, default:1000000");
        availableOptions.addOption("F",  "num-different-keys",   true,   "Number of different keys (if < NUM-KEYS, the same key will re-used multiple times), default:NUM-KEYS");
        availableOptions.addOption("N",  "skip-keys",            true,   "Fraction of keys to skip initially, default:0");
        availableOptions.addOption("t",  "threads",              true,   "Number of threads to use, default:50");
        availableOptions.addOption("c",  "columns",              true,   "Number of columns per key, default:5");
        availableOptions.addOption("S",  "column-size",          true,   "Size of column values in bytes, default:34");
        availableOptions.addOption("C",  "cardinality",          true,   "Number of unique values stored in columns, default:50");
        availableOptions.addOption("d",  "nodes",                true,   "Host nodes (comma separated), default:locahost");
        availableOptions.addOption("D",  "nodesfile",            true,   "File containing host nodes (one per line)");
        availableOptions.addOption("s",  "stdev",                true,   "Standard Deviation Factor, default:0.1");
        availableOptions.addOption("r",  "random",               false,  "Use random key generator (STDEV will have no effect), default:false");
        availableOptions.addOption("f",  "file",                 true,   "Write output to given file");
        availableOptions.addOption("p",  "port",                 true,   "Thrift port, default:9160");
        availableOptions.addOption("o",  "operation",            true,   "Operation to perform (INSERT, READ, RANGE_SLICE, INDEXED_RANGE_SLICE, MULTI_GET, COUNTER_ADD, COUNTER_GET), default:INSERT");
        availableOptions.addOption("u",  "supercolumns",         true,   "Number of super columns per key, default:1");
        availableOptions.addOption("y",  "family-type",          true,   "Column Family Type (Super, Standard), default:Standard");
        availableOptions.addOption("K",  "keep-trying",          true,   "Retry on-going operation N times (in case of failure). positive integer, default:10");
        availableOptions.addOption("k",  "keep-going",           false,  "Ignore errors inserting or reading (when set, --keep-trying has no effect), default:false");
        availableOptions.addOption("i",  "progress-interval",    true,   "Progress Report Interval (seconds), default:10");
        availableOptions.addOption("g",  "keys-per-call",        true,   "Number of keys to get_range_slices or multiget per call, default:1000");
        availableOptions.addOption("l",  "replication-factor",   true,   "Replication Factor to use when creating needed column families, default:1");
        availableOptions.addOption("L",  "enable-cql",           false,  "Perform queries using CQL2 (Cassandra Query Language v 2.0.0)");
        availableOptions.addOption("L3", "enable-cql3",          false,  "Perform queries using CQL3 (Cassandra Query Language v 3.0.0)");
        availableOptions.addOption("b",  "enable-native-protocol",  false,  "Use the binary native protocol (only work along with -L3)");
        availableOptions.addOption("P",  "use-prepared-statements", false, "Perform queries using prepared statements (only applicable to CQL).");
        availableOptions.addOption("e",  "consistency-level",    true,   "Consistency Level to use (ONE, QUORUM, LOCAL_QUORUM, EACH_QUORUM, ALL, ANY), default:ONE");
        availableOptions.addOption("x",  "create-index",         true,   "Type of index to create on needed column families (KEYS)");
        availableOptions.addOption("R",  "replication-strategy", true,   "Replication strategy to use (only on insert if keyspace does not exist), default:org.apache.cassandra.locator.SimpleStrategy");
        availableOptions.addOption("O",  "strategy-properties",  true,   "Replication strategy properties in the following format <dc_name>:<num>,<dc_name>:<num>,...");
        availableOptions.addOption("W",  "no-replicate-on-write",false,  "Set replicate_on_write to false for counters. Only counter add with CL=ONE will work");
        availableOptions.addOption("V",  "average-size-values",  false,  "Generate column values of average rather than specific size");
        availableOptions.addOption("T",  "send-to",              true,   "Send this as a request to the stress daemon at specified address.");
        availableOptions.addOption("I",  "compression",          true,   "Specify the compression to use for sstable, default:no compression");
        availableOptions.addOption("Q",  "query-names",          true,   "Comma-separated list of column names to retrieve from each row.");
        availableOptions.addOption("Z",  "compaction-strategy",  true,   "CompactionStrategy to use.");
        availableOptions.addOption("U",  "comparator",           true,   "Column Comparator to use. Currently supported types are: TimeUUIDType, AsciiType, UTF8Type.");
        availableOptions.addOption("tf", "transport-factory",    true,   "Fully-qualified TTransportFactory class name for creating a connection. Note: For Thrift over SSL, use org.apache.cassandra.stress.SSLTransportFactory.");
        availableOptions.addOption("ts", SSL_TRUSTSTORE,         true, "SSL: full path to truststore");
        availableOptions.addOption("tspw", SSL_TRUSTSTORE_PW,    true, "SSL: full path to truststore");
        availableOptions.addOption("prtcl", SSL_PROTOCOL,        true, "SSL: connections protocol to use (default: TLS)");
        availableOptions.addOption("alg", SSL_ALGORITHM,         true, "SSL: algorithm (default: SunX509)");
        availableOptions.addOption("st", SSL_STORE_TYPE,         true, "SSL: type of store");
        availableOptions.addOption("ciphers", SSL_CIPHER_SUITES, true, "SSL: comma-separated list of encryption suites to use");
    }

    private int numKeys          = 1000 * 1000;
    private int numDifferentKeys = numKeys;
    private float skipKeys       = 0;
    private int threads          = 50;
    private int columns          = 5;
    private int columnSize       = 34;
    private int cardinality      = 50;
    public String[] nodes        = new String[] { "127.0.0.1" };
    private boolean random       = false;
    private int retryTimes       = 10;
    public int port              = 9160;
    private int superColumns     = 1;
    private String compression   = null;
    private String compactionStrategy = null;

    private int progressInterval  = 10;
    private int keysPerCall       = 1000;
    private boolean replicateOnWrite = true;
    private boolean ignoreErrors  = false;
    private boolean enable_cql    = false;
    private boolean use_prepared  = false;
    private boolean trace         = false;
    public boolean use_native_protocol = false;

    private final String outFileName;

    private IndexType indexType = null;
    private Stress.Operations operation = Stress.Operations.INSERT;
    private ColumnFamilyType columnFamilyType = ColumnFamilyType.Standard;
    private ConsistencyLevel consistencyLevel = ConsistencyLevel.ONE;
    private String replicationStrategy = "org.apache.cassandra.locator.SimpleStrategy";
    private Map<String, String> replicationStrategyOptions = new HashMap<String, String>();

    // if we know exactly column names that we want to read (set by -Q option)
    public final List<ByteBuffer> columnNames;

    public String cqlVersion;

    public final boolean averageSizeValues;

    // required by Gaussian distribution.
    protected int   mean;
    protected float sigma;

    public final InetAddress sendToDaemon;
    public final String comparator;
    public final boolean timeUUIDComparator;
    public double traceProbability = 0.0;
    public EncryptionOptions encOptions = new ClientEncryptionOptions();
    public TTransportFactory transportFactory = new FramedTransportFactory();

    public Session(String[] arguments) throws IllegalArgumentException, SyntaxException
    {
        float STDev = 0.1f;
        CommandLineParser parser = new PosixParser();

        try
        {
            CommandLine cmd = parser.parse(availableOptions, arguments);

            if (cmd.getArgs().length > 0)
            {
                System.err.println("Application does not allow arbitrary arguments: " + StringUtils.join(cmd.getArgList(), ", "));
                System.exit(1);
            }

            if (cmd.hasOption("h"))
                throw new IllegalArgumentException("help");

            if (cmd.hasOption("n"))
                numKeys = Integer.parseInt(cmd.getOptionValue("n"));

            if (cmd.hasOption("F"))
                numDifferentKeys = Integer.parseInt(cmd.getOptionValue("F"));
            else
                numDifferentKeys = numKeys;

            if (cmd.hasOption("N"))
                skipKeys = Float.parseFloat(cmd.getOptionValue("N"));

            if (cmd.hasOption("t"))
                threads = Integer.parseInt(cmd.getOptionValue("t"));

            if (cmd.hasOption("c"))
                columns = Integer.parseInt(cmd.getOptionValue("c"));

            if (cmd.hasOption("S"))
                columnSize = Integer.parseInt(cmd.getOptionValue("S"));

            if (cmd.hasOption("C"))
                cardinality = Integer.parseInt(cmd.getOptionValue("C"));

            if (cmd.hasOption("d"))
                nodes = cmd.getOptionValue("d").split(",");

            if (cmd.hasOption("D"))
            {
                try
                {
                    String node = null;
                    List<String> tmpNodes = new ArrayList<String>();
                    BufferedReader in = new BufferedReader(new InputStreamReader(new FileInputStream(cmd.getOptionValue("D"))));
                    while ((node = in.readLine()) != null)
                    {
                        if (node.length() > 0)
                            tmpNodes.add(node);
                    }
                    nodes = tmpNodes.toArray(new String[tmpNodes.size()]);
                    in.close();
                }
                catch(IOException ioe)
                {
                    throw new RuntimeException(ioe);
                }
            }

            if (cmd.hasOption("s"))
                STDev = Float.parseFloat(cmd.getOptionValue("s"));

            if (cmd.hasOption("r"))
                random = true;

            outFileName = (cmd.hasOption("f")) ? cmd.getOptionValue("f") : null;

            if (cmd.hasOption("p"))
                port = Integer.parseInt(cmd.getOptionValue("p"));

            if (cmd.hasOption("o"))
                operation = Stress.Operations.valueOf(cmd.getOptionValue("o").toUpperCase());

            if (cmd.hasOption("u"))
                superColumns = Integer.parseInt(cmd.getOptionValue("u"));

            if (cmd.hasOption("y"))
                columnFamilyType = ColumnFamilyType.valueOf(cmd.getOptionValue("y"));

            if (cmd.hasOption("K"))
            {
                retryTimes = Integer.valueOf(cmd.getOptionValue("K"));

                if (retryTimes <= 0)
                {
                    throw new RuntimeException("--keep-trying option value should be > 0");
                }
            }

            if (cmd.hasOption("k"))
            {
                retryTimes = 1;
                ignoreErrors = true;
            }


            if (cmd.hasOption("i"))
                progressInterval = Integer.parseInt(cmd.getOptionValue("i"));

            if (cmd.hasOption("g"))
                keysPerCall = Integer.parseInt(cmd.getOptionValue("g"));

            if (cmd.hasOption("e"))
                consistencyLevel = ConsistencyLevel.valueOf(cmd.getOptionValue("e").toUpperCase());

            if (cmd.hasOption("x"))
                indexType = IndexType.valueOf(cmd.getOptionValue("x").toUpperCase());

            if (cmd.hasOption("R"))
                replicationStrategy = cmd.getOptionValue("R");

            if (cmd.hasOption("l"))
                replicationStrategyOptions.put("replication_factor", String.valueOf(Integer.parseInt(cmd.getOptionValue("l"))));
            else if (replicationStrategy.endsWith("SimpleStrategy"))
                replicationStrategyOptions.put("replication_factor", "1");

            if (cmd.hasOption("L"))
            {
                enable_cql = true;
                cqlVersion = "2.0.0";
            }

            if (cmd.hasOption("L3"))
            {
                enable_cql = true;
                cqlVersion = "3.0.0";
            }

            if (cmd.hasOption("b"))
            {
                if (!(enable_cql && cqlVersion.startsWith("3")))
                    throw new IllegalArgumentException("Cannot use binary protocol without -L3");
                use_native_protocol = true;
            }

            if (cmd.hasOption("P"))
            {
                if (!enable_cql)
                {
                    System.err.println("-P/--use-prepared-statements is only applicable with CQL (-L/--enable-cql)");
                    System.exit(-1);
                }
                use_prepared = true;
            }

            if (cmd.hasOption("O"))
            {
                String[] pairs = StringUtils.split(cmd.getOptionValue("O"), ',');

                for (String pair : pairs)
                {
                    String[] keyAndValue = StringUtils.split(pair, ':');

                    if (keyAndValue.length != 2)
                        throw new RuntimeException("Invalid --strategy-properties value.");

                    replicationStrategyOptions.put(keyAndValue[0], keyAndValue[1]);
                }
            }

            if (cmd.hasOption("W"))
                replicateOnWrite = false;

            if (cmd.hasOption("I"))
                compression = cmd.getOptionValue("I");

            averageSizeValues = cmd.hasOption("V");

            try
            {
                sendToDaemon = cmd.hasOption("send-to")
                                ? InetAddress.getByName(cmd.getOptionValue("send-to"))
                                : null;
            }
            catch (UnknownHostException e)
            {
                throw new RuntimeException(e);
            }

            if (cmd.hasOption("Q"))
            {
                AbstractType comparator = TypeParser.parse(DEFAULT_COMPARATOR);

                String[] names = StringUtils.split(cmd.getOptionValue("Q"), ",");
                columnNames = new ArrayList<ByteBuffer>(names.length);

                for (String columnName : names)
                    columnNames.add(comparator.fromString(columnName));
            }
            else
            {
                columnNames = null;
            }

            if (cmd.hasOption("Z"))
            {
                compactionStrategy = cmd.getOptionValue("Z");

                try
                {
                    // validate compaction strategy class
                    CFMetaData.createCompactionStrategy(compactionStrategy);
                }
                catch (ConfigurationException e)
                {
                    System.err.println(e.getMessage());
                    System.exit(1);
                }
            }

            if (cmd.hasOption("U"))
            {
                AbstractType parsed = null;

                try
                {
                    parsed = TypeParser.parse(cmd.getOptionValue("U"));
                }
                catch (ConfigurationException e)
                {
                    System.err.println(e.getMessage());
                    System.exit(1);
                }

                comparator = cmd.getOptionValue("U");
                timeUUIDComparator = parsed instanceof TimeUUIDType;

                if (!(parsed instanceof TimeUUIDType || parsed instanceof AsciiType || parsed instanceof UTF8Type))
                {
                    System.err.println("Currently supported types are: TimeUUIDType, AsciiType, UTF8Type.");
                    System.exit(1);
                }
            }
            else
            {
                comparator = null;
                timeUUIDComparator = false;
            }

            if(cmd.hasOption(SSL_TRUSTSTORE))
                encOptions.truststore = cmd.getOptionValue(SSL_TRUSTSTORE);

            if(cmd.hasOption(SSL_TRUSTSTORE_PW))
                encOptions.truststore_password = cmd.getOptionValue(SSL_TRUSTSTORE_PW);

            if(cmd.hasOption(SSL_PROTOCOL))
                encOptions.protocol = cmd.getOptionValue(SSL_PROTOCOL);

            if(cmd.hasOption(SSL_ALGORITHM))
                encOptions.algorithm = cmd.getOptionValue(SSL_ALGORITHM);

            if(cmd.hasOption(SSL_STORE_TYPE))
                encOptions.store_type = cmd.getOptionValue(SSL_STORE_TYPE);

            if(cmd.hasOption(SSL_CIPHER_SUITES))
                encOptions.cipher_suites = cmd.getOptionValue(SSL_CIPHER_SUITES).split(",");

            if (cmd.hasOption("tf"))
                transportFactory = validateAndSetTransportFactory(cmd.getOptionValue("tf"));

        }
        catch (ParseException e)
        {
            throw new IllegalArgumentException(e.getMessage(), e);
        }
        catch (ConfigurationException e)
        {
            throw new IllegalStateException(e.getMessage(), e);
        }

        mean  = numDifferentKeys / 2;
        sigma = numDifferentKeys * STDev;
    }

    private TTransportFactory validateAndSetTransportFactory(String transportFactory)
    {
        try
        {
            Class factory = Class.forName(transportFactory);

            if(!TTransportFactory.class.isAssignableFrom(factory))
                throw new IllegalArgumentException(String.format("transport factory '%s' " +
                        "not derived from TTransportFactory", transportFactory));

            return (TTransportFactory) factory.newInstance();
        }
        catch (Exception e)
        {
            throw new IllegalArgumentException(String.format("Cannot create a transport factory '%s'.", transportFactory), e);
        }
    }

    public int getCardinality()
    {
        return cardinality;
    }

    public int getColumnSize()
    {
        return columnSize;
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

    public int getNumDifferentKeys()
    {
        return numDifferentKeys;
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
        return Integer.toString(numDifferentKeys).length();
    }

    public ConsistencyLevel getConsistencyLevel()
    {
        return consistencyLevel;
    }

    public int getRetryTimes()
    {
        return retryTimes;
    }

    public boolean ignoreErrors()
    {
        return ignoreErrors;
    }

    public Stress.Operations getOperation()
    {
        return operation;
    }

    public PrintStream getOutputStream()
    {
        try
        {
            return (outFileName == null) ? System.out : new PrintStream(new FileOutputStream(outFileName));
        }
        catch (FileNotFoundException e)
        {
            throw new RuntimeException(e.getMessage(), e);
        }
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

    public boolean isCQL()
    {
        return enable_cql;
    }

    public boolean usePreparedStatements()
    {
        return use_prepared;
    }

    /**
     * Create Keyspace with Standard and Super/Counter column families
     */
    public void createKeySpaces()
    {
        KsDef keyspace = new KsDef();
        String defaultComparator = comparator == null ? DEFAULT_COMPARATOR : comparator;

        // column family for standard columns
        CfDef standardCfDef = new CfDef(KEYSPACE_NAME, "Standard1");
        Map<String, String> compressionOptions = new HashMap<String, String>();
        if (compression != null)
            compressionOptions.put("sstable_compression", compression);

        standardCfDef.setComparator_type(defaultComparator)
                     .setDefault_validation_class(DEFAULT_VALIDATOR)
                     .setCompression_options(compressionOptions);

        if (!timeUUIDComparator)
        {
            for (int i = 0; i < getColumnsPerKey(); i++)
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
        CfDef superCfDef = new CfDef(KEYSPACE_NAME, "Super1").setColumn_type("Super");
        superCfDef.setComparator_type(DEFAULT_COMPARATOR)
                  .setSubcomparator_type(defaultComparator)
                  .setDefault_validation_class(DEFAULT_VALIDATOR)
                  .setCompression_options(compressionOptions);

        // column family for standard counters
        CfDef counterCfDef = new CfDef(KEYSPACE_NAME, "Counter1").setComparator_type(defaultComparator)
                                                                 .setComparator_type(defaultComparator)
                                                                 .setDefault_validation_class("CounterColumnType")
                                                                 .setReplicate_on_write(replicateOnWrite)
                                                                 .setCompression_options(compressionOptions);

        // column family with counter super columns
        CfDef counterSuperCfDef = new CfDef(KEYSPACE_NAME, "SuperCounter1").setComparator_type(defaultComparator)
                                                                           .setDefault_validation_class("CounterColumnType")
                                                                           .setReplicate_on_write(replicateOnWrite)
                                                                           .setColumn_type("Super")
                                                                           .setCompression_options(compressionOptions);

        keyspace.setName(KEYSPACE_NAME);
        keyspace.setStrategy_class(replicationStrategy);

        if (!replicationStrategyOptions.isEmpty())
        {
            keyspace.setStrategy_options(replicationStrategyOptions);
        }

        if (compactionStrategy != null)
        {
            standardCfDef.setCompaction_strategy(compactionStrategy);
            superCfDef.setCompaction_strategy(compactionStrategy);
            counterCfDef.setCompaction_strategy(compactionStrategy);
            counterSuperCfDef.setCompaction_strategy(compactionStrategy);
        }

        keyspace.setCf_defs(new ArrayList<CfDef>(Arrays.asList(standardCfDef, superCfDef, counterCfDef, counterSuperCfDef)));

        CassandraClient client = getClient(false);

        try
        {
            client.system_add_keyspace(keyspace);

            /* CQL3 counter cf */
            client.set_cql_version("3.0.0"); // just to create counter cf for cql3

            client.set_keyspace(KEYSPACE_NAME);
            client.execute_cql3_query(createCounterCFStatementForCQL3(), Compression.NONE, ConsistencyLevel.ONE);

            if (enable_cql)
                client.set_cql_version(cqlVersion);
            /* end */

            System.out.println(String.format("Created keyspaces. Sleeping %ss for propagation.", nodes.length));
            Thread.sleep(nodes.length * 1000); // seconds
        }
        catch (InvalidRequestException e)
        {
            System.err.println("Unable to create stress keyspace: " + e.getWhy());
        }
        catch (Exception e)
        {
            System.err.println(e.getMessage());
        }
    }

    /**
     * Thrift client connection with Keyspace1 set.
     * @return cassandra client connection
     */
    public CassandraClient getClient()
    {
        return getClient(true);
    }

    /**
     * Thrift client connection
     * @param setKeyspace - should we set keyspace for client or not
     * @return cassandra client connection
     */
    public CassandraClient getClient(boolean setKeyspace)
    {
        // random node selection for fake load balancing
        String currentNode = nodes[Stress.randomizer.nextInt(nodes.length)];

        TSocket socket = new TSocket(currentNode, port);
        TTransport transport = transportFactory.getTransport(socket);
        CassandraClient client = new CassandraClient(new TBinaryProtocol(transport));

        try
        {
            if(!transport.isOpen())
                transport.open();

            if (enable_cql)
                client.set_cql_version(cqlVersion);

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

    public SimpleClient getNativeClient()
    {
        try
        {
            String currentNode = nodes[Stress.randomizer.nextInt(nodes.length)];
            SimpleClient client = new SimpleClient(currentNode, 9042);
            client.connect(false);
            client.execute("USE \"Keyspace1\";", org.apache.cassandra.db.ConsistencyLevel.ONE);
            return client;
        }
        catch (Exception e)
        {
            throw new RuntimeException(e.getMessage());
        }
    }

    public static InetAddress getLocalAddress()
    {
        if (localInetAddress == null)
        {
            try
            {
                localInetAddress = InetAddress.getLocalHost();
            }
            catch (UnknownHostException e)
            {
                throw new RuntimeException(e);
            }
        }

        return localInetAddress;
    }

    private ByteBuffer createCounterCFStatementForCQL3()
    {
        StringBuilder counter3 = new StringBuilder("CREATE TABLE \"Counter3\" (KEY blob PRIMARY KEY, ");

        for (int i = 0; i < getColumnsPerKey(); i++)
        {
            counter3.append("c").append(i).append(" counter");
            if (i != getColumnsPerKey() - 1)
                counter3.append(", ");
        }
        counter3.append(");");

        return ByteBufferUtil.bytes(counter3.toString());
    }
}
