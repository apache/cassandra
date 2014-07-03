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
package org.apache.cassandra.stress.settings;

import java.io.Serializable;
import java.util.*;

import org.apache.commons.cli.*;
import org.apache.commons.cli.Option;

public class Legacy implements Serializable
{

    // command line options
    public static final Options availableOptions = new Options();

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
        availableOptions.addOption("t",  "threadCount",              true,   "Number of threadCount to use, default:50");
        availableOptions.addOption("c",  "columns",              true,   "Number of columns per key, default:5");
        availableOptions.addOption("S",  "column-size",          true,   "Size of column values in bytes, default:34");
        availableOptions.addOption("C",  "unique columns",       true,   "Max number of unique columns per key, default:50");
        availableOptions.addOption("RC", "unique rows",          true,   "Max number of unique rows, default:50");
        availableOptions.addOption("d",  "nodes",                true,   "Host nodes (comma separated), default:locahost");
        availableOptions.addOption("D",  "nodesfile",            true,   "File containing host nodes (one per line)");
        availableOptions.addOption("s",  "stdev",                true,   "Standard Deviation for gaussian read key generation, default:0.1");
        availableOptions.addOption("r",  "random",               false,  "Use random key generator for read key generation (STDEV will have no effect), default:false");
        availableOptions.addOption("f",  "file",                 true,   "Write output to given file");
        availableOptions.addOption("p",  "port",                 true,   "Thrift port, default:9160");
        availableOptions.addOption("o",  "operation",            true,   "Operation to perform (WRITE, READ, READWRITE, RANGE_SLICE, INDEXED_RANGE_SLICE, MULTI_GET, COUNTERWRITE, COUNTER_GET), default:WRITE");
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
        availableOptions.addOption("V",  "average-size-values",  false,  "Generate column values of average rather than specific size");
        availableOptions.addOption("T",  "send-to",              true,   "Send this as a request to the stress daemon at specified address.");
        availableOptions.addOption("I",  "compression",          true,   "Specify the compression to use for sstable, default:no compression");
        availableOptions.addOption("Q",  "query-names",          true,   "Comma-separated list of column names to retrieve from each row.");
        availableOptions.addOption("Z",  "compaction-strategy",  true,   "CompactionStrategy to use.");
        availableOptions.addOption("U",  "comparator",           true,   "Column Comparator to use. Currently supported types are: TimeUUIDType, AsciiType, UTF8Type.");
        availableOptions.addOption("tf", "transport-factory",    true,   "Fully-qualified TTransportFactory class name for creating a connection. Note: For Thrift over SSL, use org.apache.cassandra.stress.SSLTransportFactory.");
        availableOptions.addOption("ns", "no-statistics",        false,  "Turn off the aggegate statistics that is normally output after completion.");
        availableOptions.addOption("ts", SSL_TRUSTSTORE,         true, "SSL: full path to truststore");
        availableOptions.addOption("tspw", SSL_TRUSTSTORE_PW,    true, "SSL: full path to truststore");
        availableOptions.addOption("prtcl", SSL_PROTOCOL,        true, "SSL: connections protocol to use (default: TLS)");
        availableOptions.addOption("alg", SSL_ALGORITHM,         true, "SSL: algorithm (default: SunX509)");
        availableOptions.addOption("st", SSL_STORE_TYPE,         true, "SSL: type of store");
        availableOptions.addOption("ciphers", SSL_CIPHER_SUITES, true, "SSL: comma-separated list of encryption suites to use");
        availableOptions.addOption("th",  "throttle",            true,   "Throttle the total number of operations per second to a maximum amount.");
    }

    public static StressSettings build(String[] arguments)
    {
        CommandLineParser parser = new PosixParser();

        final Converter r = new Converter();
        try
        {
            CommandLine cmd = parser.parse(availableOptions, arguments);

            if (cmd.getArgs().length > 0)
            {
                System.err.println("Application does not allow arbitrary arguments: " + Arrays.asList(cmd.getArgList()));
                System.exit(1);
            }

            if (cmd.hasOption("h"))
                printHelpMessage();

            if (cmd.hasOption("C"))
                System.out.println("Ignoring deprecated option -C");

            if (cmd.hasOption("o"))
                r.setCommand(cmd.getOptionValue("o").toLowerCase());
            else
                r.setCommand("insert");

            if (cmd.hasOption("K"))
                r.add("command", "tries=" + cmd.getOptionValue("K"));

            if (cmd.hasOption("k"))
            {
                if (!cmd.hasOption("K"))
                    r.add("command", "retry=1");
                r.add("command", "ignore_errors");
            }

            if (cmd.hasOption("g"))
                r.add("command", "at-once=" + cmd.getOptionValue("g"));

            if (cmd.hasOption("e"))
                r.add("command", "cl=" + cmd.getOptionValue("e"));

            String numKeys;
            if (cmd.hasOption("n"))
                numKeys = cmd.getOptionValue("n");
            else
                numKeys = "1000000";
            r.add("command", "n=" + numKeys);

            String uniqueKeys;
            if (cmd.hasOption("F"))
                uniqueKeys = cmd.getOptionValue("F");
            else
                uniqueKeys = numKeys;

            if (r.opts.containsKey("write") || r.opts.containsKey("counterwrite"))
            {
                if (!uniqueKeys.equals(numKeys))
                    r.add("-key", "populate=1.." + uniqueKeys);
            }
            else if (cmd.hasOption("r"))
            {
                r.add("-key", "dist=uniform(1.." + uniqueKeys + ")");
            }
            else
            {
                if (!cmd.hasOption("s"))
                    r.add("-key", "dist=gauss(1.." + uniqueKeys + ",5)");
                else
                    r.add("-key", String.format("dist=gauss(1..%s,%.2f)", uniqueKeys,
                            0.5 / Float.parseFloat(cmd.getOptionValue("s"))));
            }

            String colCount;
            if (cmd.hasOption("c"))
                colCount = cmd.getOptionValue("c");
            else
                colCount = "5";

            String colSize;
            if (cmd.hasOption("S"))
                colSize = cmd.getOptionValue("S");
            else
                colSize = "34";

            r.add("-col", "n=fixed(" + colCount + ")");
            if (cmd.hasOption("V"))
            {
                r.add("-col", "size=uniform(1.." + Integer.parseInt(colSize) * 2 + ")");
                r.add("-col", "data=rand()");
            }
            else
            {
                r.add("-col", "size=fixed(" + colSize + ")");
                r.add("-col", "data=repeat(1)");
            }
            if (cmd.hasOption("Q"))
                r.add("-col", "names=" + cmd.getOptionValue("Q"));

            if (cmd.hasOption("U"))
                r.add("-col", "comparator=" + cmd.getOptionValue("U"));

            if (cmd.hasOption("y") && cmd.getOptionValue("y").equals("Super"))
                r.add("-col", "super=" + (cmd.hasOption("u") ? cmd.getOptionValue("u") : "1"));

            if (cmd.hasOption("t"))
                r.add("-rate", "threads=" + cmd.getOptionValue("t"));
            else
                r.add("-rate", "threads=50");

            if (cmd.hasOption("th"))
                r.add("-rate", "limit=" + cmd.getOptionValue("th") + "/s");

            if (cmd.hasOption("f"))
                r.add("-log", "file=" + cmd.getOptionValue("f"));

            if (cmd.hasOption("p"))
                r.add("-port", cmd.getOptionValue("p"));

            if (cmd.hasOption("i"))
                r.add("-log", "interval=" + cmd.getOptionValue("i"));
            else
                r.add("-log", "interval=10");

            if (cmd.hasOption("x"))
                r.add("-schema", "index=" + cmd.getOptionValue("x"));

            if (cmd.hasOption("R") || cmd.hasOption("l") || cmd.hasOption("O"))
            {
                StringBuilder rep = new StringBuilder();
                if (cmd.hasOption("R"))
                    rep.append("strategy=" + cmd.getOptionValue("R"));
                if (cmd.hasOption("l"))
                {
                    if (rep.length() > 0)
                        rep.append(",");
                    rep.append("factor=" + cmd.getOptionValue("l"));
                }
                if (cmd.hasOption("O"))
                {
                    if (rep.length() > 0)
                        rep.append(",");
                    rep.append(cmd.getOptionValue("O").replace(':','='));
                }
                r.add("-schema", "replication(" + rep + ")");
            }

            if (cmd.hasOption("L"))
                r.add("-mode", cmd.hasOption("P") ? "prepared cql2" : "cql2");
            else if (cmd.hasOption("L3"))
                r.add("-mode", (cmd.hasOption("P") ? "prepared" : "") + (cmd.hasOption("b") ? "native" : "") +  "cql3");
            else
                r.add("-mode", "thrift");

            if (cmd.hasOption("I"))
                r.add("-schema", "compression=" + cmd.getOptionValue("I"));

            if (cmd.hasOption("d"))
                r.add("-node", cmd.getOptionValue("d"));

            if (cmd.hasOption("D"))
                r.add("-node", "file=" + cmd.getOptionValue("D"));


            if (cmd.hasOption("send-to"))
                r.add("-send-to", cmd.getOptionValue("send-to"));

            if (cmd.hasOption("Z"))
                r.add("-schema", "compaction=" + cmd.getOptionValue("Z"));

            if (cmd.hasOption("ns"))
                r.add("-log", "no-summary");

            if (cmd.hasOption("tf"))
                r.add("-transport", "factory=" + cmd.getOptionValue("tf"));

            if(cmd.hasOption(SSL_TRUSTSTORE))
                r.add("-transport", "truststore=" + cmd.getOptionValue(SSL_TRUSTSTORE));

            if(cmd.hasOption(SSL_TRUSTSTORE_PW))
                r.add("-transport", "truststore-password=" + cmd.getOptionValue(SSL_TRUSTSTORE_PW));

            if(cmd.hasOption(SSL_PROTOCOL))
                r.add("-transport", "ssl-protocol=" + cmd.getOptionValue(SSL_PROTOCOL));

            if(cmd.hasOption(SSL_ALGORITHM))
                r.add("-transport", "ssl-alg=" +  cmd.getOptionValue(SSL_ALGORITHM));

            if(cmd.hasOption(SSL_STORE_TYPE))
                r.add("-transport", "store-type=" +  cmd.getOptionValue(SSL_STORE_TYPE));

            if(cmd.hasOption(SSL_CIPHER_SUITES))
                r.add("-transport", "ssl-ciphers=" +  cmd.getOptionValue(SSL_CIPHER_SUITES));

        }
        catch (ParseException e)
        {
            printHelpMessage();
            System.exit(1);
        }

        r.printNewCommand();
        return r.get();
    }

    private static final class Converter
    {
        private Map<String, List<String>> opts = new LinkedHashMap<>();
        List<String> command;
        public void add(String option, String suboption)
        {
            if (option.equals("command"))
            {
                command.add(suboption);
                return;
            }
            List<String> params = opts.get(option);
            if (params == null)
                opts.put(option, params = new ArrayList());
            params.add(suboption);
        }
        StressSettings get(){
            Map<String, String[]> clArgs = new HashMap<>();
            for (Map.Entry<String, List<String>> e : opts.entrySet())
                clArgs .put(e.getKey(), e.getValue().toArray(new String[0]));
            return StressSettings.get(clArgs);
        }
        void setCommand(String command)
        {
            command = Command.get(command).toString().toLowerCase();
            opts.put(command, this.command = new ArrayList<>());
        }
        void printNewCommand()
        {
            StringBuilder sb = new StringBuilder("stress");
            for (Map.Entry<String, List<String>> e : opts.entrySet())
            {
                sb.append(" ");
                sb.append(e.getKey());
                for (String opt : e.getValue())
                {
                    sb.append(" ");
                    sb.append(opt);
                }
            }
            System.out.println("Running in legacy support mode. Translating command to: ");
            System.out.println(sb.toString());
        }
    }

    public static void printHelpMessage()
    {
        System.out.println("Usage: ./bin/cassandra-stress legacy [options]\n\nOptions:");
        System.out.println("THIS IS A LEGACY SUPPORT MODE");

        for(Object o : availableOptions.getOptions())
        {
            Option option = (Option) o;
            String upperCaseName = option.getLongOpt().toUpperCase();
            System.out.println(String.format("-%s%s, --%s%s%n\t\t%s%n", option.getOpt(), (option.hasArg()) ? " "+upperCaseName : "",
                    option.getLongOpt(), (option.hasArg()) ? "="+upperCaseName : "", option.getDescription()));
        }
    }

    public static Runnable helpPrinter()
    {
        return new Runnable()
        {
            @Override
            public void run()
            {
                printHelpMessage();
            }
        };
    }

}
