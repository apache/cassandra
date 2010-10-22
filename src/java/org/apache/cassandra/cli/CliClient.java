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
package org.apache.cassandra.cli;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.*;

import com.google.common.base.Charsets;

import org.antlr.runtime.tree.CommonTree;
import org.antlr.runtime.tree.Tree;
import org.apache.cassandra.auth.SimpleAuthenticator;
import org.apache.cassandra.config.ConfigurationException;
import org.apache.cassandra.db.marshal.*;
import org.apache.cassandra.thrift.*;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.UUIDGen;
import org.apache.thrift.TException;

// Cli Client Side Library
public class CliClient 
{

    /**
     * Available value conversion functions
     * Used by convertValueByFunction(Tree functionCall) method
     */
    private enum Function
    {
        BYTES       (BytesType.instance),
        INTEGER     (IntegerType.instance),
        LONG        (LongType.instance),
        LEXICALUUID (LexicalUUIDType.instance),
        TIMEUUID    (TimeUUIDType.instance),
        UTF8        (UTF8Type.instance),
        ASCII       (AsciiType.instance);

        private AbstractType validator;
        
        Function(AbstractType validator)
        {
            this.validator = validator;  
        }

        public AbstractType getValidator()
        {
            return this.validator;
        }

        public static String getFunctionNames()
        {
            Function[] functions = Function.values();
            StringBuilder functionNames = new StringBuilder();

            for (int i = 0; i < functions.length; i++)
            {
                StringBuilder currentName = new StringBuilder(functions[i].name().toLowerCase());
                functionNames.append(currentName.append(((i != functions.length-1) ? ", " : ".")));
            }

            return functionNames.toString();
        }
    }
    
    /*
     * the <i>add column family</i> command requires a list of arguments, 
     *  this enum defines which arguments are valid.
     */
    private enum ColumnFamilyArgument
    {
        COLUMN_TYPE,
        COMPARATOR,
        SUBCOMPARATOR,
        COMMENT,
        ROWS_CACHED,
        ROW_CACHE_SAVE_PERIOD,
        KEYS_CACHED,
        KEY_CACHE_SAVE_PERIOD,
        READ_REPAIR_CHANCE,
        GC_GRACE,
        COLUMN_METADATA,
        MEMTABLE_OPERATIONS,
        MEMTABLE_THROUGHPUT,
        MEMTABLE_FLUSH_AFTER,
        DEFAULT_VALIDATION_CLASS,
        MIN_COMPACTION_THRESHOLD,
        MAX_COMPACTION_THRESHOLD,
    }

    private EnumMap<ColumnFamilyArgument, String> argumentExplanations = new EnumMap<ColumnFamilyArgument, String>(ColumnFamilyArgument.class)
    {{
        put(ColumnFamilyArgument.COLUMN_TYPE, "Super or Standard");
        put(ColumnFamilyArgument.COMMENT, "Human-readable column family description. Any string is acceptable");
        put(ColumnFamilyArgument.COMPARATOR, "The class used as a comparator when sorting column names.\n                  Valid options include: AsciiType, BytesType, LexicalUUIDType,\n                  LongType, TimeUUIDType, and UTF8Type");
        put(ColumnFamilyArgument.SUBCOMPARATOR, "Comparator for sorting subcolumn names, for Super columns only");
        put(ColumnFamilyArgument.MEMTABLE_OPERATIONS, "Flush memtables after this many operations");
        put(ColumnFamilyArgument.MEMTABLE_THROUGHPUT, "... or after this many bytes have been written");
        put(ColumnFamilyArgument.MEMTABLE_FLUSH_AFTER, "... or after this many seconds");
        put(ColumnFamilyArgument.ROWS_CACHED, "Number or percentage of rows to cache");
        put(ColumnFamilyArgument.ROW_CACHE_SAVE_PERIOD, "Period with which to persist the row cache, in seconds");
        put(ColumnFamilyArgument.KEYS_CACHED, "Number or percentage of keys to cache");
        put(ColumnFamilyArgument.KEY_CACHE_SAVE_PERIOD, "Period with which to persist the key cache, in seconds");
        put(ColumnFamilyArgument.READ_REPAIR_CHANCE, "Probability (0.0-1.0) with which to perform read repairs on CL.ONE reads");
        put(ColumnFamilyArgument.GC_GRACE, "Discard tombstones after this many seconds");
        put(ColumnFamilyArgument.MIN_COMPACTION_THRESHOLD, "Avoid minor compactions of less than this number of sstable files");
        put(ColumnFamilyArgument.MAX_COMPACTION_THRESHOLD, "Compact no more than this number of sstable files at once");
    }};

    /*
     * the <i>add keyspace</i> command requires a list of arguments,
     *  this enum defines which arguments are valid
     */
    private enum AddKeyspaceArgument {
        REPLICATION_FACTOR,
        PLACEMENT_STRATEGY,
        STRATEGY_OPTIONS
    }

    private Cassandra.Client thriftClient_ = null;
    private CliSessionState css_ = null;
    private String keySpace = null;
    private String username = null;
    private Map<String, KsDef> keyspacesMap = new HashMap<String, KsDef>();

    private final String DEFAULT_PLACEMENT_STRATEGY = "org.apache.cassandra.locator.SimpleStrategy";
    
    public CliClient(CliSessionState cliSessionState, Cassandra.Client thriftClient)
    {
        css_ = cliSessionState;
        thriftClient_ = thriftClient;
    }

    // Execute a CLI Statement 
    public void executeCLIStmt(String stmt) throws TException, NotFoundException, InvalidRequestException, UnavailableException, TimedOutException, IllegalAccessException, ClassNotFoundException, InstantiationException, NoSuchFieldException
    {
        CommonTree ast = CliCompiler.compileQuery(stmt);

        switch (ast.getType())
        {
            case CliParser.NODE_EXIT:
                cleanupAndExit();
                break;
            case CliParser.NODE_THRIFT_GET:
                executeGet(ast);
                break;
            case CliParser.NODE_THRIFT_GET_WITH_CONDITIONS:
                executeGetWithConditions(ast);
                break;
            case CliParser.NODE_HELP:
                printCmdHelp(ast);
                break;
            case CliParser.NODE_THRIFT_SET:
                executeSet(ast);
                break;
            case CliParser.NODE_THRIFT_DEL:
                executeDelete(ast);
                break;
            case CliParser.NODE_THRIFT_COUNT:
                executeCount(ast);
                break;
            case CliParser.NODE_ADD_KEYSPACE:
                executeAddKeyspace(ast.getChild(0));
                break;
            case CliParser.NODE_ADD_COLUMN_FAMILY:
                executeAddColumnFamily(ast.getChild(0));
                break;
            case CliParser.NODE_UPDATE_KEYSPACE:
                executeUpdateKeyspace(ast.getChild(0));
                break;
            case CliParser.NODE_UPDATE_COLUMN_FAMILY:
                executeUpdateColumnFamily(ast.getChild(0));
                break;
            case CliParser.NODE_DEL_COLUMN_FAMILY:
                executeDelColumnFamily(ast);
                break;
            case CliParser.NODE_DEL_KEYSPACE:
                executeDelKeyspace(ast);
                break;
            case CliParser.NODE_SHOW_CLUSTER_NAME:
                executeShowClusterName();
                break;
            case CliParser.NODE_SHOW_VERSION:
                executeShowVersion();
                break;
            case CliParser.NODE_SHOW_TABLES:
                executeShowTables();
                break;
            case CliParser.NODE_DESCRIBE_TABLE:
                executeDescribeTable(ast);
                break;
            case CliParser.NODE_USE_TABLE:
                executeUseTable(ast);
                break;
            case CliParser.NODE_CONNECT:
                executeConnect(ast);
                break;
            case CliParser.NODE_LIST:
                executeList(ast);
                break;
            case CliParser.NODE_NO_OP:
                // comment lines come here; they are treated as no ops.
                break;
            default:
                css_.err.println("Invalid Statement (Type: " + ast.getType() + ")");
                if (css_.batch)
                    System.exit(2);
                break;
        }
    }

    private void printCmdHelp(CommonTree ast)
    {
        if (ast.getChildCount() > 0)
        {
            int helpType = ast.getChild(0).getType();
                    
            switch(helpType)
            {
            case CliParser.NODE_HELP:
                css_.out.println("help <command>\n");
                css_.out.println("Display the general help page with a list of available commands.");
                break;
            case CliParser.NODE_CONNECT:
                css_.out.println("connect <hostname>/<port>\n");
                css_.out.println("Connect to the specified host on the specified port.\n");
                css_.out.println("example:");
                css_.out.println("connect localhost/9160");
                break;
                
            case CliParser.NODE_USE_TABLE:
                css_.out.println("use <keyspace>");
                css_.out.println("use <keyspace> <username> '<password>'\n");
                css_.out.println("Switch to the specified keyspace. The optional username and password fields");
                css_.out.println("are needed when performing authentication.\n");
                break;
                
            case CliParser.NODE_DESCRIBE_TABLE:
                css_.out.println("describe keyspace <keyspace>\n");
                css_.out.println("Show additional information about the specified keyspace.\n");
                css_.out.println("example:");
                css_.out.println("describe keyspace system");
                break;
                
            case CliParser.NODE_EXIT:
                css_.out.println("exit");
                css_.out.println("quit\n");
                css_.out.println("Exit this utility.");
                break;
                
            case CliParser.NODE_SHOW_CLUSTER_NAME:
                css_.out.println("show cluster name\n");
                css_.out.println("Displays the name of the currently connected cluster.");
                break;
                
            case CliParser.NODE_SHOW_VERSION:
                css_.out.println("show api version\n");
                css_.out.println("Displays the API version number.");
                break;
                
            case CliParser.NODE_SHOW_TABLES:  
                css_.out.println("show keyspaces\n");
                css_.out.println("Displays a list of the keyspaces available on the currently connected cluster.");
                break;
                
            case CliParser.NODE_ADD_KEYSPACE:
                css_.out.println("create keyspace <keyspace>");
                css_.out.println("create keyspace <keyspace> with <att1>=<value1>");
                css_.out.println("create keyspace <keyspace> with <att1>=<value1> and <att2>=<value2> ...\n");
                css_.out.println("Create a new keyspace with the specified values for the given set of attributes.\n");
                css_.out.println("valid attributes are:");
                css_.out.println("    replication_factor: to how many nodes should entries to this keyspace be");
                css_.out.println("                        replicated. Valid entries are integers greater than 0.");
                css_.out.println("    placement_strategy: the fully qualified class used to place replicas in");
                css_.out.println("                        this keyspace. Valid values are");
                css_.out.println("                        org.apache.cassandra.locator.SimpleStrategy,");
                css_.out.println("                        org.apache.cassandra.locator.NetworkTopologyStrategy,");
                css_.out.println("                        and org.apache.cassandra.locator.OldNetworkTopologyStrategy");
                css_.out.println("      strategy_options: additional options for placement_strategy.\n");
                css_.out.println("example:");
                css_.out.println("create keyspace foo with replication_factor = 3 and ");
                css_.out.println("        placement_strategy = 'org.apache.cassandra.locator.SimpleStrategy'");
                css_.out.println("        and strategy_options=[{DC1:2, DC2:2}]");
                break;

            case CliParser.NODE_UPDATE_KEYSPACE:
                css_.out.println("update keyspace <keyspace>");
                css_.out.println("update keyspace <keyspace> with <att1>=<value1>");
                css_.out.println("update keyspace <keyspace> with <att1>=<value1> and <att2>=<value2> ...\n");
                css_.out.println("Update a keyspace with the specified values for the given set of attributes.\n");
                css_.out.println("valid attributes are:");
                css_.out.println("    replication_factor: to how many nodes should entries to this keyspace be");
                css_.out.println("                        replicated. Valid entries are integers greater than 0.");
                css_.out.println("    placement_strategy: the fully qualified class used to place replicas in");
                css_.out.println("                        this keyspace. Valid values are");
                css_.out.println("                        org.apache.cassandra.locator.SimpleStrategy,");
                css_.out.println("                        org.apache.cassandra.locator.NetworkTopologyStrategy,");
                css_.out.println("                        and org.apache.cassandra.locator.OldNetworkTopologyStrategy");
                css_.out.println("      strategy_options: additional options for placement_strategy.\n");
                css_.out.println("example:");
                css_.out.println("update keyspace foo with replication_factor = 2 and ");
                css_.out.println("        placement_strategy = 'org.apache.cassandra.locator.LocalStrategy'");
                css_.out.println("        and strategy_options=[{DC1:1, DC2:4, DC3:2}]");
                break;

            case CliParser.NODE_ADD_COLUMN_FAMILY:
                css_.out.println("create column family Bar");
                css_.out.println("create column family Bar with <att1>=<value1>");
                css_.out.println("create column family Bar with <att1>=<value1> and <att2>=<value2>...\n");
                css_.out.println("Create a new column family with the specified values for the given set of");
                css_.out.println("attributes. Note that you must be using a keyspace.\n");
                css_.out.println("valid attributes are:");
                for (ColumnFamilyArgument argument : ColumnFamilyArgument.values())
                    css_.out.printf("    - %s: %s\n", argument.toString().toLowerCase(), argumentExplanations.get(argument));
                css_.out.println("    - column_metadata: Metadata which describes columns of column family.");
                css_.out.println("        Supported format is [{ k:v, k:v, ... }, { ... }, ...]");
                css_.out.println("        Valid attributes: column_name, validation_class (see comparator),");
                css_.out.println("                          index_type (integer), index_name.");
                css_.out.println("example:\n");
                css_.out.println("create column family Bar with column_type = 'Super' and comparator = 'AsciiType'");
                css_.out.println("      and rows_cached = 10000");
                css_.out.println("create column family Baz with comparator = 'LongType' and rows_cached = 10000");
                css_.out.print("create column family Foo with comparator=LongType and column_metadata=");
                css_.out.print("[{ column_name:Test, validation_class:IntegerType, index_type:0, index_name:IdxName");
                css_.out.println("}, { column_name:'other name', validation_class:LongType }]");
                break;

            case CliParser.NODE_UPDATE_COLUMN_FAMILY:
                css_.out.println("update column family Bar");
                css_.out.println("update column family Bar with <att1>=<value1>");
                css_.out.println("update column family Bar with <att1>=<value1> and <att2>=<value2>...\n");
                css_.out.println("Update a column family with the specified values for the given set of");
                css_.out.println("attributes. Note that you must be using a keyspace.\n");
                css_.out.println("valid attributes are:");
                for (ColumnFamilyArgument argument : ColumnFamilyArgument.values())
                {
                    if (argument == ColumnFamilyArgument.COMPARATOR || argument == ColumnFamilyArgument.SUBCOMPARATOR)
                        continue;
                    css_.out.printf("    - %s: %s\n", argument.toString().toLowerCase(), argumentExplanations.get(argument));
                }
                css_.out.println("    - column_metadata: Metadata which describes columns of column family.");
                css_.out.println("        Supported format is [{ k:v, k:v, ... }, { ... }, ...]");
                css_.out.println("        Valid attributes: column_name, validation_class (see comparator),");
                css_.out.println("                          index_type (integer), index_name.");
                css_.out.println("example:\n");
                css_.out.print("update column family Foo with column_metadata=");
                css_.out.print("[{ column_name:Test, validation_class:IntegerType, index_type:0, index_name:IdxName");
                css_.out.println("}] and rows_cached=100 and comment='this is helpful comment.'");
                break;
                
            case CliParser.NODE_DEL_KEYSPACE:
                css_.out.println("drop keyspace <keyspace>\n");
                css_.out.println("Drops the specified keyspace.\n");
                css_.out.println("example:");
                css_.out.println("drop keyspace foo");
                break;
                
            case CliParser.NODE_DEL_COLUMN_FAMILY:
                css_.out.println("drop column family <name>\n");
                css_.out.println("Drops the specified column family.\n");
                css_.out.println("example:");
                css_.out.println("drop column family foo");
                break;
                
            case CliParser.NODE_THRIFT_GET :
                css_.out.println("get <cf>['<key>']");
                css_.out.println("get <cf>['<key>']['<col>'] (as <type>)*");
                css_.out.println("get <cf>['<key>']['<super>']");
                css_.out.println("get <cf> where <column> = <value> [and <column> > <value> and ...] [limit <integer>]");
                css_.out.println("Default LIMIT is 100. Available operations: =, >, >=, <, <=\n");
                css_.out.println("get <cf>['<key>']['<super>']['<col>'] (as <type>)*");
                css_.out.print("Note: `as <type>` is optional, it dynamically converts column value to the specified type");
                css_.out.println(", column value validator will be set to <type>.");
                css_.out.println("Available types: IntegerType, LongType, UTF8Type, ASCIIType, TimeUUIDType, LexicalUUIDType.\n");
                css_.out.println("examples:");
                css_.out.println("get bar[testkey]");
                css_.out.println("get bar[testkey][test_column] as IntegerType");
                break;
                
            case CliParser.NODE_THRIFT_SET:
                css_.out.println("set <cf>['<key>']['<col>'] = <value>");
                css_.out.println("set <cf>['<key>']['<super>']['<col>'] = <value>");
                css_.out.println("set <cf>['<key>']['<col>'] = <function>(<argument>)");
                css_.out.println("set <cf>['<key>']['<super>']['<col>'] = <function>(<argument>)");
                css_.out.println("Available functions: " + Function.getFunctionNames() + "\n");
                css_.out.println("examples:");
                css_.out.println("set bar['testkey']['my super']['test col']='this is a test'");
                css_.out.println("set baz['testkey']['test col']='this is also a test'");
                css_.out.println("set diz[testkey][testcol] = utf8('this is utf8 string.')");
                break;
                
            case CliParser.NODE_THRIFT_DEL:
                css_.out.println("del <cf>['<key>'] ");
                css_.out.println("del <cf>['<key>']['<col>'] ");
                css_.out.println("del <cf>['<key>']['<super>']['<col>']\n");
                css_.out.println("Deletes a record, a column, or a subcolumn.\n");
                css_.out.println("example:");
                css_.out.println("del bar['testkey']['my super']['test col']");
                css_.out.println("del baz['testkey']['test col']");
                css_.out.println("del baz['testkey']");
                break;
                
            case CliParser.NODE_THRIFT_COUNT:
                css_.out.println("count <cf>['<key>']");
                css_.out.println("count <cf>['<key>']['<super>']\n");
                css_.out.println("Count the number of columns in the specified key or subcolumns in the specified");
                css_.out.println("super column.\n");
                css_.out.println("example:");
                css_.out.println("count bar['testkey']['my super']");
                css_.out.println("count baz['testkey']");
                break;

            case CliParser.NODE_LIST:
                css_.out.println("list <cf>");
                css_.out.println("list <cf>[<startKey>:]");
                css_.out.println("list <cf>[<startKey>:<endKey>]");
                css_.out.println("list ... limit N");
                css_.out.println("List a range of rows in the column or supercolumn family.\n");
                css_.out.println("example:");
                css_.out.println("list Users[j:] limit 40");
                break;
                
            default:
                css_.out.println("?");
                break;
            }
        }
        else
        {
            css_.out.println("List of all CLI commands:");
            css_.out.println("?                                                          Display this message.");
            css_.out.println("help                                                          Display this help.");
            css_.out.println("help <command>                          Display detailed, command-specific help.");
            css_.out.println("connect <hostname>/<port>                             Connect to thrift service.");
            css_.out.println("use <keyspace> [<username> 'password']                     Switch to a keyspace.");
            css_.out.println("describe keyspace <keyspacename>                              Describe keyspace.");
            css_.out.println("exit                                                                   Exit CLI.");
            css_.out.println("quit                                                                   Exit CLI.");
            css_.out.println("show cluster name                                          Display cluster name.");
            css_.out.println("show keyspaces                                           Show list of keyspaces.");
            css_.out.println("show api version                                        Show server API version.");
            css_.out.println("create keyspace <keyspace> [with <att1>=<value1> [and <att2>=<value2> ...]]");
            css_.out.println("                Add a new keyspace with the specified attribute(s) and value(s).");
            css_.out.println("update keyspace <keyspace> [with <att1>=<value1> [and <att2>=<value2> ...]]");
            css_.out.println("                 Update a keyspace with the specified attribute(s) and value(s).");
            css_.out.println("create column family <cf> [with <att1>=<value1> [and <att2>=<value2> ...]]");
            css_.out.println("        Create a new column family with the specified attribute(s) and value(s).");
            css_.out.println("update column family <cf> [with <att1>=<value1> [and <att2>=<value2> ...]]");
            css_.out.println("            Update a column family with the specified attribute(s) and value(s).");
            css_.out.println("drop keyspace <keyspace>                                      Delete a keyspace.");
            css_.out.println("drop column family <cf>                                  Delete a column family.");
            css_.out.println("rename keyspace <keyspace> <keyspace_new_name>                Rename a keyspace.");
            css_.out.println("rename column family <cf> <new_name>                     Rename a column family.");
            css_.out.println("get <cf>['<key>']                                        Get a slice of columns.");
            css_.out.println("get <cf>['<key>']['<super>']                         Get a slice of sub columns.");
            css_.out.println("get <cf> where <column> = <value> [and <column> > <value> and ...] [limit int]. ");
            css_.out.println("get <cf>['<key>']['<col>'] (as <type>)*                      Get a column value.");
            css_.out.println("get <cf>['<key>']['<super>']['<col>'] (as <type>)*       Get a sub column value.");
            css_.out.println("set <cf>['<key>']['<col>'] = <value>                               Set a column.");
            css_.out.println("set <cf>['<key>']['<super>']['<col>'] = <value>                Set a sub column.");
            css_.out.println("del <cf>['<key>']                                                 Delete record.");
            css_.out.println("del <cf>['<key>']['<col>']                                        Delete column.");
            css_.out.println("del <cf>['<key>']['<super>']['<col>']                         Delete sub column.");
            css_.out.println("count <cf>['<key>']                                     Count columns in record.");
            css_.out.println("count <cf>['<key>']['<super>']                  Count columns in a super column.");
            css_.out.println("list <cf>                                  List all rows in the column family.");
            css_.out.println("list <cf>[<startKey>:]");
            css_.out.println("                       List rows in the column family beginning with <startKey>.");
            css_.out.println("list <cf>[<startKey>:<endKey>]");
            css_.out.println("        List rows in the column family in the range from <startKey> to <endKey>.");
            css_.out.println("list ... limit N                                    Limit the list results to N.");
        } 
    }

    private void cleanupAndExit()
    {
        CliMain.disconnect();
        System.exit(0);
    }
    
    KsDef getKSMetaData(String keyspace) throws NotFoundException, InvalidRequestException, TException
    {
        // Lazily lookup keyspace meta-data.
        if (!(keyspacesMap.containsKey(keyspace)))
            keyspacesMap.put(keyspace, thriftClient_.describe_keyspace(keyspace));
        return keyspacesMap.get(keyspace);
    }
    
    private void executeCount(CommonTree ast) throws TException, InvalidRequestException, UnavailableException, TimedOutException
    {
       if (!CliMain.isConnected() || !hasKeySpace())
           return;

       int childCount = ast.getChildCount();
       assert(childCount == 1);

       CommonTree columnFamilySpec = (CommonTree)ast.getChild(0);
       assert(columnFamilySpec.getType() == CliParser.NODE_COLUMN_ACCESS);

       String key = CliCompiler.getKey(columnFamilySpec);
       String columnFamily = CliCompiler.getColumnFamily(columnFamilySpec);
       int columnSpecCnt = CliCompiler.numColumnSpecifiers(columnFamilySpec);
       
       ColumnParent colParent;
       
       if (columnSpecCnt == 0)
       {
           colParent = new ColumnParent(columnFamily).setSuper_column((ByteBuffer)null);
       }
       else
       {
           assert (columnSpecCnt == 1);
           colParent = new ColumnParent(columnFamily).setSuper_column(CliCompiler.getColumn(columnFamilySpec, 0).getBytes(Charsets.UTF_8));
       }

       SliceRange range = new SliceRange(FBUtilities.EMPTY_BYTE_BUFFER, FBUtilities.EMPTY_BYTE_BUFFER, false, Integer.MAX_VALUE);
       SlicePredicate predicate = new SlicePredicate().setColumn_names(null).setSlice_range(range);
       
       int count = thriftClient_.get_count(ByteBuffer.wrap(key.getBytes(Charsets.UTF_8)), colParent, predicate, ConsistencyLevel.ONE);
       css_.out.printf("%d columns\n", count);
    }
    
    private void executeDelete(CommonTree ast) throws TException, InvalidRequestException, UnavailableException, TimedOutException
    {
        if (!CliMain.isConnected() || !hasKeySpace())
            return;

        int childCount = ast.getChildCount();
        assert(childCount == 1);

        CommonTree columnFamilySpec = (CommonTree)ast.getChild(0);
        assert(columnFamilySpec.getType() == CliParser.NODE_COLUMN_ACCESS);

        String key = CliCompiler.getKey(columnFamilySpec);
        String columnFamily = CliCompiler.getColumnFamily(columnFamilySpec);
        int columnSpecCnt = CliCompiler.numColumnSpecifiers(columnFamilySpec);

        byte[] superColumnName = null;
        byte[] columnName = null;
        boolean isSuper;

        List<String> cfnames = new ArrayList<String>();
        for (CfDef cfd : keyspacesMap.get(keySpace).cf_defs) {
            cfnames.add(cfd.name);
        }

        int idx = cfnames.indexOf(columnFamily);
        if (idx == -1)
        {
            css_.out.println("No such column family: " + columnFamily);
            return;
        }
            
        isSuper = keyspacesMap.get(keySpace).cf_defs.get(idx).column_type.equals("Super");
     
        if ((columnSpecCnt < 0) || (columnSpecCnt > 2))
        {
            css_.out.println("Invalid row, super column, or column specification.");
            return;
        }
        
        if (columnSpecCnt == 1)
        {
            // table.cf['key']['column']
            if (isSuper)
                superColumnName = CliCompiler.getColumn(columnFamilySpec, 0).getBytes(Charsets.UTF_8);
            else
                columnName = CliCompiler.getColumn(columnFamilySpec, 0).getBytes(Charsets.UTF_8);
        }
        else if (columnSpecCnt == 2)
        {
            // table.cf['key']['column']['column']
            superColumnName = CliCompiler.getColumn(columnFamilySpec, 0).getBytes(Charsets.UTF_8);
            columnName = CliCompiler.getColumn(columnFamilySpec, 1).getBytes(Charsets.UTF_8);
        }

        ColumnPath path = new ColumnPath(columnFamily);
        if(superColumnName != null)
            path.setSuper_column(superColumnName);
        
        if(columnName != null)
            path.setColumn(columnName);
        
        thriftClient_.remove(ByteBuffer.wrap(key.getBytes(Charsets.UTF_8)), path,
                             FBUtilities.timestampMicros(), ConsistencyLevel.ONE);
        css_.out.println(String.format("%s removed.", (columnSpecCnt == 0) ? "row" : "column"));
    }

    private void doSlice(String keyspace, String key, String columnFamily, byte[] superColumnName)
            throws InvalidRequestException, UnavailableException, TimedOutException, TException, IllegalAccessException, NotFoundException, InstantiationException, NoSuchFieldException
    {
        
        ColumnParent parent = new ColumnParent(columnFamily);
        if(superColumnName != null)
            parent.setSuper_column(superColumnName);
                
        SliceRange range = new SliceRange(FBUtilities.EMPTY_BYTE_BUFFER, FBUtilities.EMPTY_BYTE_BUFFER, true, 1000000);
        List<ColumnOrSuperColumn> columns = thriftClient_.get_slice(ByteBuffer.wrap(key.getBytes(Charsets.UTF_8)),parent,
                                                                    new SlicePredicate().setColumn_names(null).setSlice_range(range), ConsistencyLevel.ONE);
        int size = columns.size();

        AbstractType validator;
        CfDef cfDef = getCfDef(columnFamily);
        
        // Print out super columns or columns.
        for (ColumnOrSuperColumn cosc : columns)
        {
            if (cosc.isSetSuper_column())
            {
                SuperColumn superColumn = cosc.super_column;

                css_.out.printf("=> (super_column=%s,", formatSuperColumnName(keyspace, columnFamily, superColumn));
                for (Column col : superColumn.getColumns())
                {
                    validator = getValidatorForValue(cfDef, col.getName());
                    css_.out.printf("\n     (column=%s, value=%s, timestamp=%d)", formatSubcolumnName(keyspace, columnFamily, col),
                                    validator.getString(col.value), col.timestamp);
                }
                
                css_.out.println(")");
            }
            else
            {
                Column column = cosc.column;
                validator = getValidatorForValue(cfDef, column.getName());
                css_.out.printf("=> (column=%s, value=%s, timestamp=%d)\n", formatColumnName(keyspace, columnFamily, column),
                                validator.getString(column.value), column.timestamp);
            }
        }
        
        css_.out.println("Returned " + size + " results.");
    }
 
    private String formatSuperColumnName(String keyspace, String columnFamily, SuperColumn column) throws NotFoundException, TException, IllegalAccessException, InstantiationException, NoSuchFieldException
    {
        return getFormatTypeForColumn(getCfDef(keyspace,columnFamily).comparator_type).getString(column.name);
    }

    private String formatSubcolumnName(String keyspace, String columnFamily, Column subcolumn) throws NotFoundException, TException, IllegalAccessException, InstantiationException, NoSuchFieldException
    {
        return getFormatTypeForColumn(getCfDef(keyspace,columnFamily).subcomparator_type).getString(subcolumn.name);
    }

    private String formatColumnName(String keyspace, String columnFamily, Column column) throws NotFoundException, TException, IllegalAccessException, InstantiationException, NoSuchFieldException
    {
        return getFormatTypeForColumn(getCfDef(keyspace,columnFamily).comparator_type).getString(column.name);
    }

    private AbstractType getFormatTypeForColumn(String compareWith)
    {
        Function function;
        try
        {
            function = Function.valueOf(compareWith.toUpperCase());
        }
        catch (IllegalArgumentException e)
        {
            try
            {
                return FBUtilities.getComparator(compareWith);
            }
            catch (ConfigurationException e1)
            {
                StringBuilder errorMessage = new StringBuilder("Unknown comparator '" + compareWith + "'. ");
                errorMessage.append("Available functions: ");
                throw new RuntimeException(errorMessage.append(Function.getFunctionNames()).toString());
            }
        }

        return function.validator;
    }

    // Execute GET statement
    private void executeGet(CommonTree ast) throws TException, NotFoundException, InvalidRequestException, UnavailableException, TimedOutException, IllegalAccessException, InstantiationException, ClassNotFoundException, NoSuchFieldException
    {
        if (!CliMain.isConnected() || !hasKeySpace())
            return;

        CommonTree columnFamilySpec = (CommonTree) ast.getChild(0);
        
        String key = CliCompiler.getKey(columnFamilySpec);
        String columnFamily = CliCompiler.getColumnFamily(columnFamilySpec);
        int columnSpecCnt = CliCompiler.numColumnSpecifiers(columnFamilySpec);
        CfDef columnFamilyDef = getCfDef(columnFamily);
        boolean isSuper = columnFamilyDef.comparator_type.equals("Super");
        
        byte[] superColumnName = null;
        String columnName;

        // table.cf['key'] -- row slice
        if (columnSpecCnt == 0)
        {
            doSlice(keySpace, key, columnFamily, superColumnName);
            return;
        }
        
        // table.cf['key']['column'] -- slice of a super, or get of a standard
        if (columnSpecCnt == 1)
        {
            if (isSuper)
            {
                superColumnName = CliCompiler.getColumn(columnFamilySpec, 0).getBytes(Charsets.UTF_8);
                doSlice(keySpace, key, columnFamily, superColumnName);
                return;
            }
            else 
            {
                 columnName = CliCompiler.getColumn(columnFamilySpec, 0);
            }
        }
        // table.cf['key']['column']['column'] -- get of a sub-column
        else if (columnSpecCnt == 2)
        {
            superColumnName = CliCompiler.getColumn(columnFamilySpec, 0).getBytes(Charsets.UTF_8);
            columnName = CliCompiler.getColumn(columnFamilySpec, 1);
        }
        // The parser groks an arbitrary number of these so it is possible to get here.
        else
        {
            css_.out.println("Invalid row, super column, or column specification.");
            return;
        }

        ByteBuffer columnNameInBytes = columnNameAsBytes(columnName, columnFamily);
        AbstractType validator = getValidatorForValue(columnFamilyDef, columnNameInBytes.array());
        
        // Perform a get()
        ColumnPath path = new ColumnPath(columnFamily);
        if(superColumnName != null) path.setSuper_column(superColumnName);
        if(columnNameInBytes != null) path.setColumn(columnNameInBytes);
        Column column = thriftClient_.get(ByteBuffer.wrap(key.getBytes(Charsets.UTF_8)), path, ConsistencyLevel.ONE).column;

        byte[] columnValue = column.getValue();       
        String valueAsString;
        
        // we have ^(CONVERT_TO_TYPE <type>) inside of GET statement
        // which means that we should try to represent byte[] value according
        // to specified type
        if (ast.getChildCount() == 2)
        {
            // getting ^(CONVERT_TO_TYPE <type>) tree 
            Tree typeTree = ast.getChild(1).getChild(0);
            // .getText() will give us <type>
            String typeName = CliUtils.unescapeSQLString(typeTree.getText());
            // building AbstractType from <type>
            AbstractType valueValidator = getFormatTypeForColumn(typeName);

            // setting value for output
            valueAsString = valueValidator.getString(ByteBuffer.wrap(columnValue));
            // updating column value validator class
            updateColumnMetaData(columnFamilyDef, columnNameInBytes, valueValidator.getClass().getName());
        }
        else
        {
            valueAsString = (validator == null) ? new String(columnValue, Charsets.UTF_8) : validator.getString(ByteBuffer.wrap(columnValue));
        }

        // print results
        css_.out.printf("=> (column=%s, value=%s, timestamp=%d)\n",
                        formatColumnName(keySpace, columnFamily, column), valueAsString, column.timestamp);
    }

    /**
     * Process get operation with conditions (using Thrift get_indexed_slices method)
     * @param statement - tree representation of the current statement
     * Format: ^(NODE_THRIFT_GET_WITH_CONDITIONS cf ^(CONDITIONS ^(CONDITION >= column1 value1) ...) ^(NODE_LIMIT int)*)
     */
    private void executeGetWithConditions(Tree statement)
    {
        if (!CliMain.isConnected() || !hasKeySpace())
            return;

        IndexClause clause = new IndexClause();
        String columnFamily = statement.getChild(0).getText();
        // ^(CONDITIONS ^(CONDITION $column $value) ...)
        Tree conditions = statement.getChild(1);
        
        // fetching column family definition
        CfDef columnFamilyDef = getCfDef(columnFamily);

        // fetching all columns
        SlicePredicate predicate = new SlicePredicate();
        SliceRange sliceRange = new SliceRange();
        sliceRange.setStart(new byte[0]).setFinish(new byte[0]);
        predicate.setSlice_range(sliceRange);

        for (int i = 0; i < conditions.getChildCount(); i++)
        {
            // ^(CONDITION operator $column $value)
            Tree condition = conditions.getChild(i);

            // =, >, >=, <, <=
            String operator = condition.getChild(0).getText();
            String columnNameString  = CliUtils.unescapeSQLString(condition.getChild(1).getText());
            // it could be a basic string or function call
            Tree valueTree = condition.getChild(2);

            try
            {
                ByteBuffer value;
                ByteBuffer columnName = columnNameAsBytes(columnNameString, columnFamily);

                if (valueTree.getType() == CliParser.FUNCTION_CALL)
                {
                    value = convertValueByFunction(valueTree, columnFamilyDef, columnName);
                }
                else
                {
                    String valueString = CliUtils.unescapeSQLString(valueTree.getText());
                    value = columnValueAsBytes(columnName, columnFamily, valueString);
                }

                // index operator from string
                IndexOperator idxOperator = CliUtils.getIndexOperator(operator);
                // adding new index expression into index clause
                clause.addToExpressions(new IndexExpression(columnName, idxOperator, value));
            }
            catch (Exception e)
            {
                throw new RuntimeException(e.getMessage());
            }
        }

        List<KeySlice> slices;
        clause.setStart_key(new byte[] {});

        // when we have ^(NODE_LIMIT Integer)
        if (statement.getChildCount() == 3)
        {
            Tree limitNode = statement.getChild(2);
            int limitValue = Integer.valueOf(limitNode.getChild(0).getText());

            if (limitValue == 0)
            {
                throw new IllegalArgumentException("LIMIT should be greater than zero.");
            }
            
            clause.setCount(limitValue);    
        }

        try
        {
            ColumnParent parent = new ColumnParent(columnFamily);
            slices = thriftClient_.get_indexed_slices(parent, clause, predicate, ConsistencyLevel.ONE);
            printSliceList(columnFamilyDef, slices);
        }
        catch (InvalidRequestException e)
        {
            throw new RuntimeException(e.getWhy());
        }
        catch (Exception e)
        {
            throw new RuntimeException(e.getMessage());
        }
    }

    // Execute SET statement
    private void executeSet(CommonTree ast)
    throws TException, InvalidRequestException, UnavailableException, TimedOutException, NoSuchFieldException, InstantiationException, IllegalAccessException
    {
        if (!CliMain.isConnected() || !hasKeySpace())
            return;
        
        // ^(NODE_COLUMN_ACCESS <cf> <key> <column>)
        CommonTree columnFamilySpec = (CommonTree) ast.getChild(0);
        
        String key = CliCompiler.getKey(columnFamilySpec);
        String columnFamily = CliCompiler.getColumnFamily(columnFamilySpec);
        int columnSpecCnt = CliCompiler.numColumnSpecifiers(columnFamilySpec);
        String value = CliUtils.unescapeSQLString(ast.getChild(1).getText());
        Tree valueTree = ast.getChild(1);
        
        byte[] superColumnName = null;
        String columnName;

        // table.cf['key']
        if (columnSpecCnt == 0)
        {
            css_.err.println("No column name specified, (type 'help' or '?' for help on syntax).");
            return;
        }
        // table.cf['key']['column'] = 'value'
        else if (columnSpecCnt == 1)
        {
            // get the column name
            columnName = CliCompiler.getColumn(columnFamilySpec, 0);
        }
        // table.cf['key']['super_column']['column'] = 'value'
        else
        {
            assert (columnSpecCnt == 2) : "serious parsing error (this is a bug).";
            
            // get the super column and column names
            superColumnName = CliCompiler.getColumn(columnFamilySpec, 0).getBytes(Charsets.UTF_8);
            columnName = CliCompiler.getColumn(columnFamilySpec, 1);
        }


        ByteBuffer columnNameInBytes = columnNameAsBytes(columnName, columnFamily);
        ByteBuffer columnValueInBytes;

        switch (valueTree.getType())
        {
        case CliParser.FUNCTION_CALL:
            columnValueInBytes = convertValueByFunction(valueTree, getCfDef(columnFamily), columnNameInBytes, true);
            break;
        default:
            columnValueInBytes = columnValueAsBytes(columnNameInBytes, columnFamily, value);
        }

        ColumnParent parent = new ColumnParent(columnFamily);
        if(superColumnName != null)
            parent.setSuper_column(superColumnName);
        
        // do the insert
        thriftClient_.insert(ByteBuffer.wrap(key.getBytes(Charsets.UTF_8)), parent,
                             new Column(columnNameInBytes, columnValueInBytes, FBUtilities.timestampMicros()), ConsistencyLevel.ONE);
        
        css_.out.println("Value inserted.");
    }
    
    private void executeShowClusterName() throws TException
    {
        if (!CliMain.isConnected())
            return;
        css_.out.println(thriftClient_.describe_cluster_name());
    }

    /**
     * Add a keyspace
     * @param statement - a token tree representing current statement
     */
    private void executeAddKeyspace(Tree statement)
    {

        if (!CliMain.isConnected())
        {
            return;
        }
        
        // defaults
        List<CfDef> columnList = new LinkedList<CfDef>();
        
        // first value is the keyspace name, after that it is all key=value
        String keyspaceName = statement.getChild(0).getText();
        KsDef ksDef = new KsDef(keyspaceName, DEFAULT_PLACEMENT_STRATEGY, 1, columnList);

        try
        {
            css_.out.println(thriftClient_.system_add_keyspace(updateKsDefAttributes(statement, ksDef)));
            keyspacesMap.put(keyspaceName, thriftClient_.describe_keyspace(keyspaceName));
        }
        catch (InvalidRequestException e)
        {
            throw new RuntimeException(e.getWhy());
        }
        catch (Exception e)
        {
            throw new RuntimeException(e.getMessage(), e);
        }
    }


    /**
     * Add a column family
     * @param statement - a token tree representing current statement
     */
    private void executeAddColumnFamily(Tree statement)
    {
        if (!CliMain.isConnected() || !hasKeySpace())
        {
            return;
        }

        // first value is the column family name, after that it is all key=value
        String columnFamilyName = statement.getChild(0).getText();
        CfDef cfDef = new CfDef(keySpace, columnFamilyName);

        try
        {
            css_.out.println(thriftClient_.system_add_column_family(updateCfDefAttributes(statement, cfDef)));
            keyspacesMap.put(keySpace, thriftClient_.describe_keyspace(keySpace));
        }
        catch (InvalidRequestException e)
        {
            throw new RuntimeException(e.getWhy());
        }
        catch (Exception e)
        {
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    /**
     * Update existing keyspace identified by name
     * @param statement - tree represeting statement
     */
    private void executeUpdateKeyspace(Tree statement)
    {
        if (!CliMain.isConnected())
        {
            return;
        }

        String keyspaceName = statement.getChild(0).getText();
        
        try
        {
            KsDef currentKsDef = getKSMetaData(keyspaceName);
            KsDef updatedKsDef = updateKsDefAttributes(statement, currentKsDef);

            css_.out.println(thriftClient_.system_update_keyspace(updatedKsDef));
            keyspacesMap.put(keyspaceName, thriftClient_.describe_keyspace(keyspaceName));
        }
        catch (InvalidRequestException e)
        {
            throw new RuntimeException(e.getWhy());
        }
        catch (Exception e)
        {
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    /**
     * Update existing column family identified by name
     * @param statement - tree represeting statement
     */
    private void executeUpdateColumnFamily(Tree statement)
    {
        if (!CliMain.isConnected() || !hasKeySpace())
        {
            return;
        }

        String columnFamilyName = statement.getChild(0).getText();
        CfDef cfDef = getCfDef(columnFamilyName);

        try
        {
            css_.out.println(thriftClient_.system_update_column_family(updateCfDefAttributes(statement, cfDef)));
            keyspacesMap.put(keySpace, thriftClient_.describe_keyspace(keySpace));
        }
        catch (InvalidRequestException e)
        {
            throw new RuntimeException(e.getWhy());
        }
        catch (Exception e)
        {
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    /**
     * Used to update keyspace definition attributes
     * @param statement - ANTRL tree representing current statement
     * @param ksDefToUpdate - keyspace definition to update
     * @return ksDef - updated keyspace definition
     */
    private KsDef updateKsDefAttributes(Tree statement, KsDef ksDefToUpdate)
    {
        KsDef ksDef = new KsDef(ksDefToUpdate);
        
        // removing all column definitions - thrift system_update_keyspace method requires that 
        ksDef.setCf_defs(new LinkedList<CfDef>());
        
        for(int i = 1; i < statement.getChildCount(); i += 2)
        {
            String currentStatement = statement.getChild(i).getText().toUpperCase();
            AddKeyspaceArgument mArgument = AddKeyspaceArgument.valueOf(currentStatement);
            String mValue = statement.getChild(i + 1).getText();

            switch(mArgument)
            {
            case PLACEMENT_STRATEGY: 
                ksDef.setStrategy_class(CliUtils.unescapeSQLString(mValue));
                break;
            case REPLICATION_FACTOR:
                ksDef.setReplication_factor(Integer.parseInt(mValue));
                break;
            case STRATEGY_OPTIONS:
                ksDef.setStrategy_options(getStrategyOptionsFromTree(statement.getChild(i + 1)));
                break;
            default:
                //must match one of the above or we'd throw an exception at the valueOf statement above.
                assert(false);
            }
        }

        return ksDef;
    }
    
    /**
     * Update column family definition attributes
     * @param statement - ANTLR tree representing current statement
     * @param cfDefToUpdate - column family definition to apply updates on
     * @return cfDef - updated column family definition
     */
    private CfDef updateCfDefAttributes(Tree statement, CfDef cfDefToUpdate)
    {
        CfDef cfDef = new CfDef(cfDefToUpdate);

        for (int i = 1; i < statement.getChildCount(); i += 2)
        {
            String currentArgument = statement.getChild(i).getText().toUpperCase();
            ColumnFamilyArgument mArgument = ColumnFamilyArgument.valueOf(currentArgument);
            String mValue = statement.getChild(i + 1).getText();

            switch(mArgument)
            {
            case COLUMN_TYPE:
                cfDef.setColumn_type(CliUtils.unescapeSQLString(mValue));
                break;
            case COMPARATOR:
                cfDef.setComparator_type(CliUtils.unescapeSQLString(mValue));
                break;
            case SUBCOMPARATOR:
                cfDef.setSubcomparator_type(CliUtils.unescapeSQLString(mValue));
                break;
            case COMMENT:
                cfDef.setComment(CliUtils.unescapeSQLString(mValue));
                break;
            case ROWS_CACHED:
                cfDef.setRow_cache_size(Double.parseDouble(mValue));
                break;
            case KEYS_CACHED:
                cfDef.setKey_cache_size(Double.parseDouble(mValue));
                break;
            case READ_REPAIR_CHANCE:
                cfDef.setRead_repair_chance(Double.parseDouble(mValue));
                break;
            case GC_GRACE:
                cfDef.setGc_grace_seconds(Integer.parseInt(mValue));
                break;
            case COLUMN_METADATA:
                Tree arrayOfMetaAttributes = statement.getChild(i + 1);
                if (!arrayOfMetaAttributes.getText().equals("ARRAY"))
                    throw new RuntimeException("'column_metadata' format - [{ k:v, k:v, ..}, { ... }, ...]");
                cfDef.setColumn_metadata(getCFColumnMetaFromTree(arrayOfMetaAttributes));
                break;
            case MEMTABLE_OPERATIONS:
                cfDef.setMemtable_operations_in_millions(Double.parseDouble(mValue));
                break;
            case MEMTABLE_FLUSH_AFTER:
                cfDef.setMemtable_flush_after_mins(Integer.parseInt(mValue));
                break;
            case MEMTABLE_THROUGHPUT:
                cfDef.setMemtable_throughput_in_mb(Integer.parseInt(mValue));
                break;
            case ROW_CACHE_SAVE_PERIOD:
                cfDef.setRow_cache_save_period_in_seconds(Integer.parseInt(mValue));
                break;
            case KEY_CACHE_SAVE_PERIOD:
                cfDef.setKey_cache_save_period_in_seconds(Integer.parseInt(mValue));
                break;
            case DEFAULT_VALIDATION_CLASS:
                cfDef.setDefault_validation_class(mValue);
                break;
            case MIN_COMPACTION_THRESHOLD:
                cfDef.setMin_compaction_threshold(Integer.parseInt(mValue));
                break;
            case MAX_COMPACTION_THRESHOLD:
                cfDef.setMax_compaction_threshold(Integer.parseInt(mValue));
                break;
            default:
                //must match one of the above or we'd throw an exception at the valueOf statement above.
                assert(false);

            }
        }

        return cfDef;
    }

    /**
     * Delete a keyspace
     * @param ast - a token tree representing current statement
     * @throws TException - exception
     * @throws InvalidRequestException - exception
     * @throws NotFoundException - exception
     */
    private void executeDelKeyspace(CommonTree ast) throws TException, InvalidRequestException, NotFoundException
    {
        if (!CliMain.isConnected())
        {
            return;
        }

        String keyspaceName = ast.getChild(0).getText();

        css_.out.println(thriftClient_.system_drop_keyspace(keyspaceName));
    }

    /**
     * Delete a column family
     * @param ast - a token tree representing current statement
     * @throws TException - exception
     * @throws InvalidRequestException - exception
     * @throws NotFoundException - exception
     */
    private void executeDelColumnFamily(CommonTree ast) throws TException, InvalidRequestException, NotFoundException
    {
        if (!CliMain.isConnected() || !hasKeySpace())
        {
            return;
        }
        String columnName = ast.getChild(0).getText();
        css_.out.println(thriftClient_.system_drop_column_family(columnName));
    }

    private void executeList(CommonTree ast)
    throws TException, InvalidRequestException, NotFoundException, IllegalAccessException, InstantiationException, NoSuchFieldException, UnavailableException, TimedOutException
    {
        if (!CliMain.isConnected())
            return;

        Iterator<CommonTree> iter = ast.getChildren().iterator();

        // extract column family
        String columnFamily = iter.next().getText();

        String startKey = "";
        String endKey = "";
        int limitCount = Integer.MAX_VALUE; // will reset to default later if it's not specified

        // optional arguments: key range and limit
        while (iter.hasNext())
        {
            CommonTree child = iter.next();
            if (child.getType() == CliParser.NODE_KEY_RANGE)
            {
                if (child.getChildCount() > 0)
                {
                    startKey = CliUtils.unescapeSQLString(child.getChild(0).getText());
                    if (child.getChildCount() > 1)
                        endKey = CliUtils.unescapeSQLString(child.getChild(1).getText());
                }
            }
            else
            {
                if (child.getChildCount() != 1)
                {
                    css_.out.println("Invalid limit clause");
                    return;
                }
                limitCount = Integer.parseInt(child.getChild(0).getText());
                if (limitCount <= 0)
                {
                    css_.out.println("Invalid limit " + limitCount);
                    return;
                }
            }
        }

        if (limitCount == Integer.MAX_VALUE)
        {
            limitCount = 100;
            css_.out.println("Using default limit of 100");
        }

        CfDef columnFamilyDef = getCfDef(columnFamily);

        // read all columns and superColumns
        SlicePredicate predicate = new SlicePredicate();
        SliceRange sliceRange = new SliceRange();
        sliceRange.setStart(new byte[0]).setFinish(new byte[0]);
        predicate.setSlice_range(sliceRange);

        // set the key range
        KeyRange range = new KeyRange(limitCount);
        range.setStart_key(startKey.getBytes()).setEnd_key(endKey.getBytes());

        ColumnParent columnParent = new ColumnParent(columnFamily);
        List<KeySlice> keySlices = thriftClient_.get_range_slices(columnParent, predicate, range, ConsistencyLevel.ONE);
        int toIndex = keySlices.size();

        if (limitCount < keySlices.size())
        {

            // limitCount could be Integer.MAX_VALUE
            toIndex = limitCount;
        }


        printSliceList(columnFamilyDef, keySlices.subList(0, toIndex));
    }

    private void executeShowVersion() throws TException
    {
        if (!CliMain.isConnected())
            return;
        css_.out.println(thriftClient_.describe_version());
    }

    // process "show tables" statement
    private void executeShowTables() throws TException, InvalidRequestException
    {
        if (!CliMain.isConnected())
            return;
        
        List<KsDef> tables = thriftClient_.describe_keyspaces();
        for (KsDef t : tables) {
            describeTableInternal(t.name, t);
        }
    }
    
    private boolean hasKeySpace() 
    {
    	if (keySpace == null)
        {
            css_.out.println("Not authenticated to a working keyspace.");
            return false;
        }
        return true;
    }
    
    public String getKeySpace() 
    {
        return keySpace == null ? "unknown" : keySpace;
    }
    
    public void setKeyspace(String keySpace) throws NotFoundException, InvalidRequestException, TException 
    {
        this.keySpace = keySpace;
        //We do nothing with the return value, but it hits a cache and
        // the tab-completer.
        getKSMetaData(keySpace);
    }
    
    public String getUsername() 
    {
        return username == null ? "default" : username;
    }
    
    public void setUsername(String username)
    {
        this.username = username;
    }
    
    private void executeUseTable(CommonTree ast) throws TException
    {
        if (!CliMain.isConnected())
            return;
    	
        int childCount = ast.getChildCount();
        String tableName, username = null, password = null;
        assert(childCount > 0);

        // Get table name
        tableName = ast.getChild(0).getText();
        
        if (childCount == 3) {
            username  = ast.getChild(1).getText();
            password  = ast.getChild(2).getText();
        }
        
        if( tableName == null ) 
        {
            css_.out.println("Keyspace argument required");
            return;
        }
        
        try 
        {
        	AuthenticationRequest authRequest;
        	Map<String, String> credentials = new HashMap<String, String>();
        	
        	
        	thriftClient_.set_keyspace(tableName);
   
        	if (username != null && password != null) 
        	{
        	    /* remove quotes */
        	    password = password.replace("\'", "");
        	    credentials.put(SimpleAuthenticator.USERNAME_KEY, username);
                credentials.put(SimpleAuthenticator.PASSWORD_KEY, password);
                authRequest = new AuthenticationRequest(credentials);
                thriftClient_.login(authRequest);
        	}
        	
            keySpace = tableName;
            this.username = username != null ? username : "default";
            
            if (!(keyspacesMap.containsKey(keySpace))) 
            {
                keyspacesMap.put(keySpace, thriftClient_.describe_keyspace(keySpace));
            }
            Set<String> cfnames = new HashSet<String>();
            KsDef ksd = keyspacesMap.get(keySpace);
            for (CfDef cfd : ksd.cf_defs) {
                cfnames.add(cfd.name);
            }
            CliMain.updateCompletor(cfnames);
            css_.out.println("Authenticated to keyspace: " + keySpace);
        } 
        catch (AuthenticationException e) 
        {
            css_.err.println("Exception during authentication to the cassandra node: " +
            		"verify keyspace exists, and you are using correct credentials.");
        } 
        catch (AuthorizationException e) 
        {
            css_.err.println("You are not authorized to use keyspace: " + tableName);
        }
        catch (InvalidRequestException e)
        {
            css_.err.println(tableName + " does not exist.");
        }
        catch (NotFoundException e)
        {
            css_.err.println(tableName + " does not exist.");
        } 
        catch (TException e) 
        {
            if (css_.debug)
                e.printStackTrace();
            
            css_.err.println("Login failure. Did you specify 'keyspace', 'username' and 'password'?");
        }
    }

    private void describeTableInternal(String tableName, KsDef metadata) throws TException {
        // Describe and display
        css_.out.println("Keyspace: " + tableName + ":");
        try
        {
            KsDef ks_def;
            ks_def = metadata == null ? thriftClient_.describe_keyspace(tableName) : metadata;
            css_.out.println("  Replication Factor: " + ks_def.replication_factor);
            css_.out.println("  Column Families:");

            for (CfDef cf_def : ks_def.cf_defs)
            {
                css_.out.printf("    ColumnFamily: %s%s\n", cf_def.name, cf_def.column_type.equals("Super") ? " (Super)" : "");
                if (cf_def.comment != null && !cf_def.comment.isEmpty())
                    css_.out.printf("    \"%s\"\n", cf_def.comment);
                css_.out.printf("      Columns sorted by: %s%s\n", cf_def.comparator_type, cf_def.column_type.equals("Super") ? "/" + cf_def.subcomparator_type : "");
                if (cf_def.subcomparator_type != null)
                    css_.out.println("      Subcolumns sorted by: " + cf_def.comparator_type);
                css_.out.printf("      Row cache size / save period: %s/%s\n", cf_def.row_cache_size, cf_def.row_cache_save_period_in_seconds);
                css_.out.printf("      Key cache size / save period: %s/%s\n", cf_def.key_cache_size, cf_def.key_cache_save_period_in_seconds);
                css_.out.printf("      Memtable thresholds: %s/%s/%s\n",
                                cf_def.memtable_operations_in_millions, cf_def.memtable_throughput_in_mb, cf_def.memtable_flush_after_mins);
                css_.out.printf("      GC grace seconds: %s\n", cf_def.gc_grace_seconds);
                css_.out.printf("      Compaction min/max thresholds: %s/%s\n", cf_def.min_compaction_threshold, cf_def.max_compaction_threshold);

                if (cf_def.getColumn_metadataSize() != 0)
                {
                    String leftSpace = "      ";
                    String columnLeftSpace = leftSpace + "    ";

                    AbstractType columnNameValidator = getFormatTypeForColumn(cf_def.comparator_type);

                    css_.out.println(leftSpace + "Column Metadata:");
                    for (ColumnDef columnDef : cf_def.getColumn_metadata())
                    {
                        String columnName = columnNameValidator.getString(columnDef.name);

                        css_.out.println(leftSpace + "  Column Name: " + columnName);
                        css_.out.println(columnLeftSpace + "Validation Class: " + columnDef.getValidation_class());

                        if (columnDef.isSetIndex_name())
                        {
                            css_.out.println(columnLeftSpace + "Index Name: " + columnDef.getIndex_name());
                        }

                        if (columnDef.isSetIndex_type())
                        {
                            css_.out.println(columnLeftSpace + "Index Type: " + columnDef.getIndex_type().name());
                        }
                    }
                }
            }
        }
        catch (InvalidRequestException e)
        {
            css_.out.println("Invalid request: " + e);
        }
        catch (NotFoundException e)
        {
            css_.out.println("Keyspace " + tableName + " could not be found.");
        }
    }
    // process a statement of the form: describe table <tablename> 
    private void executeDescribeTable(CommonTree ast) throws TException
    {
        if (!CliMain.isConnected())
            return;

        // Get table name
        int childCount = ast.getChildCount();
        assert(childCount == 1);

        String tableName = ast.getChild(0).getText();

        if( tableName == null ) {
            css_.out.println("Keyspace argument required");
            return;
        }
        
        describeTableInternal(tableName, null);
    }

    // process a statement of the form: connect hostname/port
    private void executeConnect(CommonTree ast)
    {
        int portNumber = Integer.parseInt(ast.getChild(1).getText());
        Tree idList = ast.getChild(0);
        
        StringBuilder hostName = new StringBuilder();
        int idCount = idList.getChildCount(); 
        for (int idx = 0; idx < idCount; idx++)
        {
            hostName.append(idList.getChild(idx).getText());
        }
        
        // disconnect current connection, if any.
        // This is a no-op, if you aren't currently connected.
        CliMain.disconnect();

        // now, connect to the newly specified host name and port
        css_.hostName = hostName.toString();
        css_.thriftPort = portNumber;
        CliMain.connect(css_.hostName, css_.thriftPort);
    }

    /**
     * To get Column Family Definition object from specified keyspace
     * @param keySpaceName key space name to search for specific column family
     * @param columnFamilyName column family name 
     * @return CfDef - Column family definition object
     */
    private CfDef getCfDef(String keySpaceName, String columnFamilyName)
    {
        KsDef keySpaceDefinition = keyspacesMap.get(keySpaceName);
        
        for (CfDef columnFamilyDef : keySpaceDefinition.cf_defs)
        {
            if (columnFamilyDef.name.equals(columnFamilyName))
            {
                return columnFamilyDef;
            }
        }

        throw new RuntimeException("No such column family: " + columnFamilyName);
    }

    /**
     * Uses getCfDef(keySpaceName, columnFamilyName) with current keyspace
     * @param columnFamilyName column family name to find in specified keyspace
     * @return CfDef - Column family definition object
     */
    private CfDef getCfDef(String columnFamilyName)
    {
        return getCfDef(this.keySpace, columnFamilyName);
    }
    
    /**
     * Used to parse meta tree and compile meta attributes into List<ColumnDef>
     * @param meta (Tree representing Array of the hashes with metadata attributes)
     * @return List<ColumnDef> List of the ColumnDef's
     * 
     * meta is in following format - ^(ARRAY ^(HASH ^(PAIR .. ..) ^(PAIR .. ..)) ^(HASH ...))
     */
    private List<ColumnDef> getCFColumnMetaFromTree(Tree meta)
    {
        // this list will be returned
        List<ColumnDef> columnDefinitions = new ArrayList<ColumnDef>();
        
        // each child node is a ^(HASH ...)
        for (int i = 0; i < meta.getChildCount(); i++)
        {
            Tree metaHash = meta.getChild(i);

            ColumnDef columnDefinition = new ColumnDef();
            
            // each child node is ^(PAIR $key $value)
            for (int j = 0; j < metaHash.getChildCount(); j++)
            {
                Tree metaPair = metaHash.getChild(j);

                // current $key
                String metaKey = CliUtils.unescapeSQLString(metaPair.getChild(0).getText());
                // current $value
                String metaVal = CliUtils.unescapeSQLString(metaPair.getChild(1).getText());

                if (metaKey.equals("column_name"))
                {
                    columnDefinition.setName(metaVal.getBytes(Charsets.UTF_8));
                }
                else if (metaKey.equals("validation_class"))
                {
                    columnDefinition.setValidation_class(metaVal);
                }
                else if (metaKey.equals("index_type"))
                {
                    columnDefinition.setIndex_type(getIndexTypeFromString(metaVal));
                }
                else if (metaKey.equals("index_name"))
                {
                    columnDefinition.setIndex_name(metaVal);    
                }
            }

            // validating columnDef structure, 'name' and 'validation_class' must be set 
            try
            {
                columnDefinition.validate();
            }
            catch (TException e)
            {
                throw new RuntimeException(e.getMessage(), e);
            }

            columnDefinitions.add(columnDefinition);
        }

        return columnDefinitions;
    }

    /**
     * Getting IndexType object from indexType string
     * @param indexTypeAsString - string return by parser corresponding to IndexType 
     * @return IndexType - an IndexType object
     */
    private IndexType getIndexTypeFromString(String indexTypeAsString)
    {
        Integer indexTypeId;
        IndexType indexType;

        try {
            indexTypeId = new Integer(indexTypeAsString);
        }
        catch (NumberFormatException e) {
            throw new RuntimeException("Could not convert " + indexTypeAsString + " into Integer.");
        }

        indexType = IndexType.findByValue(indexTypeId);

        if (indexType == null) {
            throw new RuntimeException(indexTypeAsString + " is unsupported.");
        }

        return indexType;
    }

    /**
     * Converts object represented as string into byte[] according to comparator
     * @param object - object to covert into byte array
     * @param comparator - comparator used to convert object
     * @return byte[] - object in the byte array representation
     */
    private ByteBuffer getBytesAccordingToType(String object, AbstractType comparator)
    {
        if (comparator instanceof LongType)
        {
            long longType;
            try
            {
                longType = Long.valueOf(object);
            }
            catch (Exception e)
            {
                throw new RuntimeException("'" + object + "' could not be translated into a LongType.");
            }

            return FBUtilities.toByteBuffer(longType);
        }
        else if (comparator instanceof LexicalUUIDType || comparator instanceof TimeUUIDType)
        {
            UUID uuid = UUID.fromString(object);

            if (comparator instanceof TimeUUIDType && uuid.version() != 1)
                throw new IllegalArgumentException("TimeUUID supports only version 1 UUIDs");    

            return ByteBuffer.wrap(UUIDGen.decompose(uuid));    
        }
        else if (comparator instanceof IntegerType)
        {
            BigInteger integerType;

            try
            {
                integerType =  new BigInteger(object);
            }
            catch (Exception e)
            {
                throw new RuntimeException("'" + object + "' could not be translated into an IntegerType.");
            }

            return ByteBuffer.wrap(integerType.toByteArray());
        }
        else if (comparator instanceof AsciiType)
        {
            return ByteBuffer.wrap(object.getBytes(Charsets.US_ASCII));
        }
        else
        {
            return ByteBuffer.wrap(object.getBytes(Charsets.UTF_8));
        }
    }
    
    /**
     * Converts column name into byte[] according to comparator type
     * @param column - column name from parser
     * @param columnFamily - column family name from parser
     * @return byte[] - array of bytes in which column name was converted according to comparator type
     * @throws NoSuchFieldException - raised from getFormatTypeForColumn call
     * @throws InstantiationException - raised from getFormatTypeForColumn call
     * @throws IllegalAccessException - raised from getFormatTypeForColumn call
     */
    private ByteBuffer columnNameAsBytes(String column, String columnFamily) throws NoSuchFieldException, InstantiationException, IllegalAccessException
    {
        CfDef columnFamilyDef   = getCfDef(columnFamily);
        String comparatorClass  = columnFamilyDef.comparator_type;

        return getBytesAccordingToType(column, getFormatTypeForColumn(comparatorClass));   
    }

    /**
     * Converts column value into byte[] according to validation class
     * @param columnName - column name to which value belongs
     * @param columnFamilyName - column family name
     * @param columnValue - actual column value
     * @return byte[] - value in byte array representation
     */
    private ByteBuffer columnValueAsBytes(ByteBuffer columnName, String columnFamilyName, String columnValue)
    {
        CfDef columnFamilyDef = getCfDef(columnFamilyName);
        
        for (ColumnDef columnDefinition : columnFamilyDef.getColumn_metadata())
        {
            byte[] currentColumnName = columnDefinition.getName();

            if (ByteBufferUtil.compare(currentColumnName,columnName)==0)
            {
                try
                {
                    String validationClass = columnDefinition.getValidation_class();
                    return getBytesAccordingToType(columnValue, getFormatTypeForColumn(validationClass));
                }
                catch (Exception e)
                {
                    throw new RuntimeException(e.getMessage(), e);
                }
            }
        }

        // if no validation were set returning simple .getBytes()
        return ByteBuffer.wrap(columnValue.getBytes());
    }

    /**
     * Get validator for specific column value
     * @param ColumnFamilyDef - CfDef object representing column family with metadata
     * @param columnNameInBytes - column name as byte array
     * @return AbstractType - validator for column value
     */
    private AbstractType getValidatorForValue(CfDef ColumnFamilyDef, byte[] columnNameInBytes)
    {
        String defaultValidator = ColumnFamilyDef.default_validation_class;
        
        for (ColumnDef columnDefinition : ColumnFamilyDef.getColumn_metadata())
        {
            byte[] nameInBytes = columnDefinition.getName();

            if (nameInBytes.equals(columnNameInBytes))
            {
                return getFormatTypeForColumn(columnDefinition.getValidation_class());
            }
        }

        if (defaultValidator != null && !defaultValidator.isEmpty()) 
        {
            return getFormatTypeForColumn(defaultValidator);
        }

        return null;
    }

    /**
     * Used to get Map of the provided options by create/update keyspace commands
     * @param options - tree representing options
     * @return Map - strategy_options map
     */
    private Map<String, String> getStrategyOptionsFromTree(Tree options)
    {
        // this map will be returned
        Map<String, String> strategyOptions = new HashMap<String, String>();

        // each child node is a ^(HASH ...)
        for (int i = 0; i < options.getChildCount(); i++)
        {
            Tree optionsHash = options.getChild(i);
            
            // each child node is ^(PAIR $key $value)
            for (int j = 0; j < optionsHash.getChildCount(); j++)
            {
                Tree optionPair = optionsHash.getChild(j);

                // current $key
                String key = CliUtils.unescapeSQLString(optionPair.getChild(0).getText());
                // current $value
                String val = CliUtils.unescapeSQLString(optionPair.getChild(1).getText());

                strategyOptions.put(key, val);
            }
        }

        return strategyOptions;
    }

    /**
     * Used to convert value (function argument, string) into byte[]
     * calls convertValueByFunction method with "withUpdate" set to false
     * @param functionCall - tree representing function call ^(FUNCTION_CALL function_name value)
     * @param columnFamily - column family definition (CfDef) 
     * @param columnName   - also updates column family metadata for given column
     * @return byte[] - string value as byte[] 
     */
    private ByteBuffer convertValueByFunction(Tree functionCall, CfDef columnFamily, ByteBuffer columnName)
    {
        return convertValueByFunction(functionCall, columnFamily, columnName, false);
    }
    
    /**
     * Used to convert value (function argument, string) into byte[]
     * @param functionCall - tree representing function call ^(FUNCTION_CALL function_name value)
     * @param columnFamily - column family definition (CfDef)
     * @param columnName   - column name as byte[] (used to update CfDef)
     * @param withUpdate   - also updates column family metadata for given column
     * @return byte[] - string value as byte[]
     */
    private ByteBuffer convertValueByFunction(Tree functionCall, CfDef columnFamily, ByteBuffer columnName, boolean withUpdate)
    {
        String functionName = functionCall.getChild(0).getText();
        String functionArg  = CliUtils.unescapeSQLString(functionCall.getChild(1).getText());
        Function function;

        try
        {
            function = Function.valueOf(functionName.toUpperCase());
        }
        catch (IllegalArgumentException e)
        {
            StringBuilder errorMessage = new StringBuilder("Function '" + functionName + "' not found. ");
            errorMessage.append("Available functions: ");
            throw new RuntimeException(errorMessage.append(Function.getFunctionNames()).toString());  
        }

        try
        {
            AbstractType validator = function.getValidator();
            ByteBuffer value = getBytesAccordingToType(functionArg, validator);

            // performing ColumnDef local validator update
            if (withUpdate)
            {
                updateColumnMetaData(columnFamily, columnName, validator.getClass().getName());
            }

            return value;
        }
        catch (Exception e)
        {
            throw new RuntimeException(e.getMessage());
        }
    }

    /**
     * Used to locally update column family definition with new column metadata
     * @param columnFamily    - CfDef record
     * @param columnName      - column name represented as byte[]
     * @param validationClass - value validation class
     */
    private void updateColumnMetaData(CfDef columnFamily, ByteBuffer columnName, String validationClass)
    {
        List<ColumnDef> columnMetaData = columnFamily.getColumn_metadata();
        ColumnDef column = getColumnDefByName(columnFamily, columnName);

        if (column != null)
        {
            // if validation class is the same - no need to modify it
            if (column.getValidation_class().equals(validationClass))
                return;

            // updating column definition with new validation_class
            column.setValidation_class(validationClass);
        }
        else
        {
            columnMetaData.add(new ColumnDef(columnName, validationClass));
        }
    }

    /**
     * Get specific ColumnDef in column family meta data by column name
     * @param columnFamily - CfDef record
     * @param columnName   - column name represented as byte[]
     * @return ColumnDef   - found column definition
     */
    private ColumnDef getColumnDefByName(CfDef columnFamily, ByteBuffer columnName)
    {
        for (ColumnDef columnDef : columnFamily.getColumn_metadata())
        {
            byte[] currName = columnDef.getName();

            if (ByteBufferUtil.compare(currName, columnName) == 0)
            {
                return columnDef;
            }
        }

        return null;
    }

    /**
     * Prints out KeySlice list
     * @param columnFamilyDef - column family definition
     * @param slices - list of the KeySlice's to print out
     * @throws NotFoundException - column not found
     * @throws TException - transfer is broken
     * @throws IllegalAccessException - can't do operation
     * @throws InstantiationException - can't instantiate a class
     * @throws NoSuchFieldException - column not found
     */
    private void printSliceList(CfDef columnFamilyDef, List<KeySlice> slices)
    throws NotFoundException, TException, IllegalAccessException, InstantiationException, NoSuchFieldException
    {
        AbstractType validator;
        String columnFamilyName = columnFamilyDef.getName();

        for (KeySlice ks : slices)
        {
            css_.out.printf("-------------------\n");
            css_.out.printf("RowKey: %s\n", ByteBufferUtil.string(ks.key, Charsets.UTF_8));

            Iterator<ColumnOrSuperColumn> iterator = ks.getColumnsIterator();

            while (iterator.hasNext())
            {
                ColumnOrSuperColumn columnOrSuperColumn = iterator.next();

                if (columnOrSuperColumn.column != null)
                {
                    Column col = columnOrSuperColumn.column;
                    validator = getValidatorForValue(columnFamilyDef, col.getName());

                    css_.out.printf("=> (column=%s, value=%s, timestamp=%d)\n",
                                    formatColumnName(keySpace, columnFamilyName, col), validator.getString(col.value), col.timestamp);
                }
                else if (columnOrSuperColumn.super_column != null)
                {
                    SuperColumn superCol = columnOrSuperColumn.super_column;
                    css_.out.printf("=> (super_column=%s,", formatSuperColumnName(keySpace, columnFamilyName, superCol));

                    for (Column col : superCol.columns)
                    {
                        validator = getValidatorForValue(columnFamilyDef, col.getName());

                        css_.out.printf("\n     (column=%s, value=%s, timestamp=%d)",
                                        formatSubcolumnName(keySpace, columnFamilyName, col), validator.getString(col.value), col.timestamp);
                    }

                    css_.out.println(")");
                }
            }
        }

        css_.out.printf("\n%d Row%s Returned.\n", slices.size(), (slices.size() > 1 ? "s" : ""));
    }
}
