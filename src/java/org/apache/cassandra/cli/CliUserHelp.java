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

import org.antlr.runtime.tree.Tree;

import java.util.EnumMap;

/**
 * @author Pavel A. Yaskevich
 */
public class CliUserHelp {

    /*
     * the <i>add column family</i> command requires a list of arguments,
     *  this enum defines which arguments are valid.
     */
    protected enum ColumnFamilyArgument
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

    protected EnumMap<ColumnFamilyArgument, String> argumentExplanations = new EnumMap<ColumnFamilyArgument, String>(ColumnFamilyArgument.class)
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
    
    protected void printCmdHelp(Tree statement, CliSessionState state)
    {
        if (statement.getChildCount() > 0)
        {
            int helpType = statement.getChild(0).getType();

            switch(helpType)
            {
            case CliParser.NODE_HELP:
                state.out.println("help <command>;\n");
                state.out.println("Display the general help page with a list of available commands.");
                break;
            case CliParser.NODE_CONNECT:
                state.out.println("connect <hostname>/<port>;\n");
                state.out.println("Connect to the specified host on the specified port.\n");
                state.out.println("example:");
                state.out.println("connect localhost/9160;");
                break;

            case CliParser.NODE_USE_TABLE:
                state.out.println("use <keyspace>;");
                state.out.println("use <keyspace> <username> '<password>';\n");
                state.out.println("Switch to the specified keyspace. The optional username and password fields");
                state.out.println("are needed when performing authentication.\n");
                break;

            case CliParser.NODE_DESCRIBE_TABLE:
                state.out.println("describe keyspace <keyspace>;\n");
                state.out.println("Show additional information about the specified keyspace.\n");
                state.out.println("example:");
                state.out.println("describe keyspace system;");
                break;

            case CliParser.NODE_EXIT:
                state.out.println("exit;");
                state.out.println("quit;\n");
                state.out.println("Exit this utility.");
                break;

            case CliParser.NODE_SHOW_CLUSTER_NAME:
                state.out.println("show cluster name;\n");
                state.out.println("Displays the name of the currently connected cluster.");
                break;

            case CliParser.NODE_SHOW_VERSION:
                state.out.println("show api version;\n");
                state.out.println("Displays the API version number.");
                break;

            case CliParser.NODE_SHOW_KEYSPACES:
                state.out.println("show keyspaces;\n");
                state.out.println("Displays a list of the keyspaces available on the currently connected cluster.");
                break;

            case CliParser.NODE_ADD_KEYSPACE:
                state.out.println("create keyspace <keyspace>;");
                state.out.println("create keyspace <keyspace> with <att1>=<value1>;");
                state.out.println("create keyspace <keyspace> with <att1>=<value1> and <att2>=<value2> ...;\n");
                state.out.println("Create a new keyspace with the specified values for the given set of attributes.\n");
                state.out.println("valid attributes are:");
                state.out.println("    replication_factor: to how many nodes should entries to this keyspace be");
                state.out.println("                        replicated. Valid entries are integers greater than 0.");
                state.out.println("    placement_strategy: the fully qualified class used to place replicas in");
                state.out.println("                        this keyspace. Valid values are");
                state.out.println("                        org.apache.cassandra.locator.SimpleStrategy,");
                state.out.println("                        org.apache.cassandra.locator.NetworkTopologyStrategy,");
                state.out.println("                        and org.apache.cassandra.locator.OldNetworkTopologyStrategy");
                state.out.println("      strategy_options: additional options for placement_strategy.\n");
                state.out.println("example:");
                state.out.println("create keyspace foo with replication_factor = 3 and ");
                state.out.println("        placement_strategy = 'org.apache.cassandra.locator.SimpleStrategy'");
                state.out.println("        and strategy_options=[{DC1:2, DC2:2}];");
                break;

            case CliParser.NODE_UPDATE_KEYSPACE:
                state.out.println("update keyspace <keyspace>;");
                state.out.println("update keyspace <keyspace> with <att1>=<value1>;");
                state.out.println("update keyspace <keyspace> with <att1>=<value1> and <att2>=<value2> ...;\n");
                state.out.println("Update a keyspace with the specified values for the given set of attributes.\n");
                state.out.println("valid attributes are:");
                state.out.println("    replication_factor: to how many nodes should entries to this keyspace be");
                state.out.println("                        replicated. Valid entries are integers greater than 0.");
                state.out.println("    placement_strategy: the fully qualified class used to place replicas in");
                state.out.println("                        this keyspace. Valid values are");
                state.out.println("                        org.apache.cassandra.locator.SimpleStrategy,");
                state.out.println("                        org.apache.cassandra.locator.NetworkTopologyStrategy,");
                state.out.println("                        and org.apache.cassandra.locator.OldNetworkTopologyStrategy");
                state.out.println("      strategy_options: additional options for placement_strategy.\n");
                state.out.println("example:");
                state.out.println("update keyspace foo with replication_factor = 2 and ");
                state.out.println("        placement_strategy = 'org.apache.cassandra.locator.LocalStrategy'");
                state.out.println("        and strategy_options=[{DC1:1, DC2:4, DC3:2}];");
                break;

            case CliParser.NODE_ADD_COLUMN_FAMILY:
                state.out.println("create column family Bar;");
                state.out.println("create column family Bar with <att1>=<value1>;");
                state.out.println("create column family Bar with <att1>=<value1> and <att2>=<value2>...;\n");
                state.out.println("Create a new column family with the specified values for the given set of");
                state.out.println("attributes. Note that you must be using a keyspace.\n");
                state.out.println("valid attributes are:");
                for (ColumnFamilyArgument argument : ColumnFamilyArgument.values())
                    state.out.printf("    - %s: %s%n", argument.toString().toLowerCase(), argumentExplanations.get(argument));
                state.out.println("    - column_metadata: Metadata which describes columns of column family.");
                state.out.println("        Supported format is [{ k:v, k:v, ... }, { ... }, ...]");
                state.out.println("        Valid attributes: column_name, validation_class (see comparator),");
                state.out.println("                          index_type (integer), index_name.");
                state.out.println("example:\n");
                state.out.println("create column family Bar with column_type = 'Super' and comparator = 'AsciiType'");
                state.out.println("      and rows_cached = 10000;");
                state.out.println("create column family Baz with comparator = 'LongType' and rows_cached = 10000;");
                state.out.print("create column family Foo with comparator=LongType and column_metadata=");
                state.out.print("[{ column_name:Test, validation_class:IntegerType, index_type:0, index_name:IdxName");
                state.out.println("}, { column_name:'other name', validation_class:LongType }];");
                break;

            case CliParser.NODE_UPDATE_COLUMN_FAMILY:
                state.out.println("update column family Bar;");
                state.out.println("update column family Bar with <att1>=<value1>;");
                state.out.println("update column family Bar with <att1>=<value1> and <att2>=<value2>...;\n");
                state.out.println("Update a column family with the specified values for the given set of");
                state.out.println("attributes. Note that you must be using a keyspace.\n");
                state.out.println("valid attributes are:");
                for (ColumnFamilyArgument argument : ColumnFamilyArgument.values())
                {
                    if (argument == ColumnFamilyArgument.COMPARATOR || argument == ColumnFamilyArgument.SUBCOMPARATOR)
                        continue;
                    state.out.printf("    - %s: %s%n", argument.toString().toLowerCase(), argumentExplanations.get(argument));
                }
                state.out.println("    - column_metadata: Metadata which describes columns of column family.");
                state.out.println("        Supported format is [{ k:v, k:v, ... }, { ... }, ...]");
                state.out.println("        Valid attributes: column_name, validation_class (see comparator),");
                state.out.println("                          index_type (integer), index_name.");
                state.out.println("example:\n");
                state.out.print("update column family Foo with column_metadata=");
                state.out.print("[{ column_name:Test, validation_class:IntegerType, index_type:0, index_name:IdxName");
                state.out.println("}] and rows_cached=100 and comment='this is helpful comment.';");
                break;

            case CliParser.NODE_DEL_KEYSPACE:
                state.out.println("drop keyspace <keyspace>;\n");
                state.out.println("Drops the specified keyspace.\n");
                state.out.println("example:");
                state.out.println("drop keyspace foo;");
                break;

            case CliParser.NODE_DEL_COLUMN_FAMILY:
                state.out.println("drop column family <name>;\n");
                state.out.println("Drops the specified column family.\n");
                state.out.println("example:");
                state.out.println("drop column family foo;");
                break;

            case CliParser.NODE_THRIFT_GET :
                state.out.println("get <cf>['<key>'];");
                state.out.println("get <cf>['<key>']['<col>'] (as <type>)*;");
                state.out.println("get <cf>['<key>']['<super>'];");
                state.out.println("get <cf>['<key>'][<function>];");
                state.out.println("get <cf>['<key>'][<function>(<super>)][<function>(<col>)];");
                state.out.println("get <cf> where <column> = <value> [and <column> > <value> and ...] [limit <integer>];");
                state.out.println("Default LIMIT is 100. Available operations: =, >, >=, <, <=\n");
                state.out.println("get <cf>['<key>']['<super>']['<col>'] (as <type>)*;");
                state.out.print("Note: `as <type>` is optional, it dynamically converts column value to the specified type");
                state.out.println(", column value validator will be set to <type>.");
                state.out.println("Available functions: " + CliClient.Function.getFunctionNames());
                state.out.println("Available types: IntegerType, LongType, UTF8Type, ASCIIType, TimeUUIDType, LexicalUUIDType.\n");
                state.out.println("examples:");
                state.out.println("get bar[testkey];");
                state.out.println("get bar[testkey][test_column] as IntegerType;");
                state.out.println("get bar[testkey][utf8(hello)];");
                break;

            case CliParser.NODE_THRIFT_SET:
                state.out.println("set <cf>['<key>']['<col>'] = <value>;");
                state.out.println("set <cf>['<key>']['<super>']['<col>'] = <value>;");
                state.out.println("set <cf>['<key>']['<col>'] = <function>(<argument>);");
                state.out.println("set <cf>['<key>']['<super>']['<col>'] = <function>(<argument>);");
                state.out.println("set <cf>[<key>][<function>(<col>)] = <value> || <function>;");
                state.out.println("Available functions: " + CliClient.Function.getFunctionNames() + "\n");
                state.out.println("examples:");
                state.out.println("set bar['testkey']['my super']['test col']='this is a test';");
                state.out.println("set baz['testkey']['test col']='this is also a test';");
                state.out.println("set diz[testkey][testcol] = utf8('this is utf8 string.');");
                state.out.println("set bar[testkey][timeuuid()] = utf('hello world');");
                break;

            case CliParser.NODE_THRIFT_DEL:
                state.out.println("del <cf>['<key>'];");
                state.out.println("del <cf>['<key>']['<col>'];");
                state.out.println("del <cf>['<key>']['<super>']['<col>'];\n");
                state.out.println("Deletes a record, a column, or a subcolumn.\n");
                state.out.println("example:");
                state.out.println("del bar['testkey']['my super']['test col'];");
                state.out.println("del baz['testkey']['test col'];");
                state.out.println("del baz['testkey'];");
                break;

            case CliParser.NODE_THRIFT_COUNT:
                state.out.println("count <cf>['<key>'];");
                state.out.println("count <cf>['<key>']['<super>'];\n");
                state.out.println("Count the number of columns in the specified key or subcolumns in the specified");
                state.out.println("super column.\n");
                state.out.println("example:");
                state.out.println("count bar['testkey']['my super'];");
                state.out.println("count baz['testkey'];");
                break;

            case CliParser.NODE_LIST:
                state.out.println("list <cf>;");
                state.out.println("list <cf>[<startKey>:];");
                state.out.println("list <cf>[<startKey>:<endKey>];");
                state.out.println("list ... limit N;");
                state.out.println("List a range of rows in the column or supercolumn family.\n");
                state.out.println("example:");
                state.out.println("list Users[j:] limit 40;");
                break;

            case CliParser.NODE_TRUNCATE:
                state.out.println("truncate <column_family>;");
                state.out.println("Truncate specified column family.\n");
                state.out.println("example:");
                state.out.println("truncate Category;");
                break;

            case CliParser.NODE_ASSUME:
                state.out.println("assume <column_family> comparator as <type>;");
                state.out.println("assume <column_family> sub_comparator as <type>;");
                state.out.println("assume <column_family> validator as <type>;");
                state.out.println("assume <column_family> keys as <type>;\n");
                state.out.println("Assume one of the attributes (comparator, sub_comparator, validator or keys)");
                state.out.println("of the given column family to match specified type. Available types: " + CliClient.Function.getFunctionNames());
                state.out.println("example:");
                state.out.println("assume Users comparator as lexicaluuid;");
                break;

            default:
                state.out.println("?");
                break;
            }
        }
        else
        {
            state.out.println("List of all CLI commands:");
            state.out.println("?                                                          Display this message.");
            state.out.println("help;                                                          Display this help.");
            state.out.println("help <command>;                          Display detailed, command-specific help.");
            state.out.println("connect <hostname>/<port>;                             Connect to thrift service.");
            state.out.println("use <keyspace> [<username> 'password'];                     Switch to a keyspace.");
            state.out.println("describe keyspace <keyspacename>;                              Describe keyspace.");
            state.out.println("exit;                                                                   Exit CLI.");
            state.out.println("quit;                                                                   Exit CLI.");
            state.out.println("show cluster name;                                          Display cluster name.");
            state.out.println("show keyspaces;                                           Show list of keyspaces.");
            state.out.println("show api version;                                        Show server API version.");
            state.out.println("create keyspace <keyspace> [with <att1>=<value1> [and <att2>=<value2> ...]];");
            state.out.println("                Add a new keyspace with the specified attribute(s) and value(s).");
            state.out.println("update keyspace <keyspace> [with <att1>=<value1> [and <att2>=<value2> ...]];");
            state.out.println("                 Update a keyspace with the specified attribute(s) and value(s).");
            state.out.println("create column family <cf> [with <att1>=<value1> [and <att2>=<value2> ...]];");
            state.out.println("        Create a new column family with the specified attribute(s) and value(s).");
            state.out.println("update column family <cf> [with <att1>=<value1> [and <att2>=<value2> ...]];");
            state.out.println("            Update a column family with the specified attribute(s) and value(s).");
            state.out.println("drop keyspace <keyspace>;                                      Delete a keyspace.");
            state.out.println("drop column family <cf>;                                  Delete a column family.");
            state.out.println("get <cf>['<key>'];                                        Get a slice of columns.");
            state.out.println("get <cf>['<key>']['<super>'];                         Get a slice of sub columns.");
            state.out.println("get <cf> where <column> = <value> [and <column> > <value> and ...] [limit int];  ");
            state.out.println("get <cf>['<key>']['<col>'] (as <type>)*;                      Get a column value.");
            state.out.println("get <cf>['<key>']['<super>']['<col>'] (as <type>)*;       Get a sub column value.");
            state.out.println("set <cf>['<key>']['<col>'] = <value>;                               Set a column.");
            state.out.println("set <cf>['<key>']['<super>']['<col>'] = <value>;                Set a sub column.");
            state.out.println("del <cf>['<key>'];                                                 Delete record.");
            state.out.println("del <cf>['<key>']['<col>'];                                        Delete column.");
            state.out.println("del <cf>['<key>']['<super>']['<col>'];                         Delete sub column.");
            state.out.println("count <cf>['<key>'];                                     Count columns in record.");
            state.out.println("count <cf>['<key>']['<super>'];                  Count columns in a super column.");
            state.out.println("truncate <column_family>;                       Truncate specified column family.");
            state.out.println("assume <column_family> <attribute> as <type>;");
            state.out.println(" Assume one of the attributes of the given column family to match specified type.");
            state.out.println("list <cf>;                                    List all rows in the column family.");
            state.out.println("list <cf>[<startKey>:];");
            state.out.println("                        List rows in the column family beginning with <startKey>.");
            state.out.println("list <cf>[<startKey>:<endKey>];");
            state.out.println("         List rows in the column family in the range from <startKey> to <endKey>.");
            state.out.println("list ... limit N;                                    Limit the list results to N.");
        }
    }

}
