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

import java.io.UnsupportedEncodingException;
import java.math.BigInteger;
import java.util.*;

import org.apache.commons.lang.ArrayUtils;

import org.antlr.runtime.tree.CommonTree;
import org.antlr.runtime.tree.Tree;
import org.apache.cassandra.auth.SimpleAuthenticator;
import org.apache.cassandra.db.marshal.*;
import org.apache.cassandra.thrift.*;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.thrift.TException;

// Cli Client Side Library
public class CliClient 
{

    /*
     * the <i>add column family</i> command requires a list of arguments, 
     *  this enum defines which arguments are valid.
     */
    private enum AddColumnFamilyArgument {
        COLUMN_TYPE,
        COMPARATOR,
        SUBCOMPARATOR,
        COMMENT,
        ROWS_CACHED,
        PRELOAD_ROW_CACHE,
        KEY_CACHE_SIZE,
        READ_REPAIR_CHANCE,
        GC_GRACE_SECONDS
    }

    /*
     * the <i>add keyspace</i> command requires a list of arguments,
     *  this enum defines which arguments are valid
     */
    private enum AddKeyspaceArgument {
        REPLICATION_FACTOR,
        PLACEMENT_STRATEGY
    }

    private Cassandra.Client thriftClient_ = null;
    private CliSessionState css_ = null;
    private String keySpace = null;
    private String username = null;
    private Map<String, KsDef> keyspacesMap = new HashMap<String, KsDef>();

    public CliClient(CliSessionState cliSessionState, Cassandra.Client thriftClient)
    {
        css_ = cliSessionState;
        thriftClient_ = thriftClient;
    }

    // Execute a CLI Statement 
    public void executeCLIStmt(String stmt) throws TException, NotFoundException, InvalidRequestException, UnavailableException, TimedOutException, IllegalAccessException, ClassNotFoundException, InstantiationException, NoSuchFieldException
    {
        CommonTree ast = CliCompiler.compileQuery(stmt);

        try
        {
            switch (ast.getType())
            {
                case CliParser.NODE_EXIT:
                    cleanupAndExit();
                    break;
                case CliParser.NODE_THRIFT_GET:
                    executeGet(ast);
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
                case CliParser.NODE_ADD_COLUMN_FAMILY:
                    executeAddColumnFamily(ast);
                    break;
                case CliParser.NODE_ADD_KEYSPACE:
                    executeAddKeyspace(ast);
                    break;
                case CliParser.NODE_DEL_COLUMN_FAMILY:
                    executeDelColumnFamily(ast);
                    break;
                case CliParser.NODE_DEL_KEYSPACE:
                    executeDelKeyspace(ast);
                    break;
                case CliParser.NODE_RENAME_COLUMN_FAMILY:
                    executeRenameColumnFamily(ast);
                    break;
                case CliParser.NODE_RENAME_KEYSPACE:
                    executeRenameKeyspace(ast);
                    break;
                case CliParser.NODE_SHOW_CLUSTER_NAME:
                    executeShowClusterName();
                    break;
                case CliParser.NODE_SHOW_VERSION:
                    executeShowVersion();
                    break;
                case CliParser.NODE_SHOW_TABLES:
                    executeShowTables(ast);
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
        catch (UnsupportedEncodingException e)
        {
            throw new RuntimeException("Unable to encode string as UTF-8", e);
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
                css_.out.println("                        and org.apache.cassandra.locator.OldNetworkTopologyStrategy\n");
                css_.out.println("example:");
                css_.out.println("create keyspace foo with replication_factor = 3 and ");
                css_.out.println("        placement_strategy = 'org.apache.cassandra.locator.SimpleStrategy'");
                break;
                
            case CliParser.NODE_ADD_COLUMN_FAMILY:
                css_.out.println("create column family Bar");
                css_.out.println("create column family Bar with <att1>=<value1>");
                css_.out.println("create column family Bar with <att1>=<value1> and <att2>=<value2>...\n");
                css_.out.println("Create a new column family with the specified values for the given set of");
                css_.out.println("attributes. Note that you must be using a keyspace.\n");
                css_.out.println("valid attributes are:");
                css_.out.println("    - column_type: One of Super or Standard");
                css_.out.println("    - comparator: The class used as a comparator when sorting column names.");
                css_.out.println("                  Valid options include: AsciiType, BytesType, LexicalUUIDType,");
                css_.out.println("                  LongType, TimeUUIDType, and UTF8Type");
                css_.out.println("    - subcomparator: Name of comparator used for subcolumns (when");
                css_.out.println("                     column_type=Super only). Valid options are identical to");
                css_.out.println("                     comparator above.");
                css_.out.println("    - comment: Human-readable column family description. Any string is valid.");
                css_.out.println("    - rows_cached: Number of rows to cache");
                css_.out.println("    - preload_row_cache: Set to true to automatically load the row cache");
                css_.out.println("    - key_cache_size: Number of keys to cache");
                css_.out.println("    - read_repair_chance: Valid values for this attribute are any number");
                css_.out.println("                          between 0.0 and 1.0\n");
                css_.out.println("example:\n");
                css_.out.println("create column family bar with column_type = 'Super' and comparator = 'AsciiType'");
                css_.out.println("      and rows_cached = 10000");
                css_.out.println("create column family baz with comparator = 'LongType' and rows_cached = 10000");
                break;
                
            case CliParser.NODE_RENAME_KEYSPACE:
                css_.out.println("rename keyspace <old_name> <new_name>\n");
                css_.out.println("Renames the specified keyspace with the given new name.\n");
                css_.out.println("example:");
                css_.out.println("rename keyspace foo bar");
                break;
                
            case CliParser.NODE_RENAME_COLUMN_FAMILY:
                css_.out.println("rename column family <name> <new_name>\n");
                css_.out.println("Renames the specified column family with the given new name.\n");
                css_.out.println("example:");
                css_.out.println("rename column family foo bar");
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
                css_.out.println("get <cf>['<key>']['<col>'] ");
                css_.out.println("get <cf>['<key>']['<super>'] ");
                css_.out.println("get <cf>['<key>']['<super>']['<col>']\n");
                css_.out.println("example:");
                css_.out.println("get bar['testkey']");
                break;
                
            case CliParser.NODE_THRIFT_SET:
                css_.out.println("set <cf>['<key>']['<col>'] = '<value>' ");
                css_.out.println("set <cf>['<key>']['<super>']['<col>'] = '<value>'\n");
                css_.out.println("example:");
                css_.out.println("set bar['testkey']['my super']['test col']='this is a test'");
                css_.out.println("set baz['testkey']['test col']='this is also a test'");
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
            css_.out.println("                   Add a new keyspace with the specified attribute and value(s).");
            css_.out.println("create column family <cf> [with <att1>=<value1> [and <att2>=<value2> ...]]");
            css_.out.println("           Create a new column family with the specified attribute and value(s).");
            css_.out.println("drop keyspace <keyspace>                                      Delete a keyspace.");
            css_.out.println("drop column family <cf>                                  Delete a column family.");
            css_.out.println("rename keyspace <keyspace> <keyspace_new_name>                Rename a keyspace.");
            css_.out.println("rename column family <cf> <new_name>                     Rename a column family.");
            css_.out.println("get <cf>['<key>']                                        Get a slice of columns.");
            css_.out.println("get <cf>['<key>']['<super>']                         Get a slice of sub columns.");
            css_.out.println("get <cf>['<key>']['<col>']                                   Get a column value.");
            css_.out.println("get <cf>['<key>']['<super>']['<col>']                    Get a sub column value.");
            css_.out.println("set <cf>['<key>']['<col>'] = '<value>'                             Set a column.");
            css_.out.println("set <cf>['<key>']['<super>']['<col>'] = '<value>'              Set a sub column.");
            css_.out.println("del <cf>['<key>']                                                 Delete record.");
            css_.out.println("del <cf>['<key>']['<col>']                                        Delete column.");
            css_.out.println("del <cf>['<key>']['<super>']['<col>']                         Delete sub column.");
            css_.out.println("count <cf>['<key>']                                     Count columns in record.");
            css_.out.println("count <cf>['<key>']['<super>']                  Count columns in a super column.");
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
    
    private void executeCount(CommonTree ast) throws TException, InvalidRequestException, UnavailableException, TimedOutException, UnsupportedEncodingException
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
           colParent = new ColumnParent(columnFamily).setSuper_column(null);
       }
       else
       {
           assert (columnSpecCnt == 1);
           colParent = new ColumnParent(columnFamily).setSuper_column(CliCompiler.getColumn(columnFamilySpec, 0).getBytes("UTF-8"));
       }

       SliceRange range = new SliceRange(ArrayUtils.EMPTY_BYTE_ARRAY, ArrayUtils.EMPTY_BYTE_ARRAY, false, Integer.MAX_VALUE);
       SlicePredicate predicate = new SlicePredicate().setColumn_names(null).setSlice_range(range);
       
       int count = thriftClient_.get_count(key.getBytes(), colParent, predicate, ConsistencyLevel.ONE);
       css_.out.printf("%d columns\n", count);
    }
    
    private void executeDelete(CommonTree ast) throws TException, InvalidRequestException, UnavailableException, TimedOutException, UnsupportedEncodingException
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
                superColumnName = CliCompiler.getColumn(columnFamilySpec, 0).getBytes("UTF-8");
            else
                columnName = CliCompiler.getColumn(columnFamilySpec, 0).getBytes("UTF-8");
        }
        else if (columnSpecCnt == 2)
        {
            // table.cf['key']['column']['column']
            superColumnName = CliCompiler.getColumn(columnFamilySpec, 0).getBytes("UTF-8");
            columnName = CliCompiler.getColumn(columnFamilySpec, 1).getBytes("UTF-8");
        }

        thriftClient_.remove(key.getBytes(), new ColumnPath(columnFamily).setSuper_column(superColumnName).setColumn(columnName),
                             FBUtilities.timestampMicros(), ConsistencyLevel.ONE);
        css_.out.println(String.format("%s removed.", (columnSpecCnt == 0) ? "row" : "column"));
    }

    private void doSlice(String keyspace, String key, String columnFamily, byte[] superColumnName)
            throws InvalidRequestException, UnavailableException, TimedOutException, TException, UnsupportedEncodingException, IllegalAccessException, NotFoundException, InstantiationException, NoSuchFieldException
    {
        SliceRange range = new SliceRange(ArrayUtils.EMPTY_BYTE_ARRAY, ArrayUtils.EMPTY_BYTE_ARRAY, true, 1000000);
        List<ColumnOrSuperColumn> columns = thriftClient_.get_slice(key.getBytes(),
                                                                    new ColumnParent(columnFamily).setSuper_column(superColumnName),
                                                                    new SlicePredicate().setColumn_names(null).setSlice_range(range), ConsistencyLevel.ONE);
        int size = columns.size();
        
        // Print out super columns or columns.
        for (ColumnOrSuperColumn cosc : columns)
        {
            if (cosc.isSetSuper_column())
            {
                SuperColumn superColumn = cosc.super_column;

                css_.out.printf("=> (super_column=%s,", formatSuperColumnName(keyspace, columnFamily, superColumn));
                for (Column col : superColumn.getColumns())
                    css_.out.printf("\n     (column=%s, value=%s, timestamp=%d)", formatSubcolumnName(keyspace, columnFamily, col),
                                    new String(col.value, "UTF-8"), col.timestamp);
                
                css_.out.println(")");
            }
            else
            {
                Column column = cosc.column;
                css_.out.printf("=> (column=%s, value=%s, timestamp=%d)\n", formatColumnName(keyspace, columnFamily, column),
                                new String(column.value, "UTF-8"), column.timestamp);
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

    private AbstractType getFormatTypeForColumn(String compareWith) throws IllegalAccessException, InstantiationException, NoSuchFieldException
    {
        AbstractType type;
        try {
            // Get the singleton instance of the AbstractType subclass
            Class c = Class.forName(compareWith);
            type = (AbstractType) c.getField("instance").get(c);
        } catch (ClassNotFoundException e) {
            type = BytesType.instance;
        }
        return type;
    }

    // Execute GET statement
    private void executeGet(CommonTree ast) throws TException, NotFoundException, InvalidRequestException, UnavailableException, TimedOutException, UnsupportedEncodingException, IllegalAccessException, InstantiationException, ClassNotFoundException, NoSuchFieldException
    {
        if (!CliMain.isConnected() || !hasKeySpace())
            return;

        // This will never happen unless the grammar is broken
        assert ast.getChildCount() == 1 : "serious parsing error (this is a bug).";

        CommonTree columnFamilySpec = (CommonTree)ast.getChild(0);
        assert(columnFamilySpec.getType() == CliParser.NODE_COLUMN_ACCESS);

        String key = CliCompiler.getKey(columnFamilySpec);
        String columnFamily = CliCompiler.getColumnFamily(columnFamilySpec);
        int columnSpecCnt = CliCompiler.numColumnSpecifiers(columnFamilySpec);

        List<String> cfnames = new ArrayList<String>();
        for (CfDef cfd : keyspacesMap.get(keySpace).cf_defs)
        {
            cfnames.add(cfd.name);
        }
 
        int idx = cfnames.indexOf(columnFamily);
        if (idx == -1)
        {
            css_.out.println("No such column family: " + columnFamily);
            return;
        }

        boolean isSuper = keyspacesMap.get(keySpace).cf_defs.get(idx).column_type.equals("Super");
        
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
                superColumnName = CliCompiler.getColumn(columnFamilySpec, 0).getBytes("UTF-8");
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
            superColumnName = CliCompiler.getColumn(columnFamilySpec, 0).getBytes("UTF-8");
            columnName = CliCompiler.getColumn(columnFamilySpec, 1);
        }
        // The parser groks an arbitrary number of these so it is possible to get here.
        else
        {
            css_.out.println("Invalid row, super column, or column specification.");
            return;
        }

        // Perform a get()
        ColumnPath path = new ColumnPath(columnFamily).setSuper_column(superColumnName).setColumn(columnNameAsByteArray(columnName, columnFamily));
        Column column = thriftClient_.get(key.getBytes(), path, ConsistencyLevel.ONE).column;
        // print results
        css_.out.printf("=> (column=%s, value=%s, timestamp=%d)\n",
                        formatColumnName(keySpace, columnFamily, column), new String(column.value, "UTF-8"), column.timestamp);
    }

    // Execute SET statement
    private void executeSet(CommonTree ast)
    throws TException, InvalidRequestException, UnavailableException, TimedOutException, UnsupportedEncodingException, NoSuchFieldException, InstantiationException, IllegalAccessException
    {
        if (!CliMain.isConnected() || !hasKeySpace())
            return;

        assert (ast.getChildCount() == 2) : "serious parsing error (this is a bug).";

        CommonTree columnFamilySpec = (CommonTree)ast.getChild(0);
        assert(columnFamilySpec.getType() == CliParser.NODE_COLUMN_ACCESS);

        String key = CliCompiler.getKey(columnFamilySpec);
        String columnFamily = CliCompiler.getColumnFamily(columnFamilySpec);
        int columnSpecCnt = CliCompiler.numColumnSpecifiers(columnFamilySpec);
        String value = CliUtils.unescapeSQLString(ast.getChild(1).getText());

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
            superColumnName = CliCompiler.getColumn(columnFamilySpec, 0).getBytes("UTF-8");
            columnName = CliCompiler.getColumn(columnFamilySpec, 1);
        }

        // do the insert
        thriftClient_.insert(key.getBytes(), new ColumnParent(columnFamily).setSuper_column(superColumnName),
                             new Column(columnNameAsByteArray(columnName, columnFamily), value.getBytes(), FBUtilities.timestampMicros()), ConsistencyLevel.ONE);
        
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
     * @throws TException
     * @throws InvalidRequestException 
     * @throws NotFoundException 
     */
    private void executeAddKeyspace(CommonTree ast) throws TException, InvalidRequestException, NotFoundException
    {

        if (!CliMain.isConnected())
        {
            return;
        }
        CommonTree newKSSpec = (CommonTree)ast.getChild(0);

        //parser send the keyspace, plus a series of key value pairs, ie should always be an odd number...
        assert(newKSSpec.getChildCount() > 0);
        assert(newKSSpec.getChildCount() % 2 == 1);

        //defaults
        String replicaPlacementStrategy = "org.apache.cassandra.locator.SimpleStrategy";
        int replicationFactor = 1;

        /*
         * first value is the keyspace name, after that it is all key=value
         */
        String keyspaceName = newKSSpec.getChild(0).getText();
        int argumentLength = newKSSpec.getChildCount();

        for(int i = 1; i < argumentLength; i = i + 2)
        {
            AddKeyspaceArgument mArgument = AddKeyspaceArgument.valueOf(newKSSpec.getChild(i).getText().toUpperCase());
            String mValue = newKSSpec.getChild(i + 1).getText();

            switch(mArgument)
            {
            case PLACEMENT_STRATEGY:
                replicaPlacementStrategy = CliUtils.unescapeSQLString(mValue);
                break;
            case REPLICATION_FACTOR:
                replicationFactor = Integer.parseInt( mValue);
                break;
            default:
                //must match one of the above or we'd throw an exception at the valueOf statement above.
                assert(false);
            }
        }
        List<CfDef> mList = new LinkedList<CfDef>();
        KsDef ks_def = new KsDef(keyspaceName, replicaPlacementStrategy, replicationFactor, mList);
        css_.out.println(thriftClient_.system_add_keyspace(ks_def));

        keyspacesMap.put(keyspaceName, thriftClient_.describe_keyspace(keyspaceName));
    }


    /**
     * Add a column family
     * @throws TException
     * @throws InvalidRequestException 
     * @throws NotFoundException 
     */
    private void executeAddColumnFamily(CommonTree ast) throws TException, InvalidRequestException, NotFoundException
    {
        if (!CliMain.isConnected() || !hasKeySpace())
        {
            return;
        }
        CommonTree newCFTree = (CommonTree)ast.getChild(0);
        //parser send the keyspace, plus a series of key value pairs, ie should always be an odd number...
        assert(newCFTree.getChildCount() > 0);
        assert(newCFTree.getChildCount() % 2 == 1);


        /*
         * first value is the keyspace name, after that it is all key=value
         */
        String columnName = newCFTree.getChild(0).getText();
        int argumentLength = newCFTree.getChildCount();
        CfDef cfDef = new CfDef(keySpace, columnName);        

        for(int i = 1; i < argumentLength; i = i + 2)
        {
            AddColumnFamilyArgument mArgument = AddColumnFamilyArgument.valueOf(newCFTree.getChild(i).getText().toUpperCase());
            String mValue = newCFTree.getChild(i + 1).getText();


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

            case PRELOAD_ROW_CACHE:
                cfDef.setPreload_row_cache(Boolean.parseBoolean(CliUtils.unescapeSQLString(mValue)));
                break;

            case KEY_CACHE_SIZE:
                cfDef.setKey_cache_size(Double.parseDouble(mValue));
                break;

            case READ_REPAIR_CHANCE:
                cfDef.setRead_repair_chance(Double.parseDouble(mValue));
                break;

            case GC_GRACE_SECONDS:
                cfDef.setGc_grace_seconds(Integer.parseInt(mValue));
                break;

            default:
                //must match one of the above or we'd throw an exception at the valueOf statement above.
                assert(false);

            }
        }
        css_.out.println(thriftClient_.system_add_column_family(cfDef));
        keyspacesMap.put(keySpace, thriftClient_.describe_keyspace(keySpace));
    }

    /**
     * Delete a keyspace
     * @throws TException
     * @throws InvalidRequestException 
     * @throws NotFoundException 
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
     * @throws TException
     * @throws InvalidRequestException 
     * @throws NotFoundException 
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

    /**
     * Add a column family
     * @throws TException
     * @throws InvalidRequestException 
     * @throws NotFoundException 
     */
    private void executeRenameKeyspace(CommonTree ast) throws TException, InvalidRequestException, NotFoundException
    {
        if (!CliMain.isConnected())
        {
            return;
        }
        String keyspaceName = ast.getChild(0).getText();
        String keyspaceNewName = ast.getChild(1).getText();

        css_.out.println(thriftClient_.system_rename_keyspace(keyspaceName, keyspaceNewName));
    }

    /**
     * Add a column family
     * @throws TException
     * @throws InvalidRequestException 
     * @throws NotFoundException 
     */
    private void executeRenameColumnFamily(CommonTree ast) throws TException, InvalidRequestException, NotFoundException
    {
        if (!CliMain.isConnected() || !hasKeySpace())
        {
            return;
        }
        String columnName = ast.getChild(0).getText();
        String columnNewName = ast.getChild(1).getText();

        css_.out.println(thriftClient_.system_rename_column_family(columnName, columnNewName));
    }

    private void executeShowVersion() throws TException
    {
        if (!CliMain.isConnected())
            return;
        css_.out.println(thriftClient_.describe_version());
    }

    // process "show tables" statement
    private void executeShowTables(CommonTree ast) throws TException, InvalidRequestException
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
            return;
        } 
        catch (AuthorizationException e) 
        {
            css_.err.println("You are not authorized to use keyspace: " + tableName);
            return;
        }
        catch (InvalidRequestException e)
        {
            css_.err.println(tableName + " does not exist.");
            return;
        }
        catch (NotFoundException e)
        {
            css_.err.println(tableName + " does not exist.");
            return;
        } 
        catch (TException e) 
        {
            if (css_.debug)
                e.printStackTrace();
            
            css_.err.println("Login failure. Did you specify 'keyspace', 'username' and 'password'?");
            return;
        }
    }

    private void describeTableInternal(String tableName, KsDef metadata) throws TException {
        // Describe and display
        css_.out.println("Keyspace: " + tableName);
        try
        {
            KsDef ks_def;
            if (metadata != null) {
                ks_def = metadata;
            }
            else {
                ks_def = thriftClient_.describe_keyspace(tableName);
            }
            css_.out.println("  Replication Factor: " + ks_def.replication_factor);
            css_.out.println("  Column Families:");

            for (CfDef cf_def : ks_def.cf_defs)
            {
                /**
                String desc = columnMap.get("Desc");
                String columnFamilyType = columnMap.get("Type");
                String sort = columnMap.get("CompareWith");
                String flushperiod = columnMap.get("FlushPeriodInMinutes");
                css_.out.println(desc);
                 */
                //css_.out.println("description");
                css_.out.println("    Column Family Name: " + cf_def.name + " {");
                css_.out.println("      Column Family Type: " + cf_def.column_type);
                css_.out.println("      Column Sorted By: " + cf_def.comparator_type);
                //css_.out.println("      flush period: " + flushperiod + " minutes");
                css_.out.println("    }");
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

    private CfDef getCfDef(String ksname, String cfname)
    {
        List<String> cfnames = new ArrayList<String>();
        KsDef ksd = keyspacesMap.get(ksname);
        for (CfDef cfd : ksd.cf_defs) {
            cfnames.add(cfd.name);
        }
        int idx = cfnames.indexOf(cfname);
        return ksd.cf_defs.get(idx);
    }

    private byte[] columnNameAsByteArray(final String column, final String columnFamily) throws NoSuchFieldException, InstantiationException, IllegalAccessException, UnsupportedEncodingException
    {
        List<String> cfnames = new ArrayList<String>();
        for (CfDef cfd : keyspacesMap.get(keySpace).cf_defs)
        {
            cfnames.add(cfd.name);
        }

        int idx = cfnames.indexOf(columnFamily);
        if (idx == -1)
        {
            throw new NoSuchFieldException("No such column family: " + columnFamily);
        }

        final String comparatorClass  = keyspacesMap.get(keySpace).cf_defs.get(idx).comparator_type;
        final AbstractType comparator = getFormatTypeForColumn(comparatorClass);

        if (comparator instanceof LongType)
        {
            return FBUtilities.toByteArray(Long.valueOf(column));
        }
        else if (comparator instanceof IntegerType)
        {
            return new BigInteger(column).toByteArray();
        }
        else if (comparator instanceof AsciiType)
        {
            return column.getBytes("US-ASCII");
        }
        else
        {
            return column.getBytes("UTF-8");
        }   
    }
}
