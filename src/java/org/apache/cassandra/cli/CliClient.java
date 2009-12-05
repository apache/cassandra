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

import org.apache.thrift.*;

import org.antlr.runtime.tree.*;
import org.apache.cassandra.service.*;

import java.util.*;
import java.io.UnsupportedEncodingException;

import org.apache.commons.lang.ArrayUtils;

// Cli Client Side Library
public class CliClient 
{
    private Cassandra.Client thriftClient_ = null;
    private CliSessionState css_ = null;
    private Map<String, Map<String, Map<String, String>>> keyspacesMap = new HashMap<String, Map<String,Map<String,String>>>();

    public CliClient(CliSessionState css, Cassandra.Client thriftClient)
    {
        css_ = css;
        thriftClient_ = thriftClient;
    }

    // Execute a CLI Statement 
    public void executeCLIStmt(String stmt) throws TException, NotFoundException, InvalidRequestException, UnavailableException, TimedOutException
    {
        CommonTree ast = null;

        ast = CliCompiler.compileQuery(stmt);

        try
        {
            switch (ast.getType()) {
                case CliParser.NODE_EXIT:
                    cleanupAndExit();
                    break;
                case CliParser.NODE_THRIFT_GET:
                    executeGet(ast);
                    break;
                case CliParser.NODE_HELP:
                    printCmdHelp();
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
                case CliParser.NODE_SHOW_CLUSTER_NAME:
                    executeShowProperty(ast, "cluster name");
                    break;
                case CliParser.NODE_SHOW_CONFIG_FILE:
                    executeShowProperty(ast, "config file");
                    break;
                case CliParser.NODE_SHOW_VERSION:
                    executeShowProperty(ast, "version");
                    break;
                case CliParser.NODE_SHOW_TABLES:
                    executeShowTables(ast);
                    break;
                case CliParser.NODE_DESCRIBE_TABLE:
                    executeDescribeTable(ast);
                    break;
                case CliParser.NODE_CONNECT:
                    executeConnect(ast);
                    break;
                case CliParser.NODE_NO_OP:
                    // comment lines come here; they are treated as no ops.
                    break;
                default:
                    css_.err.println("Invalid Statement (Type: " + ast.getType() + ")");
                    break;
            }
        }
        catch (UnsupportedEncodingException e)
        {
            throw new RuntimeException("Unable to encode string as UTF-8", e);
        }
    }
    
    private void printCmdHelp()
    {
       css_.out.println("List of all CLI commands:");
       css_.out.println("?                                                      Same as help.");
       css_.out.println("connect <hostname>/<port>                              Connect to Cassandra's thrift service.");
       css_.out.println("describe keyspace <keyspacename>                       Describe keyspace.");
       css_.out.println("exit                                                   Exit CLI.");
       css_.out.println("help                                                   Display this help.");
       css_.out.println("quit                                                   Exit CLI.");
       css_.out.println("show config file                                       Display contents of config file");
       css_.out.println("show cluster name                                      Display cluster name.");
       css_.out.println("show keyspaces                                         Show list of keyspaces.");
       css_.out.println("show version                                           Show server version.");
       css_.out.println("get <tbl>.<cf>['<rowKey>']                             Get a slice of columns.");            
       css_.out.println("get <tbl>.<cf>['<rowKey>']['<colKey>']                 Get a column value.");            
       css_.out.println("set <tbl>.<cf>['<rowKey>']['<colKey>'] = '<value>'     Set a column.");    
       css_.out.println("count <tbl>.<cf>['<rowKey>']                           Count columns in row.");
       css_.out.println("del <tbl>.<cf>['<rowKey>']                             Delete row.");
       css_.out.println("del <tbl>.<cf>['<rowKey>']['<colKey>']                 Delete column.");
    }

    private void cleanupAndExit()
    {
        CliMain.disconnect();
        System.exit(0);
    }
    
    Map<String, Map<String, String>> getCFMetaData(String keyspace) throws NotFoundException, TException
    {
        // Lazily lookup column family meta-data.
        if (!(keyspacesMap.containsKey(keyspace)))
            keyspacesMap.put(keyspace, thriftClient_.describe_keyspace(keyspace));
        return keyspacesMap.get(keyspace);
    }
    
    private void executeCount(CommonTree ast) throws TException, InvalidRequestException, UnavailableException, TimedOutException
    {
       if (!CliMain.isConnected())
           return;

       int childCount = ast.getChildCount();
       assert(childCount == 1);

       CommonTree columnFamilySpec = (CommonTree)ast.getChild(0);
       assert(columnFamilySpec.getType() == CliParser.NODE_COLUMN_ACCESS);

       String tableName = CliCompiler.getTableName(columnFamilySpec);
       String key = CliCompiler.getKey(columnFamilySpec);
       String columnFamily = CliCompiler.getColumnFamily(columnFamilySpec);
       int columnSpecCnt = CliCompiler.numColumnSpecifiers(columnFamilySpec);
       
       if (columnSpecCnt == 0)
       {
           ColumnParent cp = new ColumnParent(columnFamily, null);
           int count = thriftClient_.get_count(tableName, key, cp, ConsistencyLevel.ONE);
           css_.out.printf("%d columns\n", count);
       }
       else
       {
           //TODO could support sub columns?
           css_.err.println("Only column count for a top level row key supported");
           return;
       }
    }
    
    private void executeDelete(CommonTree ast) throws TException, InvalidRequestException, UnavailableException, TimedOutException, UnsupportedEncodingException
    {
        if (!CliMain.isConnected())
            return;

        int childCount = ast.getChildCount();
        assert(childCount == 1);

        CommonTree columnFamilySpec = (CommonTree)ast.getChild(0);
        assert(columnFamilySpec.getType() == CliParser.NODE_COLUMN_ACCESS);

        String tableName = CliCompiler.getTableName(columnFamilySpec);
        String key = CliCompiler.getKey(columnFamilySpec);
        String columnFamily = CliCompiler.getColumnFamily(columnFamilySpec);
        int columnSpecCnt = CliCompiler.numColumnSpecifiers(columnFamilySpec);

        // assume simple columnFamily for now
        String columnName = null;
        final byte[] name;
        if (columnSpecCnt == 0)
        {
            // table.cf['key']
            name = null;
        }
        else
        {
            assert columnSpecCnt == 1;
            // table.cf['key']['column']
            columnName = CliCompiler.getColumn(columnFamilySpec, 0);
            name = columnName.getBytes("UTF-8");
        }
        thriftClient_.remove(tableName, key, new ColumnPath(columnFamily, null, name), System.currentTimeMillis(), ConsistencyLevel.ONE);
        css_.out.println(String.format("%s removed.", (columnSpecCnt == 0) ? "row" : "column"));
    }  
    
    private void doSlice(String keyspace, String key, String columnFamily, byte[] superColumnName)
    throws InvalidRequestException, UnavailableException, TimedOutException, TException, UnsupportedEncodingException
    {
        SliceRange range = new SliceRange(ArrayUtils.EMPTY_BYTE_ARRAY, ArrayUtils.EMPTY_BYTE_ARRAY, true, 1000000);
        List<ColumnOrSuperColumn> columns = thriftClient_.get_slice(keyspace, key, new ColumnParent(columnFamily, superColumnName),
                                                                    new SlicePredicate(null, range), ConsistencyLevel.ONE);
        int size = columns.size();
        
        // Print out super columns or columns.
        for (ColumnOrSuperColumn cosc : columns)
        {
            if (cosc.isSetSuper_column())
            {
                SuperColumn superColumn = cosc.super_column;
                css_.out.printf("=> (super_column=%s,", new String(superColumn.name, "UTF-8"));
                for (Column col : superColumn.getColumns())
                    css_.out.printf("\n     (column=%s, value=%s, timestamp=%d)", new String(col.name, "UTF-8"),
                                    new String(col.value, "UTF-8"), col.timestamp);
                
                css_.out.println(")"); 
            }
            else
            {
                Column column = cosc.column;
                css_.out.printf("=> (column=%s, value=%s; timestamp=%d)\n", new String(column.name, "UTF-8"),
                                new String(column.value, "UTF-8"), column.timestamp);
            }
        }
        
        css_.out.println("Returned " + size + " results.");
    }
 
    // Execute GET statement
    private void executeGet(CommonTree ast) throws TException, NotFoundException, InvalidRequestException, UnavailableException, TimedOutException, UnsupportedEncodingException
    {
        if (!CliMain.isConnected())
            return;

        // This will never happen unless the grammar is broken
        assert (ast.getChildCount() == 1) : "serious parsing error (this is a bug).";

        CommonTree columnFamilySpec = (CommonTree)ast.getChild(0);
        assert(columnFamilySpec.getType() == CliParser.NODE_COLUMN_ACCESS);

        String tableName = CliCompiler.getTableName(columnFamilySpec);
        String key = CliCompiler.getKey(columnFamilySpec);
        String columnFamily = CliCompiler.getColumnFamily(columnFamilySpec);
        int columnSpecCnt = CliCompiler.numColumnSpecifiers(columnFamilySpec); 
        
        if (!(getCFMetaData(tableName).containsKey(columnFamily)))
        {
            css_.out.println("No such column family: " + columnFamily);
            return;
        }
        
        boolean isSuper = getCFMetaData(tableName).get(columnFamily).get("Type").equals("Super") ? true : false;
        
        byte[] superColumnName = null;
        byte[] columnName = null;
        
        // table.cf['key'] -- row slice
        if (columnSpecCnt == 0)
        {
            doSlice(tableName, key, columnFamily, superColumnName);
            return;
        }
        
        // table.cf['key']['column'] -- slice of a super, or get of a standard
        if (columnSpecCnt == 1)
        {
            if (isSuper)
            {
                superColumnName = CliCompiler.getColumn(columnFamilySpec, 0).getBytes("UTF-8");
                doSlice(tableName, key, columnFamily, superColumnName);
                return;
            }
            else
            {
                columnName = CliCompiler.getColumn(columnFamilySpec, 0).getBytes("UTF-8");
            }
        }
        // table.cf['key']['column']['column'] -- get of a sub-column
        else if (columnSpecCnt == 2)
        {
            superColumnName = CliCompiler.getColumn(columnFamilySpec, 0).getBytes("UTF-8");
            columnName = CliCompiler.getColumn(columnFamilySpec, 1).getBytes("UTF-8");
        }
        // The parser groks an arbitrary number of these so it is possible to get here.
        else
        {
            css_.out.println("Invalid row, super column, or column specification.");
            return;
        }
        
        // Perform a get(), print out the results.
        ColumnPath path = new ColumnPath(columnFamily, superColumnName, columnName);
        Column column = thriftClient_.get(tableName, key, path, ConsistencyLevel.ONE).column;
        css_.out.printf("=> (column=%s, value=%s, timestamp=%d)\n", new String(column.name, "UTF-8"),
                        new String(column.value, "UTF-8"), column.timestamp);
    }

    // Execute SET statement
    private void executeSet(CommonTree ast) throws TException, InvalidRequestException, UnavailableException, TimedOutException, UnsupportedEncodingException
    { 
        if (!CliMain.isConnected())
            return;

        assert (ast.getChildCount() == 2) : "serious parsing error (this is a bug).";

        CommonTree columnFamilySpec = (CommonTree)ast.getChild(0);
        assert(columnFamilySpec.getType() == CliParser.NODE_COLUMN_ACCESS);

        String tableName = CliCompiler.getTableName(columnFamilySpec);
        String key = CliCompiler.getKey(columnFamilySpec);
        String columnFamily = CliCompiler.getColumnFamily(columnFamilySpec);
        int columnSpecCnt = CliCompiler.numColumnSpecifiers(columnFamilySpec);
        String value = CliUtils.unescapeSQLString(ast.getChild(1).getText());

        byte[] superColumnName = null;
        byte[] columnName = null;
 
        // table.cf['key']['column'] = 'value'
        if (columnSpecCnt == 1)
        {
            // get the column name
            columnName = CliCompiler.getColumn(columnFamilySpec, 0).getBytes("UTF-8");
        }
        // table.cf['key']['super_column']['column'] = 'value'
        else
        {
            assert (columnSpecCnt == 2) : "serious parsing error (this is a bug).";
            
            // get the super column and column names
            superColumnName = CliCompiler.getColumn(columnFamilySpec, 0).getBytes("UTF-8");
            columnName = CliCompiler.getColumn(columnFamilySpec, 1).getBytes("UTF-8");
        }
        
        // do the insert
        thriftClient_.insert(tableName, key, new ColumnPath(columnFamily, superColumnName, columnName),
                             value.getBytes(), System.currentTimeMillis(), ConsistencyLevel.ONE);
        
        css_.out.println("Value inserted.");
    }

    private void executeShowProperty(CommonTree ast, String propertyName) throws TException
    {
        if (!CliMain.isConnected())
            return;

        String propertyValue = thriftClient_.get_string_property(propertyName);
        css_.out.println(propertyValue);
        return;
    }

    // process "show tables" statement
    private void executeShowTables(CommonTree ast) throws TException
    {
        if (!CliMain.isConnected())
            return;
        
        List<String> tables = thriftClient_.get_string_list_property("keyspaces");
        for (String table : tables)
        {
            css_.out.println(table);
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

        // Describe and display
        Map<String, Map<String, String>> columnFamiliesMap;
        try {
            columnFamiliesMap = thriftClient_.describe_keyspace(tableName);
            for (String columnFamilyName: columnFamiliesMap.keySet()) {
                Map<String, String> columnMap = columnFamiliesMap.get(columnFamilyName);
                String desc = columnMap.get("Desc");
                String columnFamilyType = columnMap.get("Type");
                String sort = columnMap.get("CompareWith");
                String flushperiod = columnMap.get("FlushPeriodInMinutes");
                css_.out.println(desc);
                css_.out.println("Column Family Type: " + columnFamilyType);
                css_.out.println("Column Sorted By: " + sort);
                css_.out.println("flush period: " + flushperiod + " minutes");
                css_.out.println("------");
            }
        } catch (NotFoundException e) {
            css_.out.println("Keyspace " + tableName + " could not be found.");
        }
        
        return;
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
}
