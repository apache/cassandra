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

    public CliClient(CliSessionState css, Cassandra.Client thriftClient)
    {
        css_ = css;
        thriftClient_ = thriftClient;
    }

    // Execute a CLI Statement 
    public void executeCLIStmt(String stmt) throws TException, NotFoundException, InvalidRequestException, UnavailableException
    {
        CommonTree ast = null;

        ast = CliCompiler.compileQuery(stmt);

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
    }

    private void cleanupAndExit()
    {
        CliMain.disconnect();
        System.exit(0);
    }

    // Execute GET statement
    private void executeGet(CommonTree ast) throws TException, NotFoundException, InvalidRequestException
    {
        if (!CliMain.isConnected())
            return;

        int childCount = ast.getChildCount();
        assert(childCount == 1);

        CommonTree columnFamilySpec = (CommonTree)ast.getChild(0);
        assert(columnFamilySpec.getType() == CliParser.NODE_COLUMN_ACCESS);

        String tableName     = CliCompiler.getTableName(columnFamilySpec);
        String key           = CliCompiler.getKey(columnFamilySpec);
        String columnFamily  = CliCompiler.getColumnFamily(columnFamilySpec);
        int    columnSpecCnt = CliCompiler.numColumnSpecifiers(columnFamilySpec);

        // assume simple columnFamily for now
        if (columnSpecCnt == 0)
        {
            // table.cf['key']
            SliceRange range = new SliceRange(ArrayUtils.EMPTY_BYTE_ARRAY, ArrayUtils.EMPTY_BYTE_ARRAY, true, 1000000);
      	    List<ColumnOrSuperColumn> columns = thriftClient_.get_slice(tableName, key, new ColumnParent(columnFamily, null), new SlicePredicate(null, range), ConsistencyLevel.ONE);
            int size = columns.size();
            for (ColumnOrSuperColumn cosc : columns)
            {
                Column column = cosc.column;
                try
                {
                    css_.out.printf("  (column=%s, value=%s; timestamp=%d)\n",
                                    new String(column.name, "UTF-8"),
                                    new String(column.value, "UTF-8"),
                                    column.timestamp);
                }
                catch (UnsupportedEncodingException e)
                {
                    throw new RuntimeException(e);
                }
            }
            css_.out.println("Returned " + size + " rows.");
        }
        else
        {
            assert columnSpecCnt == 1;
            // table.cf['key']['column']
            String columnName = CliCompiler.getColumn(columnFamilySpec, 0);
            ColumnOrSuperColumn cosc;
            try
            {
                cosc = thriftClient_.get(tableName, key, new ColumnPath(columnFamily, null, columnName.getBytes("UTF-8")), ConsistencyLevel.ONE);
                Column column = cosc.column;
                css_.out.printf("==> (name=%s, value=%s; timestamp=%d)\n",
                        new String(column.name, "UTF-8"),
                        new String(column.value, "UTF-8"),
                        column.timestamp);
            }
            catch (UnsupportedEncodingException e)
            {
                throw new RuntimeException(e);
            }
        }
    }

    // Execute SET statement
    private void executeSet(CommonTree ast) throws TException, InvalidRequestException, UnavailableException
    {
        if (!CliMain.isConnected())
            return;

        int childCount = ast.getChildCount();
        assert(childCount == 2);

        CommonTree columnFamilySpec = (CommonTree)ast.getChild(0);
        assert(columnFamilySpec.getType() == CliParser.NODE_COLUMN_ACCESS);

        String tableName     = CliCompiler.getTableName(columnFamilySpec);
        String key           = CliCompiler.getKey(columnFamilySpec);
        String columnFamily  = CliCompiler.getColumnFamily(columnFamilySpec);
        int    columnSpecCnt = CliCompiler.numColumnSpecifiers(columnFamilySpec);
        String value         = CliUtils.unescapeSQLString(ast.getChild(1).getText());

        // assume simple columnFamily for now
        if (columnSpecCnt == 1)
        {
            // We have the table.cf['key']['column'] = 'value' case.

            // get the column name
            String columnName = CliCompiler.getColumn(columnFamilySpec, 0);

            // do the insert
            try
            {
                thriftClient_.insert(tableName, key, new ColumnPath(columnFamily, null, columnName.getBytes("UTF-8")),
                                 value.getBytes(), System.currentTimeMillis(), ConsistencyLevel.ONE);
            }
            catch (UnsupportedEncodingException e)
            {
                throw new RuntimeException(e);
            }

            css_.out.println("Value inserted.");
        }
        else
        {
            /* for now (until we support batch sets) */
            assert(false);
        }
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
    private void executeConnect(CommonTree ast) throws TException
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
