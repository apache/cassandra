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
import org.apache.cassandra.cql.common.Utils;
import org.apache.cassandra.service.Cassandra;
import org.apache.cassandra.service.CassandraException;
import org.apache.cassandra.service.CqlResult_t;
import org.apache.cassandra.service.column_t;
import org.apache.cassandra.service.Cassandra.Client;
import org.apache.cassandra.utils.LogUtil;

import java.util.*;

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
    public void executeCLIStmt(String stmt) throws TException 
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
       css_.out.println("?                                                         Same as help.");
       css_.out.println("connect <hostname>/<port>                                 Connect to Cassandra's thrift service.");
       css_.out.println("describe table <tbl>                                      Describe table.");
       css_.out.println("exit                                                      Exit CLI.");
       css_.out.println("explain plan [<set stmt>|<get stmt>|<select stmt>]        Explains the PLAN for specified stmt.");
       css_.out.println("help                                                      Display this help.");
       css_.out.println("quit                                                      Exit CLI.");
       css_.out.println("show config file                                          Display contents of config file");
       css_.out.println("show cluster name                                         Display cassandra server version");
       css_.out.println("show tables                                               Show list of tables.");
       css_.out.println("show version                                              Show server version.");
       css_.out.println("select ...                                                CQL select statement (TBD).");
       css_.out.println("get ...                                                   CQL data retrieval statement.");
       css_.out.println("set ...                                                   CQL DML statement.");
       css_.out.println("thrift get <tbl>.<cf>['<rowKey>']                         (will be deprecated)");            
       css_.out.println("thrift get <tbl>.<cf>['<rowKey>']['<colKey>']             (will be deprecated)");            
       css_.out.println("thrift set <tbl>.<cf>['<rowKey>']['<colKey>'] = '<value>' (will be deprecated)");    
    }

    private void cleanupAndExit()
    {
        CliMain.disconnect();
        System.exit(0);
    }

    // Execute GET statement
    private void executeGet(CommonTree ast) throws TException
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
        	List<column_t> columns = new ArrayList<column_t>();
        	try
        	{
        		columns = thriftClient_.get_slice(tableName, key, columnFamily, -1, 1000000);
        	}
        	catch(CassandraException cex)
        	{
        		css_.out.println(LogUtil.throwableToString(cex));
        	}
            int size = columns.size();
            for (Iterator<column_t> colIter = columns.iterator(); colIter.hasNext(); )
            {
                column_t col = colIter.next();
                css_.out.printf("  (column=%s, value=%s; timestamp=%d)\n",
                                 col.columnName, col.value, col.timestamp);
            }
            css_.out.println("Returned " + size + " rows.");
        }
        else if (columnSpecCnt == 1)
        {
            // table.cf['key']['column']
            String columnName = CliCompiler.getColumn(columnFamilySpec, 0);
            column_t col = new column_t();
            try
            {
            	col = thriftClient_.get_column(tableName, key, columnFamily + ":" + columnName);
	    	}
	    	catch(CassandraException cex)
	    	{
	    		css_.out.println(LogUtil.throwableToString(cex));
	    	}
            
            css_.out.printf("==> (name=%s, value=%s; timestamp=%d)\n",
                            col.columnName, col.value, col.timestamp);
        }
        else
        {
            assert(false);
        }
    }

    // Execute SET statement
    private void executeSet(CommonTree ast) throws TException
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
        String value         = Utils.unescapeSQLString(ast.getChild(1).getText());

        // assume simple columnFamily for now
        if (columnSpecCnt == 1)
        {
            // We have the table.cf['key']['column'] = 'value' case.

            // get the column name
            String columnName = CliCompiler.getColumn(columnFamilySpec, 0);

            // do the insert
            thriftClient_.insert(tableName, key, columnFamily + ":" + columnName,
                                 value.getBytes(), System.currentTimeMillis());

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

        String propertyValue = thriftClient_.getStringProperty(propertyName);
        css_.out.println(propertyValue);
        return;
    }

    // process "show tables" statement
    private void executeShowTables(CommonTree ast) throws TException
    {
        if (!CliMain.isConnected())
            return;
        
        List<String> tables = thriftClient_.getStringListProperty("tables");
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
        String describe = thriftClient_.describeTable(tableName);
        css_.out.println(describe);
        return;
    }

    // process a statement of the form: connect hostname/port
    private void executeConnect(CommonTree ast) throws TException
    {
        int portNumber = Integer.parseInt(ast.getChild(1).getText());
        Tree idList = ast.getChild(0);
        
        StringBuffer hostName = new StringBuffer();
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

    // execute CQL query on server
    public void executeQueryOnServer(String query) throws TException
    {
        if (!CliMain.isConnected())
            return;
        
        CqlResult_t result = thriftClient_.executeQuery(query);
        
        if (result == null)
        {
            css_.out.println("Unexpected error. Received null result from server.");
            return;
        }

        if ((result.errorTxt != null) || (result.errorCode != 0))
        {
            css_.out.println("Error: " + result.errorTxt);
        }
        else
        {
            List<Map<String, String>> rows = result.resultSet;
            
            if (rows != null)
            {
                for (Map<String, String> row : rows)
                {
                    for (Iterator<Map.Entry<String, String>> it = row.entrySet().iterator(); it.hasNext(); )
                    {
                        Map.Entry<String, String> entry = it.next();
                        String key = entry.getKey();
                        String value = entry.getValue();
                        css_.out.print(key + " = " + value + "; ");
                    }
                    css_.out.println();
                }
            }
            css_.out.println("Statement processed.");
        }
    }
}
