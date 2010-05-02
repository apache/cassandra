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

import org.apache.cassandra.auth.SimpleAuthenticator;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.thrift.*;
import org.apache.thrift.*;

import org.antlr.runtime.tree.*;

import java.util.*;
import java.io.UnsupportedEncodingException;

import org.apache.commons.lang.ArrayUtils;

// Cli Client Side Library
public class CliClient 
{
    private Cassandra.Client thriftClient_ = null;
    private CliSessionState css_ = null;
    private String keySpace = null;
    private String username = null;
    private Map<String, Map<String, Map<String, String>>> keyspacesMap = new HashMap<String, Map<String,Map<String,String>>>();

    public CliClient(CliSessionState css, Cassandra.Client thriftClient)
    {
        css_ = css;
        thriftClient_ = thriftClient;
    }

    // Execute a CLI Statement 
    public void executeCLIStmt(String stmt) throws TException, NotFoundException, InvalidRequestException, UnavailableException, TimedOutException, IllegalAccessException, ClassNotFoundException, InstantiationException
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
       css_.out.println("?                                                                  Same as help.");
       css_.out.println("help                                                          Display this help.");
       css_.out.println("connect <hostname>/<port>                             Connect to thrift service.");
       css_.out.println("use <keyspace>                                    Switch to a specific keyspace.");
       css_.out.println("use <keyspace> <username> 'password'              Switch to privileged keyspace.");
       css_.out.println("describe keyspace <keyspacename>                              Describe keyspace.");
       css_.out.println("exit                                                                   Exit CLI.");
       css_.out.println("quit                                                                   Exit CLI.");
       css_.out.println("show cluster name                                          Display cluster name.");
       css_.out.println("show keyspaces                                           Show list of keyspaces.");
       css_.out.println("show api version                                        Show server API version.");
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
        
        if (!(keyspacesMap.get(keySpace).containsKey(columnFamily)))
        {
            css_.out.println("No such column family: " + columnFamily);
            return;
        }
            
        isSuper = keyspacesMap.get(keySpace).get(columnFamily).get("Type").equals("Super") ? true : false;
     
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
                             timestampMicros(), ConsistencyLevel.ONE);
        css_.out.println(String.format("%s removed.", (columnSpecCnt == 0) ? "row" : "column"));
    }

    private static long timestampMicros()
    {
        // we use microsecond resolution for compatibility with other client libraries, even though
        // we can't actually get microsecond precision.
        return System.currentTimeMillis() * 1000;
    }

    private void doSlice(String keyspace, String key, String columnFamily, byte[] superColumnName)
            throws InvalidRequestException, UnavailableException, TimedOutException, TException, UnsupportedEncodingException, IllegalAccessException, NotFoundException, InstantiationException, ClassNotFoundException
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
 
    private String formatSuperColumnName(String keyspace, String columnFamily, SuperColumn column) throws NotFoundException, TException, ClassNotFoundException, IllegalAccessException, InstantiationException
    {
        return getFormatTypeForColumn(keyspacesMap.get(keyspace).get(columnFamily).get("CompareWith")).getString(column.name);
    }

    private String formatSubcolumnName(String keyspace, String columnFamily, Column subcolumn) throws NotFoundException, TException, ClassNotFoundException, IllegalAccessException, InstantiationException
    {
        return getFormatTypeForColumn(keyspacesMap.get(keyspace).get(columnFamily).get("CompareSubcolumnsWith")).getString(subcolumn.name);
    }

    private String formatColumnName(String keyspace, String columnFamily, Column column) throws ClassNotFoundException, NotFoundException, TException, IllegalAccessException, InstantiationException
    {
        return getFormatTypeForColumn(keyspacesMap.get(keyspace).get(columnFamily).get("CompareWith")).getString(column.name);
    }

    private AbstractType getFormatTypeForColumn(String compareWith) throws ClassNotFoundException, IllegalAccessException, InstantiationException
    {
        AbstractType type;
        try {
            type = (AbstractType) Class.forName(compareWith).newInstance();
        } catch (ClassNotFoundException e) {
            type = BytesType.class.newInstance();
        }
        return type;
    }

    // Execute GET statement
    private void executeGet(CommonTree ast) throws TException, NotFoundException, InvalidRequestException, UnavailableException, TimedOutException, UnsupportedEncodingException, IllegalAccessException, InstantiationException, ClassNotFoundException
    {
        if (!CliMain.isConnected() || !hasKeySpace())
            return;

        // This will never happen unless the grammar is broken
        assert (ast.getChildCount() == 1) : "serious parsing error (this is a bug).";

        CommonTree columnFamilySpec = (CommonTree)ast.getChild(0);
        assert(columnFamilySpec.getType() == CliParser.NODE_COLUMN_ACCESS);

        String key = CliCompiler.getKey(columnFamilySpec);
        String columnFamily = CliCompiler.getColumnFamily(columnFamilySpec);
        int columnSpecCnt = CliCompiler.numColumnSpecifiers(columnFamilySpec); 
        
        if (!(keyspacesMap.get(keySpace).containsKey(columnFamily)))
        {
            css_.out.println("No such column family: " + columnFamily);
            return;
        }
        
        boolean isSuper = keyspacesMap.get(keySpace).get(columnFamily).get("Type").equals("Super") ? true : false;
        
        byte[] superColumnName = null;
        byte[] columnName = null;
        
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
        ColumnPath path = new ColumnPath(columnFamily).setSuper_column(superColumnName).setColumn(columnName);
        Column column = thriftClient_.get(key.getBytes(), path, ConsistencyLevel.ONE).column;
        css_.out.printf("=> (column=%s, value=%s, timestamp=%d)\n", formatColumnName(keySpace, columnFamily, column),
                        new String(column.value, "UTF-8"), column.timestamp);
    }

    // Execute SET statement
    private void executeSet(CommonTree ast) throws TException, InvalidRequestException, UnavailableException, TimedOutException, UnsupportedEncodingException
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
        byte[] columnName = null;

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
        thriftClient_.insert(key.getBytes(), new ColumnParent(columnFamily).setSuper_column(superColumnName),
                             new Column(columnName, value.getBytes(), timestampMicros()), ConsistencyLevel.ONE);
        
        css_.out.println("Value inserted.");
    }
    
    private void executeShowClusterName() throws TException
    {
        if (!CliMain.isConnected())
            return;
        css_.out.println(thriftClient_.describe_cluster_name());
    }
    
    private void executeShowVersion() throws TException
    {
        if (!CliMain.isConnected())
            return;
        css_.out.println(thriftClient_.describe_version());
    }

    // process "show tables" statement
    private void executeShowTables(CommonTree ast) throws TException
    {
        if (!CliMain.isConnected())
            return;
        
        Set<String> tables = thriftClient_.describe_keyspaces();
        for (String table : tables)
        {
            css_.out.println(table);
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
    
    public void setKeyspace(String keySpace) throws NotFoundException, TException 
    {
        this.keySpace = keySpace;
        getCFMetaData(keySpace);
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
            CliMain.updateCompletor(keyspacesMap.get(keySpace).keySet());
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
