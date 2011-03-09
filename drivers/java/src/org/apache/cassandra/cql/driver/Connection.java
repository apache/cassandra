/*
 * 
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 * 
 */

package org.apache.cassandra.cql.driver;

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.cassandra.thrift.AuthenticationException;
import org.apache.cassandra.thrift.AuthenticationRequest;
import org.apache.cassandra.thrift.AuthorizationException;
import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.Compression;
import org.apache.cassandra.thrift.CqlResult;
import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.cassandra.thrift.TimedOutException;
import org.apache.cassandra.thrift.UnavailableException;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** CQL connection object. */
public class Connection
{
    private static final Pattern KeyspacePattern = Pattern.compile("USE (\\w+);?", Pattern.CASE_INSENSITIVE | Pattern.MULTILINE);
    private static final Pattern SelectPattern = Pattern.compile("SELECT\\s+.+\\s+FROM\\s+(\\w+).*", Pattern.CASE_INSENSITIVE | Pattern.MULTILINE);
    
    public static Compression defaultCompression = Compression.GZIP;
    public final String hostName;
    public final int portNo;
    
    private static final Logger logger = LoggerFactory.getLogger(Connection.class);
    protected long timeOfLastFailure = 0;
    protected int numFailures = 0;
    private Cassandra.Client client;
    private TTransport transport;
    
    // todo: encapsulate.
    public String curKeyspace;
    public String curColumnFamily;
    public ColumnDecoder decoder;
    
    /**
     * Create a new <code>Connection</code> instance.
     * 
     * @param hostName hostname or IP address of the remote host
     * @param portNo TCP port number
     * @throws TTransportException if unable to connect
     */
    public Connection(String hostName, int portNo) throws TTransportException
    {
        this.hostName = hostName;
        this.portNo = portNo;
        TSocket socket = new TSocket(hostName, portNo);
        transport = new TFramedTransport(socket);
        TProtocol protocol = new TBinaryProtocol(transport);
        client = new Cassandra.Client(protocol);
        socket.open();
        
        logger.info("Connected to {}:{}", hostName, portNo);
    }
    

    /**
     * Create a new <code>Connection</code> instance.
     * 
     * @param hostName hostname or IP address of the remote host
     * @param portNo TCP port number
     * @throws AuthorizationException if authorization fails
     * @throws AuthenticationException for authentication failures
     * @throws TException on errors encountered issuing the request(s) 
     */
    public Connection(String hostName, int portNo, String userName, String password)
    throws AuthenticationException, AuthorizationException, TException
    {
        this(hostName, portNo);
        
        Map<String, String> credentials = new HashMap<String, String>();
        AuthenticationRequest areq = new AuthenticationRequest(credentials);
        client.login(areq) ;
    }
    
    /**
     * Execute a CQL query.
     * 
     * @param queryStr a CQL query string
     * @return the query results encoded as a CqlResult struct
     * @throws InvalidRequestException on poorly constructed or illegal requests
     * @throws UnavailableException when not all required replicas could be created/read
     * @throws TimedOutException when a cluster operation timed out
     * @throws TException
     */
    public CqlResult execute(String queryStr)
    throws InvalidRequestException, UnavailableException, TimedOutException, TException
    {
        return execute(queryStr, defaultCompression);
    }
    
    /**
     * Execute a CQL query.
     * 
     * @param queryStr a CQL query string
     * @param compress query compression to use
     * @return the query results encoded as a CqlResult struct
     * @throws InvalidRequestException on poorly constructed or illegal requests
     * @throws UnavailableException when not all required replicas could be created/read
     * @throws TimedOutException when a cluster operation timed out
     * @throws TException
     */
    public CqlResult execute(String queryStr, Compression compress)
    throws InvalidRequestException, UnavailableException, TimedOutException, TException
    {
        if (decoder == null)
            decoder = new ColumnDecoder(client.describe_keyspaces());
        
        Matcher isKeyspace = KeyspacePattern.matcher(queryStr);
        if (isKeyspace.matches())
            curKeyspace = isKeyspace.group(1);
        Matcher isSelect = SelectPattern.matcher(queryStr);
        if (isSelect.matches())
            curColumnFamily = isSelect.group(1);
        try
        {
            return client.execute_cql_query(Utils.compressQuery(queryStr, compress), compress);
        }
        catch (TException error)
        {
            numFailures++;
            timeOfLastFailure = System.currentTimeMillis();
            throw error;
        }
    }
    
    /** Shutdown the remote connection */
    public void close()
    {
        transport.close();
    }
    
    /** Connection state. */
    public boolean isOpen()
    {
        return transport.isOpen();
    }
}
