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

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.util.zip.Deflater;

import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.Compression;
import org.apache.cassandra.thrift.CqlResult;
import org.apache.cassandra.thrift.CqlRow;
import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.cassandra.thrift.TimedOutException;
import org.apache.cassandra.thrift.UnavailableException;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Connection
{
    private static final Logger logger = LoggerFactory.getLogger(Connection.class);
    
    public String hostName;
    public int port;
    private Cassandra.Client client;
    private TTransport transport;
    private Compression defaultCompression = Compression.GZIP;
    
    public Connection(String keyspaceName, String...hosts) throws InvalidRequestException, TException
    {
        assert hosts.length > 0;
        
        for (String hostSpec : hosts)
        {
            String[] parts = hostSpec.split(":", 2);
            this.hostName = parts[0];
            this.port = Integer.parseInt(parts[1]);
            
            // TODO: This will need to do connection pooling.
            break;
        }
        
        TSocket socket = new TSocket(hostName, port);
        transport = new TFramedTransport(socket);
        TProtocol protocol = new TBinaryProtocol(transport);
        client = new Cassandra.Client(protocol);
        socket.open();
        
        client.set_keyspace(keyspaceName);
    }
    
    private ByteBuffer compressQuery(String queryStr, Compression compression)
    {
        byte[] data = queryStr.getBytes();
        Deflater compressor = new Deflater();
        compressor.setInput(data);
        compressor.finish();
        
        ByteArrayOutputStream byteArray = new ByteArrayOutputStream();
        byte[] buffer = new byte[1024];
        
        while (!compressor.finished())
        {
            int size = compressor.deflate(buffer);
            byteArray.write(buffer, 0, size);
        }
        
        logger.trace("Compressed query statement {} bytes in length to {} bytes",
                     data.length,
                     byteArray.size());
        
        return ByteBuffer.wrap(byteArray.toByteArray());
    }
    
    public CqlResult execute(String queryStr)
    throws InvalidRequestException, UnavailableException, TimedOutException, TException
    {
        return execute(queryStr, getDefaultCompression());
    }
    
    public CqlResult execute(String queryStr, Compression compression)
    throws InvalidRequestException, UnavailableException, TimedOutException, TException
    {
        logger.trace("Executing CQL Query: {}", queryStr);
        return client.execute_cql_query(compressQuery(queryStr, compression), compression);
    }
    
    public Compression getDefaultCompression()
    {
        return defaultCompression;
    }

    public void setDefaultCompression(Compression defaultCompression)
    {
        this.defaultCompression = defaultCompression;
    }

    public static void main(String[] args) throws Exception
    {
        Connection conn = new Connection("Keyspace1", "localhost:9160");
        CqlResult result = conn.execute("UPDATE Standard2 USING CONSISTENCY.ONE WITH ROW(\"mango\", COL(\"disposition\", \"fruit\"));");
        String selectQ = "SELECT FROM Standard2 WHERE KEY > \"apple\" AND KEY < \"carrot\" ROWLIMIT 5 DESC;";
        result = conn.execute(selectQ);
        switch (result.type)
        {
            case ROWS:
                for (CqlRow row : result.rows)
                {
                    System.out.println("KEY: " + new String(row.key.array()));
                    for (org.apache.cassandra.thrift.Column col : row.columns)
                    {
                        System.out.println("  COL: " + new String(col.name.array()) + ":" + new String(col.value.array()));
                    }
                }
        }
    }
}
