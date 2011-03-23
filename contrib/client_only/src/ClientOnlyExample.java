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

import java.nio.ByteBuffer;
import java.util.*;

import org.apache.cassandra.db.*;
import org.apache.cassandra.db.filter.QueryPath;
import org.apache.cassandra.service.StorageProxy;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.thrift.*;
import org.apache.cassandra.utils.ByteBufferUtil;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ClientOnlyExample
{
    private static final Logger logger = LoggerFactory.getLogger(ClientOnlyExample.class);

    private static final String KEYSPACE = "Keyspace1";
    private static final String COLUMN_FAMILY = "Standard1";
    
    private static void startClient() throws Exception
    {
        StorageService.instance.initClient();
        // sleep for a bit so that gossip can do its thing.
        try
        {
            Thread.sleep(10000L);
        }
        catch (Exception ex)
        {
            throw new AssertionError(ex);
        }
    }

    private static void testWriting() throws Exception
    {
        // do some writing.
        for (int i = 0; i < 100; i++)
        {
            RowMutation change = new RowMutation(KEYSPACE, ByteBufferUtil.bytes(("key" + i)));
            ColumnPath cp = new ColumnPath(COLUMN_FAMILY).setColumn(("colb").getBytes());
            change.add(new QueryPath(cp), ByteBufferUtil.bytes(("value" + i)), 0);

            // don't call change.apply().  The reason is that is makes a static call into Table, which will perform
            // local storage initialization, which creates local directories.
            // change.apply();

            StorageProxy.mutate(Arrays.asList(change), ConsistencyLevel.ONE);
            System.out.println("wrote key" + i);
        }
        System.out.println("Done writing.");
    }

    private static void testReading() throws Exception
    {
        // do some queries.
        Collection<ByteBuffer> cols = new ArrayList<ByteBuffer>()
        {{
            add(ByteBufferUtil.bytes("colb"));
        }};
        for (int i = 0; i < 100; i++)
        {
            List<ReadCommand> commands = new ArrayList<ReadCommand>();
            SliceByNamesReadCommand readCommand = new SliceByNamesReadCommand(KEYSPACE, ByteBufferUtil.bytes(("key" + i)),
                                                                              new QueryPath(COLUMN_FAMILY, null, null), cols);
            readCommand.setDigestQuery(false);
            commands.add(readCommand);
            List<Row> rows = StorageProxy.read(commands, ConsistencyLevel.ONE);
            assert rows.size() == 1;
            Row row = rows.get(0);
            ColumnFamily cf = row.cf;
            if (cf != null)
            {
                for (IColumn col : cf.getSortedColumns())
                {
                    System.out.println(ByteBufferUtil.string(col.name()) + ", " + ByteBufferUtil.string(col.value()));
                }
            }
            else
                System.err.println("This output indicates that nothing was read.");
        }
    }

    /**
     * First, bring one or more nodes up. Then run ClientOnlyExample with these VM arguments:
     *
     * -Xmx1G
     * -Dcassandra.config=/Users/gary/cassandra/conf/cassandra.yaml (optional, will first look for cassandra.yaml on classpath)
     *
     * Pass "write" or "read" into the program to exercise the various methods.
     *
     * Caveats:
     *
     * 1.  Much of cassandra is not reentrant.  That is, you can't spin a client up, down, then back up in the same jvm.
     * 2.  Because of the above, you still need to force-quit the process. StorageService.stopClient() doesn't (can't)
     *     spin everything down.
     */
    public static void main(String args[]) throws Exception
    {
        startClient();
        setupKeyspace(createConnection());
        testWriting();
        logger.info("Writing is done. Sleeping, then will try to read.");
        try
        {
            Thread.currentThread().sleep(10000);
        }
        catch (InterruptedException ex) 
        {
            throw new RuntimeException(ex);
        }
        
        testReading();
        
        // no need to do this:
        // StorageService.instance().decommission();
        // do this instead:
        StorageService.instance.stopClient();
        System.exit(0); // the only way to really stop the process.
    }
    
    /**
    * This method will fail if the keyspace already exists
    */
    private static void setupKeyspace(Cassandra.Iface client) throws TException, InvalidRequestException
    {
        List<CfDef> cfDefList = new ArrayList<CfDef>();
        CfDef columnFamily = new CfDef(KEYSPACE, COLUMN_FAMILY);
        cfDefList.add(columnFamily);

        try 
        {
            client.system_add_keyspace(new KsDef(KEYSPACE, "org.apache.cassandra.locator.SimpleStrategy", 1, cfDefList));
            int magnitude = client.describe_ring(KEYSPACE).size();
            try
            {
                Thread.sleep(1000 * magnitude);
            }
            catch (InterruptedException e)
            {
                throw new RuntimeException(e);
            }
        }
        catch (InvalidRequestException probablyExists) 
        {
            logger.warn("Problem creating keyspace: " + probablyExists.getMessage());    
        }
    }

    private static Cassandra.Iface createConnection() throws TTransportException
    {
        if (System.getProperty("cassandra.host") == null || System.getProperty("cassandra.port") == null)
        {
           logger.warn("cassandra.host or cassandra.port is not defined, using default");
        }
        return createConnection( System.getProperty("cassandra.host","localhost"),
                                 Integer.valueOf(System.getProperty("cassandra.port","9160")),
                                 Boolean.valueOf(System.getProperty("cassandra.framed", "true")) );
    }

    private static Cassandra.Client createConnection(String host, Integer port, boolean framed) throws TTransportException
    {
        TSocket socket = new TSocket(host, port);
        TTransport trans = framed ? new TFramedTransport(socket) : socket;
        trans.open();
        TProtocol protocol = new TBinaryProtocol(trans);

        return new Cassandra.Client(protocol);
    }
}
