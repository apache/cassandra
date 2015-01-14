/*
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
package org.apache.cassandra.hadoop.cql3;

import java.io.IOException;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.CompositeType;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.db.marshal.TypeParser;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.exceptions.SyntaxException;
import org.apache.cassandra.hadoop.AbstractColumnFamilyOutputFormat;
import org.apache.cassandra.hadoop.AbstractColumnFamilyRecordWriter;
import org.apache.cassandra.hadoop.ConfigHelper;
import org.apache.cassandra.hadoop.HadoopCompat;
import org.apache.cassandra.thrift.*;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.util.Progressable;
import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransport;

/**
 * The <code>ColumnFamilyRecordWriter</code> maps the output &lt;key, value&gt;
 * pairs to a Cassandra column family. In particular, it applies the binded variables
 * in the value to the prepared statement, which it associates with the key, and in 
 * turn the responsible endpoint.
 *
 * <p>
 * Furthermore, this writer groups the cql queries by the endpoint responsible for
 * the rows being affected. This allows the cql queries to be executed in parallel,
 * directly to a responsible endpoint.
 * </p>
 *
 * @see CqlOutputFormat
 */
class CqlRecordWriter extends AbstractColumnFamilyRecordWriter<Map<String, ByteBuffer>, List<ByteBuffer>>
{
    private static final Logger logger = LoggerFactory.getLogger(CqlRecordWriter.class);

    // handles for clients for each range running in the threadpool
    protected final Map<InetAddress, RangeClient> clients;

    // host to prepared statement id mappings
    protected final ConcurrentHashMap<Cassandra.Client, Integer> preparedStatements = new ConcurrentHashMap<Cassandra.Client, Integer>();

    protected final String cql;

    protected AbstractType<?> keyValidator;
    protected String [] partitionKeyColumns;
    protected List<String> clusterColumns;

    /**
     * Upon construction, obtain the map that this writer will use to collect
     * mutations, and the ring cache for the given keyspace.
     *
     * @param context the task attempt context
     * @throws IOException
     */
    CqlRecordWriter(TaskAttemptContext context) throws IOException
    {
        this(HadoopCompat.getConfiguration(context));
        this.context = context;
    }

    CqlRecordWriter(Configuration conf, Progressable progressable) throws IOException
    {
        this(conf);
        this.progressable = progressable;
    }

    CqlRecordWriter(Configuration conf)
    {
        super(conf);
        this.clients = new HashMap<>();

        try
        {
            Cassandra.Client client = ConfigHelper.getClientFromOutputAddressList(conf);
            if (client != null)
            {
                client.set_keyspace(ConfigHelper.getOutputKeyspace(conf));
                String user = ConfigHelper.getOutputKeyspaceUserName(conf);
                String password = ConfigHelper.getOutputKeyspacePassword(conf);
                if ((user != null) && (password != null))
                    AbstractColumnFamilyOutputFormat.login(user, password, client);
                retrievePartitionKeyValidator(client);
                String cqlQuery = CqlConfigHelper.getOutputCql(conf).trim();
                if (cqlQuery.toLowerCase().startsWith("insert"))
                    throw new UnsupportedOperationException("INSERT with CqlRecordWriter is not supported, please use UPDATE/DELETE statement");
                cql = appendKeyWhereClauses(cqlQuery);

                TTransport transport = client.getOutputProtocol().getTransport();
                if (transport.isOpen())
                    transport.close();
            }
            else
            {
                throw new IllegalArgumentException("Invalid configuration specified " + conf);
            }
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }
    }
    
    @Override
    public void close() throws IOException
    {
        // close all the clients before throwing anything
        IOException clientException = null;
        for (RangeClient client : clients.values())
        {
            try
            {
                client.close();
            }
            catch (IOException e)
            {
                clientException = e;
            }
        }

        if (clientException != null)
            throw clientException;
    }
    
    /**
     * If the key is to be associated with a valid value, a mutation is created
     * for it with the given column family and columns. In the event the value
     * in the column is missing (i.e., null), then it is marked for
     * {@link Deletion}. Similarly, if the entire value for a key is missing
     * (i.e., null), then the entire key is marked for {@link Deletion}.
     * </p>
     *
     * @param keyColumns
     *            the key to write.
     * @param values
     *            the values to write.
     * @throws IOException
     */
    @Override
    public void write(Map<String, ByteBuffer> keyColumns, List<ByteBuffer> values) throws IOException
    {
        Range<Token> range = ringCache.getRange(getPartitionKey(keyColumns));

        // get the client for the given range, or create a new one
	final InetAddress address = ringCache.getEndpoint(range).get(0);
        RangeClient client = clients.get(address);
        if (client == null)
        {
            // haven't seen keys for this range: create new client
            client = new RangeClient(ringCache.getEndpoint(range));
            client.start();
            clients.put(address, client);
        }

        // add primary key columns to the bind variables
        List<ByteBuffer> allValues = new ArrayList<ByteBuffer>(values);
        for (String column : partitionKeyColumns)
            allValues.add(keyColumns.get(column));
        for (String column : clusterColumns)
            allValues.add(keyColumns.get(column));

        client.put(allValues);

        if (progressable != null)
            progressable.progress();
        if (context != null)
            HadoopCompat.progress(context);
    }

    /**
     * A client that runs in a threadpool and connects to the list of endpoints for a particular
     * range. Bound variables for keys in that range are sent to this client via a queue.
     */
    public class RangeClient extends AbstractRangeClient<List<ByteBuffer>>
    {
        /**
         * Constructs an {@link RangeClient} for the given endpoints.
         * @param endpoints the possible endpoints to execute the mutations on
         */
        public RangeClient(List<InetAddress> endpoints)
        {
            super(endpoints);
         }
        
        /**
         * Loops collecting cql binded variable values from the queue and sending to Cassandra
         */
        public void run()
        {
            outer:
            while (run || !queue.isEmpty())
            {
                List<ByteBuffer> bindVariables;
                try
                {
                    bindVariables = queue.take();
                }
                catch (InterruptedException e)
                {
                    // re-check loop condition after interrupt
                    continue;
                }

                Iterator<InetAddress> iter = endpoints.iterator();
                while (true)
                {
                    // send the mutation to the last-used endpoint.  first time through, this will NPE harmlessly.
                    try
                    {
                        int i = 0;
                        int itemId = preparedStatement(client);
                        while (bindVariables != null)
                        {
                            client.execute_prepared_cql3_query(itemId, bindVariables, ConsistencyLevel.ONE);
                            i++;
                            
                            if (i >= batchThreshold)
                                break;
                            
                            bindVariables = queue.poll();
                        }
                        
                        break;
                    }
                    catch (Exception e)
                    {
                        closeInternal();
                        if (!iter.hasNext())
                        {
                            lastException = new IOException(e);
                            break outer;
                        }
                    }

                    // attempt to connect to a different endpoint
                    try
                    {
                        InetAddress address = iter.next();
                        String host = address.getHostName();
                        int port = ConfigHelper.getOutputRpcPort(conf);
                        client = CqlOutputFormat.createAuthenticatedClient(host, port, conf);
                    }
                    catch (Exception e)
                    {
                        closeInternal();
                        // TException means something unexpected went wrong to that endpoint, so
                        // we should try again to another.  Other exceptions (auth or invalid request) are fatal.
                        if ((!(e instanceof TException)) || !iter.hasNext())
                        {
                            lastException = new IOException(e);
                            break outer;
                        }
                    }
                }
            }

            // close all our connections once we are done.
            closeInternal();
        }

        /** get prepared statement id from cache, otherwise prepare it from Cassandra server*/
        private int preparedStatement(Cassandra.Client client)
        {
            Integer itemId = preparedStatements.get(client);
            if (itemId == null)
            {
                CqlPreparedResult result;
                try
                {
                    result = client.prepare_cql3_query(ByteBufferUtil.bytes(cql), Compression.NONE);
                }
                catch (InvalidRequestException e)
                {
                    throw new RuntimeException("failed to prepare cql query " + cql, e);
                }
                catch (TException e)
                {
                    throw new RuntimeException("failed to prepare cql query " + cql, e);
                }

                Integer previousId = preparedStatements.putIfAbsent(client, Integer.valueOf(result.itemId));
                itemId = previousId == null ? result.itemId : previousId;
            }
            return itemId;
        }
    }

    private ByteBuffer getPartitionKey(Map<String, ByteBuffer> keyColumns)
    {
        ByteBuffer partitionKey;
        if (keyValidator instanceof CompositeType)
        {
            ByteBuffer[] keys = new ByteBuffer[partitionKeyColumns.length];
            for (int i = 0; i< keys.length; i++)
                keys[i] = keyColumns.get(partitionKeyColumns[i]);

            partitionKey = CompositeType.build(keys);
        }
        else
        {
            partitionKey = keyColumns.get(partitionKeyColumns[0]);
        }
        return partitionKey;
    }

    /** retrieve the key validator from system.schema_columnfamilies table */
    private void retrievePartitionKeyValidator(Cassandra.Client client) throws Exception
    {
        String keyspace = ConfigHelper.getOutputKeyspace(conf);
        String cfName = ConfigHelper.getOutputColumnFamily(conf);
        String query = "SELECT key_validator," +
        		       "       key_aliases," +
        		       "       column_aliases " +
                       "FROM system.schema_columnfamilies " +
                       "WHERE keyspace_name='%s' and columnfamily_name='%s'";
        String formatted = String.format(query, keyspace, cfName);
        CqlResult result = client.execute_cql3_query(ByteBufferUtil.bytes(formatted), Compression.NONE, ConsistencyLevel.ONE);

        Column rawKeyValidator = result.rows.get(0).columns.get(0);
        String validator = ByteBufferUtil.string(ByteBuffer.wrap(rawKeyValidator.getValue()));
        keyValidator = parseType(validator);
        
        Column rawPartitionKeys = result.rows.get(0).columns.get(1);
        String keyString = ByteBufferUtil.string(ByteBuffer.wrap(rawPartitionKeys.getValue()));
        logger.debug("partition keys: {}", keyString);

        List<String> keys = FBUtilities.fromJsonList(keyString);
        partitionKeyColumns = new String[keys.size()];
        int i = 0;
        for (String key : keys)
        {
            partitionKeyColumns[i] = key;
            i++;
        }

        Column rawClusterColumns = result.rows.get(0).columns.get(2);
        String clusterColumnString = ByteBufferUtil.string(ByteBuffer.wrap(rawClusterColumns.getValue()));

        logger.debug("cluster columns: {}", clusterColumnString);
        clusterColumns = FBUtilities.fromJsonList(clusterColumnString);
    }

    private AbstractType<?> parseType(String type) throws ConfigurationException
    {
        try
        {
            // always treat counters like longs, specifically CCT.serialize is not what we need
            if (type != null && type.equals("org.apache.cassandra.db.marshal.CounterColumnType"))
                return LongType.instance;
            return TypeParser.parse(type);
        }
        catch (SyntaxException e)
        {
            throw new ConfigurationException(e.getMessage(), e);
        }
    }

    /**
     * add where clauses for partition keys and cluster columns
     */
    private String appendKeyWhereClauses(String cqlQuery)
    {
        String keyWhereClause = "";

        for (String partitionKey : partitionKeyColumns)
            keyWhereClause += String.format("%s = ?", keyWhereClause.isEmpty() ? quote(partitionKey) : (" AND " + quote(partitionKey)));
        for (String clusterColumn : clusterColumns)
            keyWhereClause += " AND " + quote(clusterColumn) + " = ?";

        return cqlQuery + " WHERE " + keyWhereClause;
    }

    /** Quoting for working with uppercase */
    private String quote(String identifier)
    {
        return "\"" + identifier.replaceAll("\"", "\"\"") + "\"";
    }
}
