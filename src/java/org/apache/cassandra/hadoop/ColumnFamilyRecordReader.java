package org.apache.cassandra.hadoop;

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

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.*;

import com.google.common.collect.AbstractIterator;

import org.apache.cassandra.auth.SimpleAuthenticator;
import org.apache.cassandra.config.ConfigurationException;
import org.apache.cassandra.db.IColumn;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.TypeParser;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.thrift.*;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Pair;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.thrift.TException;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ColumnFamilyRecordReader extends RecordReader<ByteBuffer, SortedMap<ByteBuffer, IColumn>>
{
    private static final Logger                              logger = LoggerFactory
                                                                            .getLogger(ColumnFamilyRecordReader.class);
    private ColumnFamilySplit                                split;
    private RowIterator                                      iter;
    private Pair<ByteBuffer, SortedMap<ByteBuffer, IColumn>> currentRow;
    private SlicePredicate                                   predicate;
    private int                                              totalRowCount;                                            // total
                                                                                                                        // number
                                                                                                                        // of
                                                                                                                        // rows
                                                                                                                        // to
                                                                                                                        // fetch
    private int                                              batchRowCount;                                            // fetch
                                                                                                                        // this
                                                                                                                        // many
                                                                                                                        // per
                                                                                                                        // batch
    private String                                           cfName;
    private String                                           keyspace;
    private TSocket                                          socket;
    private Cassandra.Client                                 client;
    private ConsistencyLevel                                 consistencyLevel;

    public void close()
    {
        if (socket != null && socket.isOpen())
        {
            socket.close();
            socket = null;
            client = null;
        }
    }

    public ByteBuffer getCurrentKey()
    {
        return currentRow.left;
    }

    public SortedMap<ByteBuffer, IColumn> getCurrentValue()
    {
        return currentRow.right;
    }

    public float getProgress()
    {
        // the progress is likely to be reported slightly off the actual but
        // close enough
        return ((float) iter.rowsRead()) / totalRowCount;
    }

    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException
    {
        this.split = (ColumnFamilySplit) split;
        Configuration conf = context.getConfiguration();
        predicate = ConfigHelper.getInputSlicePredicate(conf);
        totalRowCount = ConfigHelper.getInputSplitSize(conf);
        batchRowCount = ConfigHelper.getRangeBatchSize(conf);
        cfName = ConfigHelper.getInputColumnFamily(conf);
        consistencyLevel = ConsistencyLevel.valueOf(ConfigHelper.getReadConsistencyLevel(conf));

        keyspace = ConfigHelper.getInputKeyspace(conf);

        try
        {
            // only need to connect once
            if (socket != null && socket.isOpen())
                return;

            // create connection using thrift
            List<String> locationsAttempted = new ArrayList<String>();
            for (Iterator<String> it = getLocations(conf); it.hasNext();)
            {
                String location = it.next();
                try
                {
                    socket = new TSocket(location, ConfigHelper.getRpcPort(conf));
                    TBinaryProtocol binaryProtocol = new TBinaryProtocol(new TFramedTransport(socket));
                    client = new Cassandra.Client(binaryProtocol);
                    socket.open();
                    break;
                }
                catch (TException e)
                {
                    logger.info("failed to connect to " + location + ':' + ConfigHelper.getRpcPort(conf), e);
                    locationsAttempted.add(location);
                    client = null;
                }
            }
            if (null == client)
            {
                throw new RuntimeException("For the split " + split + " there were no locations "
                        + (ConfigHelper.getInputSplitUseOnlySameDCReplica(conf) ? "(from same DC) " : "") + "alive: "
                        + StringUtils.join(locationsAttempted, ", "));
            }
            // log in
            client.set_keyspace(keyspace);
            if (ConfigHelper.getInputKeyspaceUserName(conf) != null)
            {
                Map<String, String> creds = new HashMap<String, String>();
                creds.put(SimpleAuthenticator.USERNAME_KEY, ConfigHelper.getInputKeyspaceUserName(conf));
                creds.put(SimpleAuthenticator.PASSWORD_KEY, ConfigHelper.getInputKeyspacePassword(conf));
                AuthenticationRequest authRequest = new AuthenticationRequest(creds);
                client.login(authRequest);
            }
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }

        iter = new RowIterator();
    }

    public boolean nextKeyValue() throws IOException
    {
        if (!iter.hasNext())
            return false;
        currentRow = iter.next();
        return true;
    }

    // we don't use endpointsnitch since we are trying to support hadoop nodes that are
    // not necessarily on Cassandra machines, too. This should be adequate for
    // single-DC clusters, at least.
    private Iterator<String> getLocations(final Configuration conf) throws IOException
    {
        try
        {
            for (InetAddress address : InetAddress.getAllByName(InetAddress.getLocalHost().getHostAddress()))
            {
                for (final String location : split.getLocations())
                {
                    InetAddress locationAddress = getInetAddressByName(location);
                    if (address.equals(locationAddress))
                    {
                        // add fall back replicas from same DC via the following
                        // Iterator
                        return new SplitEndpointIterator(location, conf);
                    }
                }
            }
        }
        catch (UnknownHostException e)
        {
            throw new AssertionError(e);
        }
        
        return Arrays.asList(split.getLocations()).iterator();
    }

    private static InetAddress getInetAddressByName(String name)
    {
        try
        {
            return InetAddress.getByName(name);
        }
        catch (UnknownHostException e)
        {
            throw new AssertionError(e);
        }
    }

    private class RowIterator extends AbstractIterator<Pair<ByteBuffer, SortedMap<ByteBuffer, IColumn>>>
    {
        private List<KeySlice>     rows;
        private String             startToken;
        private int                totalRead = 0;
        private int                i         = 0;
        private final AbstractType comparator;
        private final AbstractType subComparator;
        private final IPartitioner partitioner;

        private RowIterator()
        {
            try
            {
                partitioner = FBUtilities.newPartitioner(client.describe_partitioner());

                // Get the Keyspace metadata, then get the specific CF metadata
                // in order to populate the sub/comparator.
                KsDef ks_def = client.describe_keyspace(keyspace);
                List<String> cfnames = new ArrayList<String>();
                for (CfDef cfd : ks_def.cf_defs)
                    cfnames.add(cfd.name);
                int idx = cfnames.indexOf(cfName);
                CfDef cf_def = ks_def.cf_defs.get(idx);

                comparator = TypeParser.parse(cf_def.comparator_type);
                subComparator = cf_def.subcomparator_type == null ? null : TypeParser.parse(cf_def.subcomparator_type);
            }
            catch (ConfigurationException e)
            {
                throw new RuntimeException("unable to load sub/comparator", e);
            }
            catch (TException e)
            {
                throw new RuntimeException("error communicating via Thrift", e);
            }
            catch (Exception e)
            {
                throw new RuntimeException("unable to load keyspace " + keyspace, e);
            }
        }

        private void maybeInit()
        {
            // check if we need another batch
            if (rows != null && i >= rows.size())
                rows = null;

            if (rows != null)
                return;

            if (startToken == null)
            {
                startToken = split.getStartToken();
            }
            else if (startToken.equals(split.getEndToken()))
            {
                rows = null;
                return;
            }

            KeyRange keyRange = new KeyRange(batchRowCount).setStart_token(startToken)
                    .setEnd_token(split.getEndToken());
            try
            {
                rows = client.get_range_slices(new ColumnParent(cfName), predicate, keyRange, consistencyLevel);

                // nothing new? reached the end
                if (rows.isEmpty())
                {
                    rows = null;
                    return;
                }

                // reset to iterate through this new batch
                i = 0;

                // prepare for the next slice to be read
                KeySlice lastRow = rows.get(rows.size() - 1);
                ByteBuffer rowkey = lastRow.key;
                startToken = partitioner.getTokenFactory().toString(partitioner.getToken(rowkey));
            }
            catch (Exception e)
            {
                throw new RuntimeException(e);
            }
        }

        /**
         * @return total number of rows read by this record reader
         */
        public int rowsRead()
        {
            return totalRead;
        }

        protected Pair<ByteBuffer, SortedMap<ByteBuffer, IColumn>> computeNext()
        {
            maybeInit();
            if (rows == null)
                return endOfData();

            totalRead++;
            KeySlice ks = rows.get(i++);
            SortedMap<ByteBuffer, IColumn> map = new TreeMap<ByteBuffer, IColumn>(comparator);
            for (ColumnOrSuperColumn cosc : ks.columns)
            {
                IColumn column = unthriftify(cosc);
                map.put(column.name(), column);
            }
            return new Pair<ByteBuffer, SortedMap<ByteBuffer, IColumn>>(ks.key, map);
        }

        private IColumn unthriftify(ColumnOrSuperColumn cosc)
        {
            if (cosc.column == null)
                return unthriftifySuper(cosc.super_column);
            return unthriftifySimple(cosc.column);
        }

        private IColumn unthriftifySuper(SuperColumn super_column)
        {
            org.apache.cassandra.db.SuperColumn sc = new org.apache.cassandra.db.SuperColumn(super_column.name,
                    subComparator);
            for (Column column : super_column.columns)
            {
                sc.addColumn(unthriftifySimple(column));
            }
            return sc;
        }

        private IColumn unthriftifySimple(Column column)
        {
            return new org.apache.cassandra.db.Column(column.name, column.value, column.timestamp);
        }
    }

    private class SplitEndpointIterator extends AbstractIterator<String>
    {
        private final boolean       restrictToSameDC;
        private final String        location;
        private final Configuration conf;
        private Cassandra.Client    client;
        private List<String>        endpoints;
        private int                 endpointsIdx = -1;

        SplitEndpointIterator(final String location, final Configuration conf)
        {
            this.location = location;
            this.conf = conf;
            restrictToSameDC = ConfigHelper.getInputSplitUseOnlySameDCReplica(conf);
        }

        protected String computeNext()
        {
            if (-1 == endpointsIdx)
            {
                // location is the preference. always return it first.
                endpointsIdx = 0;
                return location;
            }
            else
            {
                if (null == endpoints)
                {
                    try
                    {
                        for (String nextLocation : split.getLocations())
                        {
                            try
                            {
                                endpoints = sortEndpointsByProximity(nextLocation, Arrays.asList(split.getLocations()),
                                        restrictToSameDC);
                                if (location.equals(endpoints.get(0)))
                                {
                                    ++endpointsIdx;
                                }
                                break;
                            }
                            catch (TException e)
                            {
                                logger.info(
                                        "failed to sortEndpointsByProximity(" + location + ", ["
                                                + StringUtils.join(split.getLocations(), ',') + "], "
                                                + restrictToSameDC + ")", e);
                            }
                            catch (IOException e)
                            {
                                logger.info(
                                        "failed to sortEndpointsByProximity(" + location + ", ["
                                                + StringUtils.join(split.getLocations(), ',') + "], "
                                                + restrictToSameDC + ")", e);
                            }
                        }
                    }
                    catch (InvalidRequestException e)
                    {
                        throw new AssertionError(e);
                    }
                    if (null == endpoints)
                    {
                        throw new AssertionError("failed to find any fallback replica endpoints from "
                                + StringUtils.join(split.getLocations(), ','));
                    }
                }
                if (endpoints.size() > endpointsIdx)
                {
                    return endpoints.get(endpointsIdx++);
                }
            }
            return endOfData();
        }

        private List<String> sortEndpointsByProximity(String connectTo, List<String> endpoints, boolean restrictToSameDC)
                throws InvalidRequestException, TException, IOException
        {
            try
            {
                // try first our configured initialAddress
                return getClient(ConfigHelper.getInitialAddress(conf)).sort_endpoints_by_proximity(location, endpoints,
                        restrictToSameDC);
            }
            catch (IOException ex)
            {
                // connect through the endpoint. if it fails it's no good
                // anyway.
                return getClient(connectTo).sort_endpoints_by_proximity(location, endpoints, restrictToSameDC);
            }
        }

        private Cassandra.Client getClient(String host) throws IOException
        {
            if (null == client)
            {
                client = ColumnFamilyInputFormat.createConnection(host, ConfigHelper.getRpcPort(conf), true);
            }
            return client;
        }
    }
}
