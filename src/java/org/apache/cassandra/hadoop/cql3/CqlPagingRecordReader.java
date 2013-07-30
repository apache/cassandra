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
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.util.*;

import com.google.common.collect.AbstractIterator;
import com.google.common.collect.Iterables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.CompositeType;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.db.marshal.TypeParser;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.exceptions.SyntaxException;
import org.apache.cassandra.hadoop.ColumnFamilySplit;
import org.apache.cassandra.hadoop.ConfigHelper;
import org.apache.cassandra.thrift.*;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransport;

/**
 * Hadoop RecordReader read the values return from the CQL query
 * It use CQL key range query to page through the wide rows.
 * <p/>
 * Return List<IColumn> as keys columns
 * <p/>
 * Map<ByteBuffer, IColumn> as column name to columns mappings
 */
public class CqlPagingRecordReader extends RecordReader<Map<String, ByteBuffer>, Map<String, ByteBuffer>>
        implements org.apache.hadoop.mapred.RecordReader<Map<String, ByteBuffer>, Map<String, ByteBuffer>>
{
    private static final Logger logger = LoggerFactory.getLogger(CqlPagingRecordReader.class);

    public static final int DEFAULT_CQL_PAGE_LIMIT = 1000; // TODO: find the number large enough but not OOM

    private ColumnFamilySplit split;
    private RowIterator rowIterator;

    private Pair<Map<String, ByteBuffer>, Map<String, ByteBuffer>> currentRow;
    private int totalRowCount; // total number of rows to fetch
    private String keyspace;
    private String cfName;
    private Cassandra.Client client;
    private ConsistencyLevel consistencyLevel;

    // partition keys -- key aliases
    private List<BoundColumn> partitionBoundColumns = new ArrayList<BoundColumn>();

    // cluster keys -- column aliases
    private List<BoundColumn> clusterColumns = new ArrayList<BoundColumn>();

    // map prepared query type to item id
    private Map<Integer, Integer> preparedQueryIds = new HashMap<Integer, Integer>();

    // cql query select columns
    private String columns;

    // the number of cql rows per page
    private int pageRowSize;

    // user defined where clauses
    private String userDefinedWhereClauses;

    private IPartitioner partitioner;

    private AbstractType<?> keyValidator;

    public CqlPagingRecordReader()
    {
        super();
    }

    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException
    {
        this.split = (ColumnFamilySplit) split;
        Configuration conf = context.getConfiguration();
        totalRowCount = (this.split.getLength() < Long.MAX_VALUE)
                      ? (int) this.split.getLength()
                      : ConfigHelper.getInputSplitSize(conf);
        cfName = ConfigHelper.getInputColumnFamily(conf);
        consistencyLevel = ConsistencyLevel.valueOf(ConfigHelper.getReadConsistencyLevel(conf));
        keyspace = ConfigHelper.getInputKeyspace(conf);
        columns = CqlConfigHelper.getInputcolumns(conf);
        userDefinedWhereClauses = CqlConfigHelper.getInputWhereClauses(conf);

        try
        {
            pageRowSize = Integer.parseInt(CqlConfigHelper.getInputPageRowSize(conf));
        }
        catch (NumberFormatException e)
        {
            pageRowSize = DEFAULT_CQL_PAGE_LIMIT;
        }

        partitioner = ConfigHelper.getInputPartitioner(context.getConfiguration());

        try
        {
            if (client != null)
                return;

            // create connection using thrift
            String location = getLocation();

            int port = ConfigHelper.getInputRpcPort(conf);
            client = CqlPagingInputFormat.createAuthenticatedClient(location, port, conf);

            // retrieve partition keys and cluster keys from system.schema_columnfamilies table
            retrieveKeys();

            client.set_keyspace(keyspace);
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }

        rowIterator = new RowIterator();

        logger.debug("created {}", rowIterator);
    }

    public void close()
    {
        if (client != null)
        {
            TTransport transport = client.getOutputProtocol().getTransport();
            if (transport.isOpen())
                transport.close();
            client = null;
        }
    }

    public Map<String, ByteBuffer> getCurrentKey()
    {
        return currentRow.left;
    }

    public Map<String, ByteBuffer> getCurrentValue()
    {
        return currentRow.right;
    }

    public float getProgress()
    {
        if (!rowIterator.hasNext())
            return 1.0F;

        // the progress is likely to be reported slightly off the actual but close enough
        float progress = ((float) rowIterator.totalRead / totalRowCount);
        return progress > 1.0F ? 1.0F : progress;
    }

    public boolean nextKeyValue() throws IOException
    {
        if (!rowIterator.hasNext())
        {
            logger.debug("Finished scanning {} rows (estimate was: {})", rowIterator.totalRead, totalRowCount);
            return false;
        }

        try
        {
            currentRow = rowIterator.next();
        }
        catch (Exception e)
        {
            // throw it as IOException, so client can catch it and handle it at client side
            IOException ioe = new IOException(e.getMessage());
            ioe.initCause(ioe.getCause());
            throw ioe;
        }
        return true;
    }

    // we don't use endpointsnitch since we are trying to support hadoop nodes that are
    // not necessarily on Cassandra machines, too.  This should be adequate for single-DC clusters, at least.
    private String getLocation()
    {
        Collection<InetAddress> localAddresses = FBUtilities.getAllLocalAddresses();

        for (InetAddress address : localAddresses)
        {
            for (String location : split.getLocations())
            {
                InetAddress locationAddress;
                try
                {
                    locationAddress = InetAddress.getByName(location);
                }
                catch (UnknownHostException e)
                {
                    throw new AssertionError(e);
                }
                if (address.equals(locationAddress))
                {
                    return location;
                }
            }
        }
        return split.getLocations()[0];
    }

    // Because the old Hadoop API wants us to write to the key and value
    // and the new asks for them, we need to copy the output of the new API
    // to the old. Thus, expect a small performance hit.
    // And obviously this wouldn't work for wide rows. But since ColumnFamilyInputFormat
    // and ColumnFamilyRecordReader don't support them, it should be fine for now.
    public boolean next(Map<String, ByteBuffer> keys, Map<String, ByteBuffer> value) throws IOException
    {
        if (nextKeyValue())
        {
            value.clear();
            value.putAll(getCurrentValue());
            
            keys.clear();
            keys.putAll(getCurrentKey());

            return true;
        }
        return false;
    }

    public long getPos() throws IOException
    {
        return (long) rowIterator.totalRead;
    }

    public Map<String, ByteBuffer> createKey()
    {
        return new LinkedHashMap<String, ByteBuffer>();
    }

    public Map<String, ByteBuffer> createValue()
    {
        return new LinkedHashMap<String, ByteBuffer>();
    }

    /** CQL row iterator */
    private class RowIterator extends AbstractIterator<Pair<Map<String, ByteBuffer>, Map<String, ByteBuffer>>>
    {
        protected int totalRead = 0;             // total number of cf rows read
        protected Iterator<CqlRow> rows;
        private int pageRows = 0;                // the number of cql rows read of this page
        private String previousRowKey = null;    // previous CF row key
        private String partitionKeyString;       // keys in <key1>, <key2>, <key3> string format
        private String partitionKeyMarkers;      // question marks in ? , ? , ? format which matches the number of keys

        public RowIterator()
        {
            // initial page
            executeQuery();
        }

        protected Pair<Map<String, ByteBuffer>, Map<String, ByteBuffer>> computeNext()
        {
            if (rows == null)
                return endOfData();

            int index = -2;
            //check there are more page to read
            while (!rows.hasNext())
            {
                // no more data
                if (index == -1 || emptyPartitionKeyValues())
                {
                    logger.debug("no more data");
                    return endOfData();
                }

                index = setTailNull(clusterColumns);
                logger.debug("set tail to null, index: {}", index);
                executeQuery();
                pageRows = 0;

                if (rows == null || !rows.hasNext() && index < 0)
                {
                    logger.debug("no more data");
                    return endOfData();
                }
            }

            Map<String, ByteBuffer> valueColumns = createValue();
            Map<String, ByteBuffer> keyColumns = createKey();
            int i = 0;
            CqlRow row = rows.next();
            for (Column column : row.columns)
            {
                String columnName = stringValue(ByteBuffer.wrap(column.getName()));
                logger.debug("column: {}", columnName);

                if (i < partitionBoundColumns.size() + clusterColumns.size())
                    keyColumns.put(stringValue(column.name), column.value);
                else
                    valueColumns.put(stringValue(column.name), column.value);

                i++;
            }

            // increase total CQL row read for this page
            pageRows++;

            // increase total CF row read
            if (newRow(keyColumns, previousRowKey))
                totalRead++;

            // read full page
            if (pageRows >= pageRowSize || !rows.hasNext())
            {
                Iterator<String> newKeys = keyColumns.keySet().iterator();
                for (BoundColumn column : partitionBoundColumns)
                    column.value = keyColumns.get(newKeys.next());

                for (BoundColumn column : clusterColumns)
                    column.value = keyColumns.get(newKeys.next());

                executeQuery();
                pageRows = 0;
            }

            return Pair.create(keyColumns, valueColumns);
        }

        /** check whether start to read a new CF row by comparing the partition keys */
        private boolean newRow(Map<String, ByteBuffer> keyColumns, String previousRowKey)
        {
            if (keyColumns.isEmpty())
                return false;

            String rowKey = "";
            if (keyColumns.size() == 1)
            {
                rowKey = partitionBoundColumns.get(0).validator.getString(keyColumns.get(partitionBoundColumns.get(0).name));
            }
            else
            {
                Iterator<ByteBuffer> iter = keyColumns.values().iterator();
                for (BoundColumn column : partitionBoundColumns)
                    rowKey = rowKey + column.validator.getString(ByteBufferUtil.clone(iter.next())) + ":";
            }

            logger.debug("previous RowKey: {}, new row key: {}", previousRowKey, rowKey);
            if (previousRowKey == null)
            {
                this.previousRowKey = rowKey;
                return true;
            }

            if (rowKey.equals(previousRowKey))
                return false;

            this.previousRowKey = rowKey;
            return true;
        }

        /** set the last non-null key value to null, and return the previous index */
        private int setTailNull(List<BoundColumn> values)
        {
            if (values.isEmpty())
                return -1;

            Iterator<BoundColumn> iterator = values.iterator();
            int previousIndex = -1;
            BoundColumn current;
            while (iterator.hasNext())
            {
                current = iterator.next();
                if (current.value == null)
                {
                    int index = previousIndex > 0 ? previousIndex : 0;
                    BoundColumn column = values.get(index);
                    logger.debug("set key {} value to  null", column.name);
                    column.value = null;
                    return previousIndex - 1;
                }

                previousIndex++;
            }

            BoundColumn column = values.get(previousIndex);
            logger.debug("set key {} value to  null", column.name);
            column.value = null;
            return previousIndex - 1;
        }

        /** serialize the prepared query, pair.left is query id, pair.right is query */
        private Pair<Integer, String> composeQuery(String columns)
        {
            Pair<Integer, String> clause = whereClause();
            if (columns == null)
            {
                columns = "*";
            }
            else
            {
                // add keys in the front in order
                String partitionKey = keyString(partitionBoundColumns);
                String clusterKey = keyString(clusterColumns);

                columns = withoutKeyColumns(columns);
                columns = (clusterKey == null || "".equals(clusterKey))
                        ? partitionKey + "," + columns
                        : partitionKey + "," + clusterKey + "," + columns;
            }

            String whereStr = userDefinedWhereClauses == null ? "" : " AND " + userDefinedWhereClauses;
            return Pair.create(clause.left,
                               String.format("SELECT %s FROM %s%s%s LIMIT %d ALLOW FILTERING",
                                             columns, quote(cfName), clause.right, whereStr, pageRowSize));
        }


        /** remove key columns from the column string */
        private String withoutKeyColumns(String columnString)
        {
            Set<String> keyNames = new HashSet<String>();
            for (BoundColumn column : Iterables.concat(partitionBoundColumns, clusterColumns))
                keyNames.add(column.name);

            String[] columns = columnString.split(",");
            String result = null;
            for (String column : columns)
            {
                String trimmed = column.trim();
                if (keyNames.contains(trimmed))
                    continue;

                String quoted = quote(trimmed);
                result = result == null ? quoted : result + "," + quoted;
            }
            return result;
        }

        /** serialize the where clause */
        private Pair<Integer, String> whereClause()
        {
            if (partitionKeyString == null)
                partitionKeyString = keyString(partitionBoundColumns);

            if (partitionKeyMarkers == null)
                partitionKeyMarkers = partitionKeyMarkers();
            // initial query token(k) >= start_token and token(k) <= end_token
            if (emptyPartitionKeyValues())
                return Pair.create(0, String.format(" WHERE token(%s) > ? AND token(%s) <= ?", partitionKeyString, partitionKeyString));

            // query token(k) > token(pre_partition_key) and token(k) <= end_token
            if (clusterColumns.size() == 0 || clusterColumns.get(0).value == null)
                return Pair.create(1,
                                   String.format(" WHERE token(%s) > token(%s)  AND token(%s) <= ?",
                                                 partitionKeyString, partitionKeyMarkers, partitionKeyString));

            // query token(k) = token(pre_partition_key) and m = pre_cluster_key_m and n > pre_cluster_key_n
            Pair<Integer, String> clause = whereClause(clusterColumns, 0);
            return Pair.create(clause.left,
                               String.format(" WHERE token(%s) = token(%s) %s", partitionKeyString, partitionKeyMarkers, clause.right));
        }

        /** recursively serialize the where clause */
        private Pair<Integer, String> whereClause(List<BoundColumn> column, int position)
        {
            if (position == column.size() - 1 || column.get(position + 1).value == null)
                return Pair.create(position + 2, String.format(" AND %s > ? ", quote(column.get(position).name)));

            Pair<Integer, String> clause = whereClause(column, position + 1);
            return Pair.create(clause.left, String.format(" AND %s = ? %s", quote(column.get(position).name), clause.right));
        }

        /** check whether all key values are null */
        private boolean emptyPartitionKeyValues()
        {
            for (BoundColumn column : partitionBoundColumns)
            {
                if (column.value != null)
                    return false;
            }
            return true;
        }

        /** serialize the partition key string in format of <key1>, <key2>, <key3> */
        private String keyString(List<BoundColumn> columns)
        {
            String result = null;
            for (BoundColumn column : columns)
                result = result == null ? quote(column.name) : result + "," + quote(column.name);

            return result == null ? "" : result;
        }

        /** serialize the question marks for partition key string in format of ?, ? , ? */
        private String partitionKeyMarkers()
        {
            String result = null;
            for (BoundColumn column : partitionBoundColumns)
                result = result == null ? "?" : result + ",?";

            return result;
        }

        /** serialize the query binding variables, pair.left is query id, pair.right is the binding variables */
        private Pair<Integer, List<ByteBuffer>> preparedQueryBindValues()
        {
            List<ByteBuffer> values = new LinkedList<ByteBuffer>();

            // initial query token(k) >= start_token and token(k) <= end_token
            if (emptyPartitionKeyValues())
            {
                values.add(partitioner.getTokenValidator().fromString(split.getStartToken()));
                values.add(partitioner.getTokenValidator().fromString(split.getEndToken()));
                return Pair.create(0, values);
            }
            else
            {
                for (BoundColumn partitionBoundColumn1 : partitionBoundColumns)
                    values.add(partitionBoundColumn1.value);

                if (clusterColumns.size() == 0 || clusterColumns.get(0).value == null)
                {
                    // query token(k) > token(pre_partition_key) and token(k) <= end_token
                    values.add(partitioner.getTokenValidator().fromString(split.getEndToken()));
                    return Pair.create(1, values);
                }
                else
                {
                    // query token(k) = token(pre_partition_key) and m = pre_cluster_key_m and n > pre_cluster_key_n
                    int type = preparedQueryBindValues(clusterColumns, 0, values);
                    return Pair.create(type, values);
                }
            }
        }

        /** recursively serialize the query binding variables */
        private int preparedQueryBindValues(List<BoundColumn> column, int position, List<ByteBuffer> bindValues)
        {
            if (position == column.size() - 1 || column.get(position + 1).value == null)
            {
                bindValues.add(column.get(position).value);
                return position + 2;
            }
            else
            {
                bindValues.add(column.get(position).value);
                return preparedQueryBindValues(column, position + 1, bindValues);
            }
        }

        /**  get the prepared query item Id  */
        private int prepareQuery(int type) throws InvalidRequestException, TException
        {
            Integer itemId = preparedQueryIds.get(type);
            if (itemId != null)
                return itemId;

            Pair<Integer, String> query = null;
            query = composeQuery(columns);
            logger.debug("type: {}, query: {}", query.left, query.right);
            CqlPreparedResult cqlPreparedResult = client.prepare_cql3_query(ByteBufferUtil.bytes(query.right), Compression.NONE);
            preparedQueryIds.put(query.left, cqlPreparedResult.itemId);
            return cqlPreparedResult.itemId;
        }

        /** Quoting for working with uppercase */
        private String quote(String identifier)
        {
            return "\"" + identifier.replaceAll("\"", "\"\"") + "\"";
        }

        /** execute the prepared query */
        private void executeQuery()
        {
            Pair<Integer, List<ByteBuffer>> bindValues = preparedQueryBindValues();
            logger.debug("query type: {}", bindValues.left);

            // check whether it reach end of range for type 1 query CASSANDRA-5573
            if (bindValues.left == 1 && reachEndRange())
            {
                rows = null;
                return;
            }

            int retries = 0;
            // only try three times for TimedOutException and UnavailableException
            while (retries < 3)
            {
                try
                {
                    CqlResult cqlResult = client.execute_prepared_cql3_query(prepareQuery(bindValues.left), bindValues.right, consistencyLevel);
                    if (cqlResult != null && cqlResult.rows != null)
                        rows = cqlResult.rows.iterator();
                    return;
                }
                catch (TimedOutException e)
                {
                    retries++;
                    if (retries >= 3)
                    {
                        rows = null;
                        RuntimeException rte = new RuntimeException(e.getMessage());
                        rte.initCause(e);
                        throw rte;
                    }
                }
                catch (UnavailableException e)
                {
                    retries++;
                    if (retries >= 3)
                    {
                        rows = null;
                        RuntimeException rte = new RuntimeException(e.getMessage());
                        rte.initCause(e);
                        throw rte;
                    }
                }
                catch (Exception e)
                {
                    rows = null;
                    RuntimeException rte = new RuntimeException(e.getMessage());
                    rte.initCause(e);
                    throw rte;
                }
            }
        }
    }

    /** retrieve the partition keys and cluster keys from system.schema_columnfamilies table */
    private void retrieveKeys() throws Exception
    {
        String query = "select key_aliases," +
                       "column_aliases, " +
                       "key_validator, " +
                       "comparator " +
                       "from system.schema_columnfamilies " +
                       "where keyspace_name='%s' and columnfamily_name='%s'";
        String formatted = String.format(query, keyspace, cfName);
        CqlResult result = client.execute_cql3_query(ByteBufferUtil.bytes(formatted), Compression.NONE, ConsistencyLevel.ONE);

        CqlRow cqlRow = result.rows.get(0);
        String keyString = ByteBufferUtil.string(ByteBuffer.wrap(cqlRow.columns.get(0).getValue()));
        logger.debug("partition keys: {}", keyString);
        List<String> keys = FBUtilities.fromJsonList(keyString);

        for (String key : keys)
            partitionBoundColumns.add(new BoundColumn(key));

        keyString = ByteBufferUtil.string(ByteBuffer.wrap(cqlRow.columns.get(1).getValue()));
        logger.debug("cluster columns: {}", keyString);
        keys = FBUtilities.fromJsonList(keyString);

        for (String key : keys)
            clusterColumns.add(new BoundColumn(key));

        Column rawKeyValidator = cqlRow.columns.get(2);
        String validator = ByteBufferUtil.string(ByteBuffer.wrap(rawKeyValidator.getValue()));
        logger.debug("row key validator: {}", validator);
        keyValidator = parseType(validator);

        if (keyValidator instanceof CompositeType)
        {
            List<AbstractType<?>> types = ((CompositeType) keyValidator).types;
            for (int i = 0; i < partitionBoundColumns.size(); i++)
                partitionBoundColumns.get(i).validator = types.get(i);
        }
        else
        {
            partitionBoundColumns.get(0).validator = keyValidator;
        }
    }

    /** check whether current row is at the end of range */
    private boolean reachEndRange()
    {
        // current row key
        ByteBuffer rowKey;
        if (keyValidator instanceof CompositeType)
        {
            ByteBuffer[] keys = new ByteBuffer[partitionBoundColumns.size()];
            for (int i = 0; i < partitionBoundColumns.size(); i++)
                keys[i] = partitionBoundColumns.get(i).value.duplicate();

            rowKey = CompositeType.build(keys);
        }
        else
        {
            rowKey = partitionBoundColumns.get(0).value;
        }

        String endToken = split.getEndToken();
        String currentToken = partitioner.getToken(rowKey).toString();
        logger.debug("End token: {}, current token: {}", endToken, currentToken);

        return endToken.equals(currentToken);
    }

    private static AbstractType<?> parseType(String type) throws IOException
    {
        try
        {
            // always treat counters like longs, specifically CCT.serialize is not what we need
            if (type != null && type.equals("org.apache.cassandra.db.marshal.CounterColumnType"))
                return LongType.instance;
            return TypeParser.parse(type);
        }
        catch (ConfigurationException e)
        {
            throw new IOException(e);
        }
        catch (SyntaxException e)
        {
            throw new IOException(e);
        }
    }

    private static class BoundColumn
    {
        final String name;
        ByteBuffer value;
        AbstractType<?> validator;

        public BoundColumn(String name)
        {
            this.name = name;
        }
    }
    
    /** get string from a ByteBuffer, catch the exception and throw it as runtime exception*/
    private static String stringValue(ByteBuffer value)
    {
        try
        {
            return ByteBufferUtil.string(value);
        }
        catch (CharacterCodingException e)
        {
            throw new RuntimeException(e);
        }
    }
}
