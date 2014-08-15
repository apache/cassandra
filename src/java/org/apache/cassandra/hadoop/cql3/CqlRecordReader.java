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
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.*;

import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Optional;
import com.google.common.base.Splitter;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;

import org.apache.cassandra.hadoop.HadoopCompat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.hadoop.ColumnFamilySplit;
import org.apache.cassandra.hadoop.ConfigHelper;
import org.apache.cassandra.utils.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.ColumnMetadata;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
/**
 * CqlRecordReader reads the rows return from the CQL query
 * It uses CQL auto-paging.
 * <p/>
 * Return a Long as a local CQL row key starts from 0;
 * <p/>
 * Row as C* java driver CQL result set row
 * 1) select clause must include partition key columns (to calculate the progress based on the actual CF row processed)
 * 2) where clause must include token(partition_key1, ...  , partition_keyn) > ? and 
 *       token(partition_key1, ... , partition_keyn) <= ?  (in the right order) 
 */
public class CqlRecordReader extends RecordReader<Long, Row>
        implements org.apache.hadoop.mapred.RecordReader<Long, Row>
{
    private static final Logger logger = LoggerFactory.getLogger(CqlRecordReader.class);

    public static final int DEFAULT_CQL_PAGE_LIMIT = 1000;

    private ColumnFamilySplit split;
    private RowIterator rowIterator;

    private Pair<Long, Row> currentRow;
    private int totalRowCount; // total number of rows to fetch
    private String keyspace;
    private String cfName;
    private String cqlQuery;
    private Cluster cluster;
    private Session session;
    private IPartitioner partitioner;
    private String inputColumns;
    private String userDefinedWhereClauses;
    private int pageRowSize;

    private List<String> partitionKeys = new ArrayList<>();

    // partition keys -- key aliases
    private LinkedHashMap<String, Boolean> partitionBoundColumns = Maps.newLinkedHashMap();

    public CqlRecordReader()
    {
        super();
    }

    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException
    {
        this.split = (ColumnFamilySplit) split;
        Configuration conf = HadoopCompat.getConfiguration(context);
        totalRowCount = (this.split.getLength() < Long.MAX_VALUE)
                      ? (int) this.split.getLength()
                      : ConfigHelper.getInputSplitSize(conf);
        cfName = ConfigHelper.getInputColumnFamily(conf);
        keyspace = ConfigHelper.getInputKeyspace(conf);
        partitioner = ConfigHelper.getInputPartitioner(conf);
        inputColumns = CqlConfigHelper.getInputcolumns(conf);
        userDefinedWhereClauses = CqlConfigHelper.getInputWhereClauses(conf);
        Optional<Integer> pageRowSizeOptional = CqlConfigHelper.getInputPageRowSize(conf);
        try
        {
            pageRowSize = pageRowSizeOptional.isPresent() ? pageRowSizeOptional.get() : DEFAULT_CQL_PAGE_LIMIT;
        }
        catch(NumberFormatException e)
        {
            pageRowSize = DEFAULT_CQL_PAGE_LIMIT;
        }
        try
        {
            if (cluster != null)
                return;

            // create a Cluster instance
            String[] locations = split.getLocations();
            cluster = CqlConfigHelper.getInputCluster(locations, conf);
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }

        if (cluster != null)
            session = cluster.connect(quote(keyspace));

        if (session == null)
          throw new RuntimeException("Can't create connection session");

        // If the user provides a CQL query then we will use it without validation
        // otherwise we will fall back to building a query using the:
        //   inputColumns
        //   whereClauses
        //   pageRowSize
        cqlQuery = CqlConfigHelper.getInputCql(conf);
        // validate that the user hasn't tried to give us a custom query along with input columns
        // and where clauses
        if (StringUtils.isNotEmpty(cqlQuery) && (StringUtils.isNotEmpty(inputColumns) ||
                                                 StringUtils.isNotEmpty(userDefinedWhereClauses)))
        {
            throw new AssertionError("Cannot define a custom query with input columns and / or where clauses");
        }

        if (StringUtils.isEmpty(cqlQuery))
            cqlQuery = buildQuery();
        logger.debug("cqlQuery {}", cqlQuery);

        rowIterator = new RowIterator();
        logger.debug("created {}", rowIterator);
    }

    public void close()
    {
        if (session != null)
            session.close();
        if (cluster != null)
            cluster.close();
    }

    public Long getCurrentKey()
    {
        return currentRow.left;
    }

    public Row getCurrentValue()
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

    // Because the old Hadoop API wants us to write to the key and value
    // and the new asks for them, we need to copy the output of the new API
    // to the old. Thus, expect a small performance hit.
    // And obviously this wouldn't work for wide rows. But since ColumnFamilyInputFormat
    // and ColumnFamilyRecordReader don't support them, it should be fine for now.
    public boolean next(Long key, Row value) throws IOException
    {
        if (nextKeyValue())
        {
            ((WrappedRow)value).setRow(getCurrentValue());
            return true;
        }
        return false;
    }

    public long getPos() throws IOException
    {
        return (long) rowIterator.totalRead;
    }

    public Long createKey()
    {
        return new Long(0L);
    }

    public Row createValue()
    {
        return new WrappedRow();
    }

    /** CQL row iterator 
     *  Input cql query  
     *  1) select clause must include key columns (if we use partition key based row count)
     *  2) where clause must include token(partition_key1 ... partition_keyn) > ? and 
     *     token(partition_key1 ... partition_keyn) <= ? 
     */
    private class RowIterator extends AbstractIterator<Pair<Long, Row>>
    {
        private long keyId = 0L;
        protected int totalRead = 0; // total number of cf rows read
        protected Iterator<Row> rows;
        private Map<String, ByteBuffer> previousRowKey = new HashMap<String, ByteBuffer>(); // previous CF row key

        public RowIterator()
        {
            AbstractType type = partitioner.getTokenValidator();
            ResultSet rs = session.execute(cqlQuery, type.compose(type.fromString(split.getStartToken())), type.compose(type.fromString(split.getEndToken())) );
            for (ColumnMetadata meta : cluster.getMetadata().getKeyspace(quote(keyspace)).getTable(quote(cfName)).getPartitionKey())
                partitionBoundColumns.put(meta.getName(), Boolean.TRUE);
            rows = rs.iterator();
        }

        protected Pair<Long, Row> computeNext()
        {
            if (rows == null || !rows.hasNext())
                return endOfData();

            Row row = rows.next();
            Map<String, ByteBuffer> keyColumns = new HashMap<String, ByteBuffer>(); 
            for (String column : partitionBoundColumns.keySet())
                keyColumns.put(column, row.getBytesUnsafe(column));

            // increase total CF row read
            if (previousRowKey.isEmpty() && !keyColumns.isEmpty())
            {
                previousRowKey = keyColumns;
                totalRead++;
            }
            else
            {
                for (String column : partitionBoundColumns.keySet())
                {
                    if (BytesType.bytesCompare(keyColumns.get(column), previousRowKey.get(column)) != 0)
                    {
                        previousRowKey = keyColumns;
                        totalRead++;
                        break;
                    }
                }
            }
            keyId ++;
            return Pair.create(keyId, row);
        }
    }

    private static class WrappedRow implements Row
    {
        private Row row;

        public void setRow(Row row)
        {
            this.row = row;
        }

        @Override
        public ColumnDefinitions getColumnDefinitions()
        {
            return row.getColumnDefinitions();
        }

        @Override
        public boolean isNull(int i)
        {
            return row.isNull(i);
        }

        @Override
        public boolean isNull(String name)
        {
            return row.isNull(name);
        }

        @Override
        public boolean getBool(int i)
        {
            return row.getBool(i);
        }

        @Override
        public boolean getBool(String name)
        {
            return row.getBool(name);
        }

        @Override
        public int getInt(int i)
        {
            return row.getInt(i);
        }

        @Override
        public int getInt(String name)
        {
            return row.getInt(name);
        }

        @Override
        public long getLong(int i)
        {
            return row.getLong(i);
        }

        @Override
        public long getLong(String name)
        {
            return row.getLong(name);
        }

        @Override
        public Date getDate(int i)
        {
            return row.getDate(i);
        }

        @Override
        public Date getDate(String name)
        {
            return row.getDate(name);
        }

        @Override
        public float getFloat(int i)
        {
            return row.getFloat(i);
        }

        @Override
        public float getFloat(String name)
        {
            return row.getFloat(name);
        }

        @Override
        public double getDouble(int i)
        {
            return row.getDouble(i);
        }

        @Override
        public double getDouble(String name)
        {
            return row.getDouble(name);
        }

        @Override
        public ByteBuffer getBytesUnsafe(int i)
        {
            return row.getBytesUnsafe(i);
        }

        @Override
        public ByteBuffer getBytesUnsafe(String name)
        {
            return row.getBytesUnsafe(name);
        }

        @Override
        public ByteBuffer getBytes(int i)
        {
            return row.getBytes(i);
        }

        @Override
        public ByteBuffer getBytes(String name)
        {
            return row.getBytes(name);
        }

        @Override
        public String getString(int i)
        {
            return row.getString(i);
        }

        @Override
        public String getString(String name)
        {
            return row.getString(name);
        }

        @Override
        public BigInteger getVarint(int i)
        {
            return row.getVarint(i);
        }

        @Override
        public BigInteger getVarint(String name)
        {
            return row.getVarint(name);
        }

        @Override
        public BigDecimal getDecimal(int i)
        {
            return row.getDecimal(i);
        }

        @Override
        public BigDecimal getDecimal(String name)
        {
            return row.getDecimal(name);
        }

        @Override
        public UUID getUUID(int i)
        {
            return row.getUUID(i);
        }

        @Override
        public UUID getUUID(String name)
        {
            return row.getUUID(name);
        }

        @Override
        public InetAddress getInet(int i)
        {
            return row.getInet(i);
        }

        @Override
        public InetAddress getInet(String name)
        {
            return row.getInet(name);
        }

        @Override
        public <T> List<T> getList(int i, Class<T> elementsClass)
        {
            return row.getList(i, elementsClass);
        }

        @Override
        public <T> List<T> getList(String name, Class<T> elementsClass)
        {
            return row.getList(name, elementsClass);
        }

        @Override
        public <T> Set<T> getSet(int i, Class<T> elementsClass)
        {
            return row.getSet(i, elementsClass);
        }

        @Override
        public <T> Set<T> getSet(String name, Class<T> elementsClass)
        {
            return row.getSet(name, elementsClass);
        }

        @Override
        public <K, V> Map<K, V> getMap(int i, Class<K> keysClass, Class<V> valuesClass)
        {
            return row.getMap(i, keysClass, valuesClass);
        }

        @Override
        public <K, V> Map<K, V> getMap(String name, Class<K> keysClass, Class<V> valuesClass)
        {
            return row.getMap(name, keysClass, valuesClass);
        }
    }

    /**
     * Build a query for the reader of the form:
     *
     * SELECT * FROM ks>cf token(pk1,...pkn)>? AND token(pk1,...pkn)<=? [AND user where clauses]
     * LIMIT pageRowSize [ALLOW FILTERING]
     */
    private String buildQuery()
    {
        fetchKeys();

        List<String> columns = getSelectColumns();
        String selectColumnList = columns.size() == 0 ? "*" : makeColumnList(columns);
        String partitionKeyList = makeColumnList(partitionKeys);

        return String.format("SELECT %s FROM %s.%s WHERE token(%s)>? AND token(%s)<=?" + getAdditionalWhereClauses(),
                             selectColumnList, quote(keyspace), quote(cfName), partitionKeyList, partitionKeyList);
    }

    private String getAdditionalWhereClauses()
    {
        String whereClause = "";
        if (StringUtils.isNotEmpty(userDefinedWhereClauses))
            whereClause += " AND " + userDefinedWhereClauses;
        whereClause += " LIMIT " + pageRowSize;
        if (StringUtils.isNotEmpty(userDefinedWhereClauses))
            whereClause += " ALLOW FILTERING";
        return whereClause;
    }

    private List<String> getSelectColumns()
    {
        List<String> selectColumns = new ArrayList<>();

        if (StringUtils.isNotEmpty(inputColumns))
        {
            // We must select all the partition keys plus any other columns the user wants
            selectColumns.addAll(partitionKeys);
            for (String column : Splitter.on(',').split(inputColumns))
            {
                if (!partitionKeys.contains(column))
                    selectColumns.add(column);
            }
        }
        return selectColumns;
    }

    private String makeColumnList(Collection<String> columns)
    {
        return Joiner.on(',').join(Iterables.transform(columns, new Function<String, String>()
        {
            public String apply(String column)
            {
                return quote(column);
            }
        }));
    }

    private void fetchKeys()
    {
        String query = "SELECT column_name, component_index, type FROM system.schema_columns WHERE keyspace_name='" +
                       keyspace + "' and columnfamily_name='" + cfName + "'";
        List<Row> rows = session.execute(query).all();
        if (CollectionUtils.isEmpty(rows))
        {
            throw new RuntimeException("No table metadata found for " + keyspace + "." + cfName);
        }
        int numberOfPartitionKeys = 0;
        for (Row row : rows)
            if (row.getString(2).equals("partition_key"))
                numberOfPartitionKeys++;
        String[] partitionKeyArray = new String[numberOfPartitionKeys];
        for (Row row : rows)
        {
            String type = row.getString(2);
            String column = row.getString(0);
            if (type.equals("partition_key"))
            {
                int componentIndex = row.isNull(1) ? 0 : row.getInt(1);
                partitionKeyArray[componentIndex] = column;
            }
        }
        partitionKeys.addAll(Arrays.asList(partitionKeyArray));
    }

    private String quote(String identifier)
    {
        return "\"" + identifier.replaceAll("\"", "\"\"") + "\"";
    }
}
