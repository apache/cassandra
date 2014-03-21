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
import java.nio.ByteBuffer;
import java.util.*;

import com.google.common.collect.AbstractIterator;
import com.google.common.collect.Maps;

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

    // partition keys -- key aliases
    private LinkedHashMap<String, Boolean> partitionBoundColumns = Maps.newLinkedHashMap();

    public CqlRecordReader()
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
        keyspace = ConfigHelper.getInputKeyspace(conf);              
        cqlQuery = CqlConfigHelper.getInputCql(conf);
        partitioner = ConfigHelper.getInputPartitioner(context.getConfiguration());
        try
        {
            if (cluster != null)
                return;

            // create connection using thrift
            String[] locations = split.getLocations();
            Exception lastException = null;
            for (String location : locations)
            {
                try
                {
                    cluster = CqlConfigHelper.getInputCluster(location, conf);
                    break;
                }
                catch (Exception e)
                {
                    lastException = e;
                    logger.warn("Failed to create authenticated client to {}", location);
                }
            }
            if (cluster == null && lastException != null)
                throw lastException;
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }

        session = cluster.connect(keyspace);
        rowIterator = new RowIterator();
        logger.debug("created {}", rowIterator);
    }

    public void close()
    {
        if (session != null)
            session.close();
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
            key = getCurrentKey();
            value = getCurrentValue();
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
        return null;
    }

    public Row createValue()
    {
        return null;
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
            for (ColumnMetadata meta : cluster.getMetadata().getKeyspace(keyspace).getTable(cfName).getPartitionKey())
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
}
