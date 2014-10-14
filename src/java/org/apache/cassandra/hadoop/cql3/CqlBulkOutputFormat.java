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
import java.util.List;

import org.apache.cassandra.hadoop.AbstractBulkOutputFormat;
import org.apache.cassandra.hadoop.ConfigHelper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.util.Progressable;

/**
 * The <code>CqlBulkOutputFormat</code> acts as a Hadoop-specific
 * OutputFormat that allows reduce tasks to store keys (and corresponding
 * bound variable values) as CQL rows (and respective columns) in a given
 * ColumnFamily.
 *
 * <p>
 * As is the case with the {@link org.apache.cassandra.hadoop.CqlOutputFormat}, 
 * you need to set the prepared statement in your
 * Hadoop job Configuration. The {@link CqlConfigHelper} class, through its
 * {@link ConfigHelper#setOutputPreparedStatement} method, is provided to make this
 * simple.
 * you need to set the Keyspace. The {@link ConfigHelper} class, through its
 * {@link ConfigHelper#setOutputColumnFamily} method, is provided to make this
 * simple.
 * </p>
 */
public class CqlBulkOutputFormat extends AbstractBulkOutputFormat<Object, List<ByteBuffer>>
{   
  
    private static final String OUTPUT_CQL_SCHEMA_PREFIX = "cassandra.columnfamily.schema.";
    private static final String OUTPUT_CQL_INSERT_PREFIX = "cassandra.columnfamily.insert.";
    private static final String DELETE_SOURCE = "cassandra.output.delete.source";
  
    /** Fills the deprecated OutputFormat interface for streaming. */
    @Deprecated
    public CqlBulkRecordWriter getRecordWriter(FileSystem filesystem, JobConf job, String name, Progressable progress) throws IOException
    {
        return new CqlBulkRecordWriter(job, progress);
    }

    /**
     * Get the {@link RecordWriter} for the given task.
     *
     * @param context
     *            the information about the current task.
     * @return a {@link RecordWriter} to write the output for the job.
     * @throws IOException
     */
    public CqlBulkRecordWriter getRecordWriter(final TaskAttemptContext context) throws IOException, InterruptedException
    {
        return new CqlBulkRecordWriter(context);
    }
    
    public static void setColumnFamilySchema(Configuration conf, String columnFamily, String schema)
    {
        conf.set(OUTPUT_CQL_SCHEMA_PREFIX + columnFamily, schema);
    }

    public static void setColumnFamilyInsertStatement(Configuration conf, String columnFamily, String insertStatement)
    {
        conf.set(OUTPUT_CQL_INSERT_PREFIX + columnFamily, insertStatement);
    }
    
    public static String getColumnFamilySchema(Configuration conf, String columnFamily)
    {
        String schema = conf.get(OUTPUT_CQL_SCHEMA_PREFIX + columnFamily);
        if (schema == null)
        { 
            throw new UnsupportedOperationException("You must set the ColumnFamily schema using setColumnFamilySchema.");
        }
        return schema; 
    }
    
    public static String getColumnFamilyInsertStatement(Configuration conf, String columnFamily)
    {
        String insert = conf.get(OUTPUT_CQL_INSERT_PREFIX + columnFamily); 
        if (insert == null)
        {
            throw new UnsupportedOperationException("You must set the ColumnFamily insert statement using setColumnFamilySchema.");
        }
        return insert;
    }
    
    public static void setDeleteSourceOnSuccess(Configuration conf, boolean deleteSrc)
    {
        conf.setBoolean(DELETE_SOURCE, deleteSrc);
    }
    
    public static boolean getDeleteSourceOnSuccess(Configuration conf)
    {
        return conf.getBoolean(DELETE_SOURCE, false);
    }
}
