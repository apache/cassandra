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
import java.util.Collection;
import java.util.List;

import org.apache.cassandra.hadoop.ConfigHelper;
import org.apache.cassandra.hadoop.HadoopCompat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.util.Progressable;

/**
 * The <code>CqlBulkOutputFormat</code> acts as a Hadoop-specific
 * OutputFormat that allows reduce tasks to store keys (and corresponding
 * bound variable values) as CQL rows (and respective columns) in a given
 * table.
 *
 * <p>
 * As is the case with the {@link org.apache.cassandra.hadoop.cql3.CqlOutputFormat}, 
 * you need to set the prepared statement in your
 * Hadoop job Configuration. The {@link CqlConfigHelper} class, through its
 * {@link ConfigHelper#setOutputPreparedStatement} method, is provided to make this
 * simple.
 * you need to set the Keyspace. The {@link ConfigHelper} class, through its
 * {@link ConfigHelper#setOutputColumnFamily} method, is provided to make this
 * simple.
 * </p>
 */
public class CqlBulkOutputFormat extends OutputFormat<Object, List<ByteBuffer>>
        implements org.apache.hadoop.mapred.OutputFormat<Object, List<ByteBuffer>>
{   
  
    private static final String OUTPUT_CQL_SCHEMA_PREFIX = "cassandra.table.schema.";
    private static final String OUTPUT_CQL_INSERT_PREFIX = "cassandra.table.insert.";
    private static final String DELETE_SOURCE = "cassandra.output.delete.source";
    private static final String TABLE_ALIAS_PREFIX = "cqlbulkoutputformat.table.alias.";
  
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

    @Override
    public void checkOutputSpecs(JobContext context)
    {
        checkOutputSpecs(HadoopCompat.getConfiguration(context));
    }

    private void checkOutputSpecs(Configuration conf)
    {
        if (ConfigHelper.getOutputKeyspace(conf) == null)
        {
            throw new UnsupportedOperationException("you must set the keyspace with setTable()");
        }
    }

    /** Fills the deprecated OutputFormat interface for streaming. */
    @Deprecated
    public void checkOutputSpecs(org.apache.hadoop.fs.FileSystem filesystem, org.apache.hadoop.mapred.JobConf job) throws IOException
    {
        checkOutputSpecs(job);
    }

    @Override
    public OutputCommitter getOutputCommitter(TaskAttemptContext context) throws IOException, InterruptedException
    {
        return new NullOutputCommitter();
    }
    
    public static void setTableSchema(Configuration conf, String columnFamily, String schema)
    {
        conf.set(OUTPUT_CQL_SCHEMA_PREFIX + columnFamily, schema);
    }

    public static void setTableInsertStatement(Configuration conf, String columnFamily, String insertStatement)
    {
        conf.set(OUTPUT_CQL_INSERT_PREFIX + columnFamily, insertStatement);
    }
    
    public static String getTableSchema(Configuration conf, String columnFamily)
    {
        String schema = conf.get(OUTPUT_CQL_SCHEMA_PREFIX + columnFamily);
        if (schema == null)
        { 
            throw new UnsupportedOperationException("You must set the Table schema using setTableSchema.");
        }
        return schema; 
    }
    
    public static String getTableInsertStatement(Configuration conf, String columnFamily)
    {
        String insert = conf.get(OUTPUT_CQL_INSERT_PREFIX + columnFamily); 
        if (insert == null)
        {
            throw new UnsupportedOperationException("You must set the Table insert statement using setTableSchema.");
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
    
    public static void setTableAlias(Configuration conf, String alias, String columnFamily)
    {
        conf.set(TABLE_ALIAS_PREFIX + alias, columnFamily);
    }
    
    public static String getTableForAlias(Configuration conf, String alias)
    {
        return conf.get(TABLE_ALIAS_PREFIX + alias);
    }

    /**
     * Set the hosts to ignore as comma delimited values.
     * Data will not be bulk loaded onto the ignored nodes.
     * @param conf job configuration
     * @param ignoreNodesCsv a comma delimited list of nodes to ignore
     */
    public static void setIgnoreHosts(Configuration conf, String ignoreNodesCsv)
    {
        conf.set(CqlBulkRecordWriter.IGNORE_HOSTS, ignoreNodesCsv);
    }

    /**
     * Set the hosts to ignore. Data will not be bulk loaded onto the ignored nodes.
     * @param conf job configuration
     * @param ignoreNodes the nodes to ignore
     */
    public static void setIgnoreHosts(Configuration conf, String... ignoreNodes)
    {
        conf.setStrings(CqlBulkRecordWriter.IGNORE_HOSTS, ignoreNodes);
    }

    /**
     * Get the hosts to ignore as a collection of strings
     * @param conf job configuration
     * @return the nodes to ignore as a collection of stirngs
     */
    public static Collection<String> getIgnoreHosts(Configuration conf)
    {
        return conf.getStringCollection(CqlBulkRecordWriter.IGNORE_HOSTS);
    }

    public static class NullOutputCommitter extends OutputCommitter
    {
        public void abortTask(TaskAttemptContext taskContext) { }

        public void cleanupJob(JobContext jobContext) { }

        public void commitTask(TaskAttemptContext taskContext) { }

        public boolean needsTaskCommit(TaskAttemptContext taskContext)
        {
            return false;
        }

        public void setupJob(JobContext jobContext) { }

        public void setupTask(TaskAttemptContext taskContext) { }
    }
}
