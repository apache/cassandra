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
import java.util.Map;

import org.apache.cassandra.hadoop.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.mapreduce.*;

/**
 * The <code>CqlOutputFormat</code> acts as a Hadoop-specific
 * OutputFormat that allows reduce tasks to store keys (and corresponding
 * bound variable values) as CQL rows (and respective columns) in a given
 * table.
 *
 * <p>
 * As is the case with the {@link org.apache.cassandra.hadoop.ColumnFamilyInputFormat}, 
 * you need to set the prepared statement in your
 * Hadoop job Configuration. The {@link CqlConfigHelper} class, through its
 * {@link CqlConfigHelper#setOutputCql} method, is provided to make this
 * simple.
 * you need to set the Keyspace. The {@link ConfigHelper} class, through its
 * {@link ConfigHelper#setOutputColumnFamily} method, is provided to make this
 * simple.
 * </p>
 * 
 * <p>
 * For the sake of performance, this class employs a lazy write-back caching
 * mechanism, where its record writer prepared statement binded variable values
 * created based on the reduce's inputs (in a task-specific map), and periodically 
 * makes the changes official by sending a execution of prepared statement request 
 * to Cassandra.
 * </p>
 */
public class CqlOutputFormat extends OutputFormat<Map<String, ByteBuffer>, List<ByteBuffer>>
        implements org.apache.hadoop.mapred.OutputFormat<Map<String, ByteBuffer>, List<ByteBuffer>>
{
    public static final String BATCH_THRESHOLD = "mapreduce.output.columnfamilyoutputformat.batch.threshold";
    public static final String QUEUE_SIZE = "mapreduce.output.columnfamilyoutputformat.queue.size";

    /**
     * Check for validity of the output-specification for the job.
     *
     * @param context
     *            information about the job
     */
    public void checkOutputSpecs(JobContext context)
    {
        checkOutputSpecs(HadoopCompat.getConfiguration(context));
    }

    protected void checkOutputSpecs(Configuration conf)
    {
        if (ConfigHelper.getOutputKeyspace(conf) == null)
            throw new UnsupportedOperationException("You must set the keyspace with setOutputKeyspace()");
        if (ConfigHelper.getOutputPartitioner(conf) == null)
            throw new UnsupportedOperationException("You must set the output partitioner to the one used by your Cassandra cluster");
        if (ConfigHelper.getOutputInitialAddress(conf) == null)
            throw new UnsupportedOperationException("You must set the initial output address to a Cassandra node");
    }

    /** Fills the deprecated OutputFormat interface for streaming. */
    @Deprecated
    public void checkOutputSpecs(org.apache.hadoop.fs.FileSystem filesystem, org.apache.hadoop.mapred.JobConf job) throws IOException
    {
        checkOutputSpecs(job);
    }

    /**
     * The OutputCommitter for this format does not write any data to the DFS.
     *
     * @param context
     *            the task context
     * @return an output committer
     * @throws IOException
     * @throws InterruptedException
     */
    public OutputCommitter getOutputCommitter(TaskAttemptContext context) throws IOException, InterruptedException
    {
        return new NullOutputCommitter();
    }

    /** Fills the deprecated OutputFormat interface for streaming. */
    @Deprecated
    public CqlRecordWriter getRecordWriter(org.apache.hadoop.fs.FileSystem filesystem, org.apache.hadoop.mapred.JobConf job, String name, org.apache.hadoop.util.Progressable progress) throws IOException
    {
        return new CqlRecordWriter(job, progress);
    }

    /**
     * Get the {@link RecordWriter} for the given task.
     *
     * @param context
     *            the information about the current task.
     * @return a {@link RecordWriter} to write the output for the job.
     * @throws IOException
     */
    public CqlRecordWriter getRecordWriter(final TaskAttemptContext context) throws IOException, InterruptedException
    {
        return new CqlRecordWriter(context);
    }

    /**
     * An {@link OutputCommitter} that does nothing.
     */
    private static class NullOutputCommitter extends OutputCommitter
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
