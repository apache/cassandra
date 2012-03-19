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
package org.apache.cassandra.hadoop;


import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.thrift.Mutation;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.*;

public class BulkOutputFormat extends OutputFormat<ByteBuffer,List<Mutation>>
    implements org.apache.hadoop.mapred.OutputFormat<ByteBuffer,List<Mutation>>
{
    private static final Logger logger = LoggerFactory.getLogger(BulkOutputFormat.class);

    @Override
    public void checkOutputSpecs(JobContext context)
    {
        checkOutputSpecs(context.getConfiguration());
    }

    private void checkOutputSpecs(Configuration conf)
    {
        if (ConfigHelper.getOutputKeyspace(conf) == null || ConfigHelper.getOutputColumnFamily(conf) == null)
        {
            throw new UnsupportedOperationException("you must set the keyspace and columnfamily with setColumnFamily()");
        }
    }

    @Override
    public OutputCommitter getOutputCommitter(TaskAttemptContext context) throws IOException, InterruptedException
    {
        return new NullOutputCommitter();
    }

    /** Fills the deprecated OutputFormat interface for streaming. */
    @Deprecated
    public void checkOutputSpecs(org.apache.hadoop.fs.FileSystem filesystem, org.apache.hadoop.mapred.JobConf job) throws IOException
    {
        checkOutputSpecs(job);
    }

    /** Fills the deprecated OutputFormat interface for streaming. */
    @Deprecated
    public BulkRecordWriter getRecordWriter(org.apache.hadoop.fs.FileSystem filesystem, org.apache.hadoop.mapred.JobConf job, String name, org.apache.hadoop.util.Progressable progress) throws IOException
    {
        return new BulkRecordWriter(job, new Progressable(progress));
    }

    @Override
    public BulkRecordWriter getRecordWriter(final TaskAttemptContext context) throws IOException, InterruptedException
    {
        return new BulkRecordWriter(context);
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
