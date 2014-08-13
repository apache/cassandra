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

import org.apache.cassandra.hadoop.AbstractColumnFamilyOutputFormat;
import org.apache.cassandra.hadoop.ConfigHelper;
import org.apache.hadoop.mapreduce.*;

/**
 * The <code>ColumnFamilyOutputFormat</code> acts as a Hadoop-specific
 * OutputFormat that allows reduce tasks to store keys (and corresponding
 * bound variable values) as CQL rows (and respective columns) in a given
 * ColumnFamily.
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
public class CqlOutputFormat extends AbstractColumnFamilyOutputFormat<Map<String, ByteBuffer>, List<ByteBuffer>>
{   
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
}
