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
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.auth.IAuthenticator;
import org.apache.cassandra.thrift.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.*;
import org.apache.thrift.TException;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;

/**
 * The <code>ColumnFamilyOutputFormat</code> acts as a Hadoop-specific
 * OutputFormat that allows reduce tasks to store keys (and corresponding
 * values) as Cassandra rows (and respective columns) in a given
 * ColumnFamily.
 * 
 * <p>
 * As is the case with the {@link ColumnFamilyInputFormat}, you need to set the
 * Keyspace and ColumnFamily in your
 * Hadoop job Configuration. The {@link ConfigHelper} class, through its
 * {@link ConfigHelper#setOutputColumnFamily} method, is provided to make this
 * simple.
 * </p>
 * 
 * <p>
 * For the sake of performance, this class employs a lazy write-back caching
 * mechanism, where its record writer batches mutations created based on the
 * reduce's inputs (in a task-specific map), and periodically makes the changes
 * official by sending a batch mutate request to Cassandra.
 * </p>
 */
public class ColumnFamilyOutputFormat extends OutputFormat<ByteBuffer,List<Mutation>>
    implements org.apache.hadoop.mapred.OutputFormat<ByteBuffer,List<Mutation>>
{
    private static final Logger logger = LoggerFactory.getLogger(ColumnFamilyOutputFormat.class);
    
    public static final String BATCH_THRESHOLD = "mapreduce.output.columnfamilyoutputformat.batch.threshold";
    public static final String QUEUE_SIZE = "mapreduce.output.columnfamilyoutputformat.queue.size";

    /**
     * Check for validity of the output-specification for the job.
     * 
     * @param context
     *            information about the job
     * @throws IOException
     *             when output should not be attempted
     */
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

    /**
     * The OutputCommitter for this format does not write any data to the DFS.
     * 
     * @param context
     *            the task context
     * @return an output committer
     * @throws IOException
     * @throws InterruptedException
     */
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
    @Deprecated @Override
    public ColumnFamilyRecordWriter getRecordWriter(org.apache.hadoop.fs.FileSystem filesystem, org.apache.hadoop.mapred.JobConf job, String name, org.apache.hadoop.util.Progressable progress) throws IOException
    {
        return new ColumnFamilyRecordWriter(job);
    }

    /**
     * Get the {@link RecordWriter} for the given task.
     * 
     * @param context
     *            the information about the current task.
     * @return a {@link RecordWriter} to write the output for the job.
     * @throws IOException
     */
    @Override
    public ColumnFamilyRecordWriter getRecordWriter(final TaskAttemptContext context) throws IOException, InterruptedException
    {
        return new ColumnFamilyRecordWriter(context);
    }

    /**
     * Return a client based on the given socket that points to the configured
     * keyspace, and is logged in with the configured credentials.
     *
     * @param socket  a socket pointing to a particular node, seed or otherwise
     * @param conf a job configuration
     * @return a cassandra client
     * @throws InvalidRequestException
     * @throws TException
     * @throws AuthenticationException
     * @throws AuthorizationException
     */
    public static Cassandra.Client createAuthenticatedClient(TSocket socket, Configuration conf)
    throws InvalidRequestException, TException, AuthenticationException, AuthorizationException
    {
        TBinaryProtocol binaryProtocol = new TBinaryProtocol(new TFramedTransport(socket));
        Cassandra.Client client = new Cassandra.Client(binaryProtocol);
        socket.open();
        client.set_keyspace(ConfigHelper.getOutputKeyspace(conf));
        if (ConfigHelper.getOutputKeyspaceUserName(conf) != null)
        {
            Map<String, String> creds = new HashMap<String, String>();
            creds.put(IAuthenticator.USERNAME_KEY, ConfigHelper.getOutputKeyspaceUserName(conf));
            creds.put(IAuthenticator.PASSWORD_KEY, ConfigHelper.getOutputKeyspacePassword(conf));
            AuthenticationRequest authRequest = new AuthenticationRequest(creds);
            client.login(authRequest);
        }
        return client;
    }

    /**
     * An {@link OutputCommitter} that does nothing.
     */
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
