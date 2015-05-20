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


import java.io.*;
import java.nio.ByteBuffer;
import java.util.*;

import org.slf4j.*;

import org.apache.cassandra.auth.*;
import org.apache.cassandra.thrift.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.thrift.protocol.*;
import org.apache.thrift.transport.*;

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
@Deprecated
public class ColumnFamilyOutputFormat extends OutputFormat<ByteBuffer,List<Mutation>>
        implements org.apache.hadoop.mapred.OutputFormat<ByteBuffer,List<Mutation>>
{
    public static final String BATCH_THRESHOLD = "mapreduce.output.columnfamilyoutputformat.batch.threshold";
    public static final String QUEUE_SIZE = "mapreduce.output.columnfamilyoutputformat.queue.size";

    private static final Logger logger = LoggerFactory.getLogger(ColumnFamilyOutputFormat.class);

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
     * Connects to the given server:port and returns a client based on the given socket that points to the configured
     * keyspace, and is logged in with the configured credentials.
     *
     * @param host fully qualified host name to connect to
     * @param port RPC port of the server
     * @param conf a job configuration
     * @return a cassandra client
     * @throws Exception set of thrown exceptions may be implementation defined,
     *                   depending on the used transport factory
     */
    @SuppressWarnings("resource")
    public static Cassandra.Client createAuthenticatedClient(String host, int port, Configuration conf) throws Exception
    {
        logger.debug("Creating authenticated client for CF output format");
        TTransport transport = ConfigHelper.getClientTransportFactory(conf).openTransport(host, port);
        TProtocol binaryProtocol = new TBinaryProtocol(transport, true, true);
        Cassandra.Client client = new Cassandra.Client(binaryProtocol);
        client.set_keyspace(ConfigHelper.getOutputKeyspace(conf));
        String user = ConfigHelper.getOutputKeyspaceUserName(conf);
        String password = ConfigHelper.getOutputKeyspacePassword(conf);
        if ((user != null) && (password != null))
            login(user, password, client);

        logger.debug("Authenticated client for CF output format created successfully");
        return client;
    }

    public static void login(String user, String password, Cassandra.Client client) throws Exception
    {
        Map<String, String> creds = new HashMap<String, String>();
        creds.put(PasswordAuthenticator.USERNAME_KEY, user);
        creds.put(PasswordAuthenticator.PASSWORD_KEY, password);
        AuthenticationRequest authRequest = new AuthenticationRequest(creds);
        client.login(authRequest);
    }

    /** Fills the deprecated OutputFormat interface for streaming. */
    @Deprecated
    public ColumnFamilyRecordWriter getRecordWriter(org.apache.hadoop.fs.FileSystem filesystem, org.apache.hadoop.mapred.JobConf job, String name, org.apache.hadoop.util.Progressable progress)
    {
        return new ColumnFamilyRecordWriter(job, progress);
    }

    /**
     * Get the {@link RecordWriter} for the given task.
     *
     * @param context
     *            the information about the current task.
     * @return a {@link RecordWriter} to write the output for the job.
     */
    public ColumnFamilyRecordWriter getRecordWriter(final TaskAttemptContext context) throws InterruptedException
    {
        return new ColumnFamilyRecordWriter(context);
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
