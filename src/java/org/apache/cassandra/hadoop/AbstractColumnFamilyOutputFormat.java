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
import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.auth.PasswordAuthenticator;
import org.apache.cassandra.thrift.AuthenticationRequest;
import org.apache.cassandra.thrift.Cassandra;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.*;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TTransport;

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
 * @param <Y>
 */
public abstract class AbstractColumnFamilyOutputFormat<K, Y> extends OutputFormat<K, Y> implements org.apache.hadoop.mapred.OutputFormat<K, Y>
{
    public static final String BATCH_THRESHOLD = "mapreduce.output.columnfamilyoutputformat.batch.threshold";
    public static final String QUEUE_SIZE = "mapreduce.output.columnfamilyoutputformat.queue.size";
    private static final Logger logger = LoggerFactory.getLogger(AbstractColumnFamilyOutputFormat.class);


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
