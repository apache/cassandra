/**
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

package org.apache.cassandra;

import java.net.InetAddress;
import java.net.URI;
import java.util.*;

import com.google.common.base.Predicate;

import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.utils.BlobUtils;
import org.apache.cassandra.utils.KeyPair;
import org.apache.cassandra.utils.Pair;
import org.apache.commons.configuration.CompositeConfiguration;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.whirr.service.*;
import org.apache.whirr.service.Cluster.Instance;
import org.apache.whirr.service.cassandra.CassandraClusterActionHandler;
import org.apache.whirr.service.jclouds.StatementBuilder;

import org.jclouds.blobstore.domain.BlobMetadata;
import org.jclouds.compute.ComputeService;
import org.jclouds.compute.domain.ExecResponse;
import org.jclouds.compute.domain.NodeMetadata;
import org.jclouds.compute.options.RunScriptOptions;
import org.jclouds.compute.RunScriptOnNodesException;
import org.jclouds.domain.Credentials;
import org.jclouds.scriptbuilder.domain.OsFamily;
import org.jclouds.scriptbuilder.domain.Statements;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CassandraServiceController
{
    private static final Logger LOG =
        LoggerFactory.getLogger(CassandraServiceController.class);

    protected static int CLIENT_PORT    = 9160;
    protected static int JMX_PORT       = 7199;

    private static final CassandraServiceController INSTANCE =
        new CassandraServiceController();
    
    public static CassandraServiceController getInstance()
    {
        return INSTANCE;
    }
    
    private boolean     running;

    private ClusterSpec         clusterSpec;
    private Service             service;
    private Cluster             cluster;
    private ComputeService      computeService;
    private CompositeConfiguration config;
    private BlobMetadata        tarball;
    private List<InetAddress>   hosts;
    
    private CassandraServiceController()
    {
    }

    public Cassandra.Client createClient(InetAddress addr) throws TException
    {
        TTransport transport    = new TSocket(
                                    addr.getHostAddress(),
                                    CLIENT_PORT,
                                    200000);
        transport               = new TFramedTransport(transport);
        TProtocol  protocol     = new TBinaryProtocol(transport);

        Cassandra.Client client = new Cassandra.Client(protocol);
        transport.open();

        return client;
    }

    private void waitForClusterInitialization()
    {
        for (InetAddress host : hosts)
            waitForNodeInitialization(host);
    }
    
    private void waitForNodeInitialization(InetAddress addr)
    {
        while (true)
        {
            try
            {
                Cassandra.Client client = createClient(addr);
                client.describe_cluster_name();
                break;
            }
            catch (TException e)
            {
                LOG.debug(e.toString());
                try
                {
                    Thread.sleep(1000);
                }
                catch (InterruptedException ie)
                {
                    break;
                }
            }
        }
    }

    public synchronized void startup() throws Exception
    {
        LOG.info("Starting up cluster...");

        config = new CompositeConfiguration();
        if (System.getProperty("whirr.config") != null)
        {
            config.addConfiguration(
                new PropertiesConfiguration(System.getProperty("whirr.config")));
        }
        config.addConfiguration(new PropertiesConfiguration("whirr-default.properties"));

        clusterSpec = new ClusterSpec(config);
        if (clusterSpec.getPrivateKey() == null)
        {
            Map<String, String> pair = KeyPair.generate();
            clusterSpec.setPublicKey(pair.get("public"));
            clusterSpec.setPrivateKey(pair.get("private"));
        }

        // if a local tarball is available deploy it to the blobstore where it will be available to cassandra
        if (System.getProperty("whirr.cassandra_tarball") != null)
        {
            Pair<BlobMetadata,URI> blob = BlobUtils.storeBlob(config, clusterSpec, System.getProperty("whirr.cassandra_tarball"));
            tarball = blob.left;
            config.setProperty(CassandraClusterActionHandler.BIN_TARBALL, blob.right.toURL().toString());
            // TODO: parse the CassandraVersion property file instead
            config.setProperty(CassandraClusterActionHandler.MAJOR_VERSION, "0.8");
        }

        service = new ServiceFactory().create(clusterSpec.getServiceName());
        cluster = service.launchCluster(clusterSpec);
        computeService = ComputeServiceContextBuilder.build(clusterSpec).getComputeService();
        hosts = new ArrayList<InetAddress>();
        for (Instance instance : cluster.getInstances())
        {
            hosts.add(instance.getPublicAddress());
        }

        ShutdownHook shutdownHook = new ShutdownHook(this);
        Runtime.getRuntime().addShutdownHook(shutdownHook);

        waitForClusterInitialization();

        running = true;
    }

    public synchronized void shutdown()
    {
        // catch and log errors, we're in a runtime shutdown hook
        try
        {
            LOG.info("Shutting down cluster...");
            if (tarball != null)
                BlobUtils.deleteBlob(config, clusterSpec, tarball);
            if (service != null)
                service.destroyCluster(clusterSpec);
            running = false;
        }
        catch (Exception e)
        {
            LOG.error("Error shutting down cluster.", e);
        }
    }

    public class ShutdownHook extends Thread
    {
        private CassandraServiceController controller;

        public ShutdownHook(CassandraServiceController controller)
        {
            this.controller = controller;
        }

        public void run()
        {
            controller.shutdown();
        }
    }

    public synchronized boolean ensureClusterRunning() throws Exception
    {
        if (running)
        {
            LOG.info("Cluster already running.");
            return false;
        }
        else
        {
            startup();
            return true;
        }
    }

    /**
     * Execute nodetool with args against localhost from the given host.
     */
    public void nodetool(String args, InetAddress... hosts)
    {
        callOnHosts(Arrays.asList(hosts), "nodetool_cassandra", args);
    }

    /**
     * Wipes all persisted state for the given node, leaving it as if it had just started.
     */
    public void wipeHosts(InetAddress... hosts)
    {
        callOnHosts(Arrays.asList(hosts), "wipe_cassandra");
    }

    public Failure failHosts(List<InetAddress> hosts)
    {
        return new Failure(hosts).trigger();
    }

    public Failure failHosts(InetAddress... hosts)
    {
        return new Failure(Arrays.asList(hosts)).trigger();
    }

    /** TODO: Move to CassandraService? */
    protected void callOnHosts(List<InetAddress> hosts, String functionName, String... functionArgs)
    {
        final Set<String> hostset = new HashSet<String>();

        for (InetAddress host : hosts)
            hostset.add(host.getHostAddress());

        StatementBuilder statementBuilder = new StatementBuilder();
        statementBuilder.addStatement(Statements.call(functionName, functionArgs));
        Credentials credentials = new Credentials(clusterSpec.getClusterUser(), clusterSpec.getPrivateKey());

        Map<? extends NodeMetadata,ExecResponse> results;
        try
        {
            results = computeService.runScriptOnNodesMatching(new Predicate<NodeMetadata>()
            {
                public boolean apply(NodeMetadata node)
                {
                    Set<String> intersection = new HashSet<String>(hostset);
                    intersection.retainAll(node.getPublicAddresses());
                    return !intersection.isEmpty();
                }
            },
            statementBuilder,
            RunScriptOptions.Builder.overrideCredentialsWith(credentials).wrapInInitScript(false).runAsRoot(false));
        }
        catch (RunScriptOnNodesException e)
        {
            throw new RuntimeException(e);
        }

        if (results.size() != hostset.size())
        {
            throw new RuntimeException(results.size() + " hosts matched " + hostset + ": " + results);
        }

        for (ExecResponse response : results.values())
        {
            if (response.getExitCode() != 0)
            {
                throw new RuntimeException("Call " + functionName + " failed on at least one of " + hostset + ": " + results.values());
            }
        }
    }

    public List<InetAddress> getHosts()
    {
        return hosts;
    }

    class Failure
    {
        private List<InetAddress> hosts;

        public Failure(List<InetAddress> hosts)
        {
            this.hosts = hosts;
        }
        
        public Failure trigger()
        {
            callOnHosts(hosts, "stop_cassandra");
            return this;
        }

        public void resolve()
        {
            callOnHosts(hosts, "start_cassandra");
            for (InetAddress host : hosts)
            {
                waitForNodeInitialization(host);
            }
        }
    }

    public InetAddress getPublicHost(InetAddress privateHost)
    {
        for (Instance instance : cluster.getInstances())
            if (privateHost.equals(instance.getPrivateAddress()))
                return instance.getPublicAddress();
        throw new RuntimeException("No public host for private host " + privateHost);
    }
}
