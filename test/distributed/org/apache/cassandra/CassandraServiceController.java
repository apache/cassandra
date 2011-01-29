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

import java.io.IOException;
import java.net.InetAddress;
import java.net.URI;
import java.util.*;

import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.TokenRange;
import org.apache.cassandra.utils.KeyPair;
import org.apache.cassandra.utils.BlobUtils;
import org.apache.cassandra.utils.Pair;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.CompositeConfiguration;
import org.apache.commons.configuration.PropertiesConfiguration;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.*;
import org.apache.thrift.transport.*;

import org.apache.whirr.service.Cluster;
import org.apache.whirr.service.Cluster.Instance;
import org.apache.whirr.service.ClusterSpec;
import org.apache.whirr.service.ComputeServiceContextBuilder;
import org.apache.whirr.service.Service;
import org.apache.whirr.service.ServiceFactory;
import org.apache.whirr.service.cassandra.CassandraService;
import org.apache.whirr.service.cassandra.CassandraClusterActionHandler;
import org.apache.whirr.service.jclouds.RunUrlStatement;

import org.jclouds.blobstore.domain.BlobMetadata;

import org.jclouds.compute.ComputeService;
import org.jclouds.compute.domain.NodeMetadata;
import org.jclouds.compute.options.RunScriptOptions;
import org.jclouds.domain.Credentials;
import org.jclouds.io.Payload;
import org.jclouds.scriptbuilder.domain.OsFamily;
import org.jclouds.ssh.ExecResponse;
import static org.jclouds.io.Payloads.newStringPayload;

import com.google.common.base.Predicate;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.assertThat;

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
    private CassandraService    service;
    private Cluster             cluster;
    private ComputeService      computeService;
    private Credentials         credentials;
    private CompositeConfiguration config;
    private BlobMetadata        tarball;
    private List<InetAddress>   hosts;
    
    private CassandraServiceController()
    {
    }

    public Cassandra.Client createClient(InetAddress addr)
        throws TTransportException, TException
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
            config.setProperty(CassandraClusterActionHandler.MAJOR_VERSION, "0.7");
        }

        service = (CassandraService)new ServiceFactory().create(clusterSpec.getServiceName());
        cluster = service.launchCluster(clusterSpec);
        computeService = ComputeServiceContextBuilder.build(clusterSpec).getComputeService();
        hosts = new ArrayList<InetAddress>();
        for (Instance instance : cluster.getInstances())
        {
            hosts.add(instance.getPublicAddress());
            credentials = instance.getLoginCredentials();
        }

        waitForClusterInitialization();

        ShutdownHook shutdownHook = new ShutdownHook(this);
        Runtime.getRuntime().addShutdownHook(shutdownHook);

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
        callOnHosts(String.format("apache/cassandra/nodetool %s", args), hosts);
    }

    /**
     * Wipes all persisted state for the given node, leaving it as if it had just started.
     */
    public void wipeHosts(InetAddress... hosts)
    {
        callOnHosts("apache/cassandra/wipe-state", hosts);
    }

    public Failure failHosts(List<InetAddress> hosts)
    {
        return new Failure(hosts.toArray(new InetAddress[hosts.size()])).trigger();
    }

    public Failure failHosts(InetAddress... hosts)
    {
        return new Failure(hosts).trigger();
    }

    /** TODO: Move to CassandraService? */
    protected void callOnHosts(String payload, InetAddress... hosts)
    {
        final Set<String> hostset = new HashSet<String>();
        for (InetAddress host : hosts)
            hostset.add(host.getHostAddress());
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
            }, newStringPayload(new RunUrlStatement(clusterSpec.getRunUrlBase(), payload).render(OsFamily.UNIX)),
            RunScriptOptions.Builder.overrideCredentialsWith(credentials));
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }
        if (results.size() != hostset.size())
            throw new RuntimeException(results.size() + " hosts matched " + hostset + ": " + results);
        for (ExecResponse response : results.values())
            if (response.getExitCode() != 0)
                throw new RuntimeException("Call " + payload + " failed on at least one of " + hostset + ": " + results.values());
    }

    public List<InetAddress> getHosts()
    {
        return hosts;
    }

    class Failure
    {
        private InetAddress[] hosts;

        public Failure(InetAddress... hosts)
        {
            this.hosts = hosts;
        }
        
        public Failure trigger()
        {
            callOnHosts("apache/cassandra/stop", hosts);
            return this;
        }

        public void resolve()
        {
            callOnHosts("apache/cassandra/start", hosts);
            for (InetAddress host : hosts)
                waitForNodeInitialization(host);
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
