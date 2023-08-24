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

package org.apache.cassandra.distributed.impl;

import java.io.IOException;
import java.net.InetAddress;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorServer;
import javax.management.remote.JMXServiceURL;
import javax.management.remote.rmi.RMIConnectorServer;
import javax.management.remote.rmi.RMIJRMPServerImpl;

import org.slf4j.Logger;

import com.google.common.util.concurrent.Uninterruptibles;
import org.apache.cassandra.distributed.api.IInstance;
import org.apache.cassandra.distributed.api.IInstanceConfig;
import org.apache.cassandra.distributed.shared.JMXUtil;
import org.apache.cassandra.utils.JMXServerUtils;
import org.apache.cassandra.utils.MBeanWrapper;
import org.apache.cassandra.utils.RMIClientSocketFactoryImpl;
import sun.rmi.transport.tcp.TCPEndpoint;

import static org.apache.cassandra.config.CassandraRelevantProperties.JAVA_RMI_DGC_LEASE_VALUE_IN_JVM_DTEST;
import static org.apache.cassandra.config.CassandraRelevantProperties.ORG_APACHE_CASSANDRA_DISABLE_MBEAN_REGISTRATION;
import static org.apache.cassandra.config.CassandraRelevantProperties.SUN_RMI_TRANSPORT_TCP_THREADKEEPALIVETIME;
import static org.apache.cassandra.distributed.api.Feature.JMX;
import static org.apache.cassandra.utils.ReflectionUtils.clearMapField;

public class IsolatedJmx
{
    public static final int RMI_KEEPALIVE_TIME = 1000;
    public static final String UNKNOWN_JMX_CONNECTION_ERROR = "Could not connect to JMX due to an unknown error";

    private JMXConnectorServer jmxConnectorServer;
    private JMXServerUtils.JmxRegistry registry;
    private RMIJRMPServerImpl jmxRmiServer;
    private MBeanWrapper.InstanceMBeanWrapper wrapper;
    private RMIClientSocketFactoryImpl clientSocketFactory;
    private CollectingRMIServerSocketFactoryImpl serverSocketFactory;
    private Logger inInstancelogger;
    private IInstanceConfig config;

    public IsolatedJmx(IInstance instance, Logger inInstanceLogger) {
        this.config = instance.config();
        this.inInstancelogger = inInstanceLogger;
    }

    public void startJmx() {
        try
        {
            // Several RMI threads hold references to in-jvm dtest objects, and are, by default, kept
            // alive for long enough (minutes) to keep classloaders from being collected.
            // Set these two system properties to a low value to allow cleanup to occur fast enough
            // for GC to collect our classloaders.
            JAVA_RMI_DGC_LEASE_VALUE_IN_JVM_DTEST.setInt(RMI_KEEPALIVE_TIME);
            SUN_RMI_TRANSPORT_TCP_THREADKEEPALIVETIME.setInt(RMI_KEEPALIVE_TIME);
            ORG_APACHE_CASSANDRA_DISABLE_MBEAN_REGISTRATION.setBoolean(false);
            InetAddress addr = config.broadcastAddress().getAddress();

            int jmxPort = config.jmxPort();

            String hostname = addr.getHostAddress();
            wrapper = new MBeanWrapper.InstanceMBeanWrapper(hostname + ":" + jmxPort);
            ((MBeanWrapper.DelegatingMbeanWrapper) MBeanWrapper.instance).setDelegate(wrapper);
            Map<String, Object> env = new HashMap<>();

            serverSocketFactory = new CollectingRMIServerSocketFactoryImpl(addr);
            env.put(RMIConnectorServer.RMI_SERVER_SOCKET_FACTORY_ATTRIBUTE,
                    serverSocketFactory);
            clientSocketFactory = new RMIClientSocketFactoryImpl(addr);
            env.put(RMIConnectorServer.RMI_CLIENT_SOCKET_FACTORY_ATTRIBUTE,
                    clientSocketFactory);

            // configure the RMI registry
            registry = new JMXServerUtils.JmxRegistry(jmxPort,
                                                      clientSocketFactory,
                                                      serverSocketFactory,
                                                      "jmxrmi");

            // Mark the JMX server as a permanently exported object. This allows the JVM to exit with the
            // server running and also exempts it from the distributed GC scheduler which otherwise would
            // potentially attempt a full GC every `sun.rmi.dgc.server.gcInterval` millis (default is 3600000ms)
            // For more background see:
            //   - CASSANDRA-2967
            //   - https://www.jclarity.com/2015/01/27/rmi-system-gc-unplugged/
            //   - https://bugs.openjdk.java.net/browse/JDK-6760712
            env.put("jmx.remote.x.daemon", "true");

            // Set the port used to create subsequent connections to exported objects over RMI. This simplifies
            // configuration in firewalled environments, but it can't be used in conjuction with SSL sockets.
            // See: CASSANDRA-7087
            int rmiPort = config.jmxPort();

            // We create the underlying RMIJRMPServerImpl so that we can manually bind it to the registry,
            // rather then specifying a binding address in the JMXServiceURL and letting it be done automatically
            // when the server is started. The reason for this is that if the registry is configured with SSL
            // sockets, the JMXConnectorServer acts as its client during the binding which means it needs to
            // have a truststore configured which contains the registry's certificate. Manually binding removes
            // this problem.
            // See CASSANDRA-12109.
            jmxRmiServer = new RMIJRMPServerImpl(rmiPort, clientSocketFactory, serverSocketFactory,
                                                 env);
            JMXServiceURL serviceURL = new JMXServiceURL("rmi", hostname, rmiPort);
            jmxConnectorServer = new RMIConnectorServer(serviceURL, env, jmxRmiServer, wrapper.getMBeanServer());

            jmxConnectorServer.start();

            registry.setRemoteServerStub(jmxRmiServer.toStub());
            JMXServerUtils.logJmxServiceUrl(addr, jmxPort);
            waitForJmxAvailability(env);
        }
        catch (Throwable t)
        {
            throw new RuntimeException("Feature.JMX was enabled but could not be started.", t);
        }
    }

    private void waitForJmxAvailability(Map<String, ?> env)
    {
        try (JMXConnector ignored = JMXUtil.getJmxConnector(config, 20, env)) {
            // Do nothing - JMXUtil now retries
        }
        catch (IOException iex)
        {
            // If we land here, there's something more than a timeout
            inInstancelogger.error(UNKNOWN_JMX_CONNECTION_ERROR, iex);
            throw new RuntimeException(UNKNOWN_JMX_CONNECTION_ERROR, iex);
        }
    }

    public void stopJmx()
    {
        if (!config.has(JMX))
            return;
        // First, swap the mbean wrapper back to a NoOp wrapper
        // This prevents later attempts to unregister mbeans from failing in Cassandra code, as we're going to
        // unregister all of them here
        ((MBeanWrapper.DelegatingMbeanWrapper) MBeanWrapper.instance).setDelegate(new MBeanWrapper.NoOpMBeanWrapper());
        try
        {
            wrapper.close();
        }
        catch (Throwable e)
        {
            inInstancelogger.warn("failed to close wrapper.", e);
        }
        try
        {
            jmxConnectorServer.stop();
        }
        catch (Throwable e)
        {
            inInstancelogger.warn("failed to close jmxConnectorServer.", e);
        }
        try
        {
            registry.close();
        }
        catch (Throwable e)
        {
            inInstancelogger.warn("failed to close registry.", e);
        }
        try
        {
            clientSocketFactory.close();
        }
        catch (Throwable e)
        {
            inInstancelogger.warn("failed to close clientSocketFactory.", e);
        }
        try
        {
            serverSocketFactory.close();
        }
        catch (Throwable e)
        {
            inInstancelogger.warn("failed to close serverSocketFactory.", e);
        }
        // The TCPEndpoint class holds references to a class in the in-jvm dtest framework
        // which transitively has a reference to the InstanceClassLoader, so we need to
        // make sure to remove the reference to them when the instance is shutting down.
        // Additionally, we must make sure to only clear endpoints created by this instance
        // As clearning the entire map can cause issues with starting and stopping nodes mid-test.
        clearMapField(TCPEndpoint.class, null, "localEndpoints", this::endpointCreateByThisInstance);
        Uninterruptibles.sleepUninterruptibly(2 * RMI_KEEPALIVE_TIME, TimeUnit.MILLISECONDS); // Double the keep-alive time to give Distributed GC some time to clean up
    }

    private boolean endpointCreateByThisInstance(Map.Entry<Object, LinkedList<TCPEndpoint>> entry)
    {
        return entry.getValue()
                    .stream()
                    .anyMatch(ep -> ep.getServerSocketFactory() == this.serverSocketFactory &&
                                    ep.getClientSocketFactory() == this.clientSocketFactory);
    }
}
