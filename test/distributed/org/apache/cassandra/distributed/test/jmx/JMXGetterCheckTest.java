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
package org.apache.cassandra.distributed.test.jmx;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import javax.management.JMRuntimeException;
import javax.management.MBeanAttributeInfo;
import javax.management.MBeanInfo;
import javax.management.MBeanOperationInfo;
import javax.management.MBeanServerConnection;
import javax.management.ObjectName;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXConnectorServer;
import javax.management.remote.JMXServiceURL;

import com.google.common.collect.ImmutableSet;
import org.junit.Test;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.utils.JMXServerUtils;

import static org.apache.cassandra.config.CassandraRelevantProperties.IS_DISABLED_MBEAN_REGISTRATION;
import static org.apache.cassandra.cql3.CQLTester.getAutomaticallyAllocatedPort;

public class JMXGetterCheckTest extends TestBaseImpl
{
    private static final Set<String> IGNORE_ATTRIBUTES = ImmutableSet.of(
    "org.apache.cassandra.net:type=MessagingService:BackPressurePerHost" // throws unsupported saying the feature was removed... dropped in CASSANDRA-15375
    );
    private static final Set<String> IGNORE_OPERATIONS = ImmutableSet.of(
    "org.apache.cassandra.db:type=StorageService:stopDaemon", // halts the instance, which then causes the JVM to exit
    "org.apache.cassandra.db:type=StorageService:drain", // don't drain, it stops things which can cause other APIs to be unstable as we are in a stopped state
    "org.apache.cassandra.db:type=StorageService:stopGossiping", // if we stop gossip this can cause other issues, so avoid
    "org.apache.cassandra.db:type=StorageService:resetLocalSchema" // this will fail when there are no other nodes which can serve schema
    );

    @Test
    public void test() throws Exception
    {
        // start JMX server, which the instance will register with
        InetAddress loopback = InetAddress.getLoopbackAddress();
        String jmxHost = loopback.getHostAddress();
        int jmxPort = getAutomaticallyAllocatedPort(loopback);
        JMXConnectorServer jmxServer = JMXServerUtils.createJMXServer(jmxPort, true);
        jmxServer.start();
        String url = "service:jmx:rmi:///jndi/rmi://" + jmxHost + ":" + jmxPort + "/jmxrmi";

        IS_DISABLED_MBEAN_REGISTRATION.setBoolean(false);
        try (Cluster cluster = Cluster.build(1).withConfig(c -> c.with(Feature.values())).start())
        {
            List<Named> errors = new ArrayList<>();
            try (JMXConnector jmxc = JMXConnectorFactory.connect(new JMXServiceURL(url), null))
            {
                MBeanServerConnection mbsc = jmxc.getMBeanServerConnection();
                Set<ObjectName> metricNames = new TreeSet<>(mbsc.queryNames(null, null));
                for (ObjectName name : metricNames)
                {
                    if (!name.getDomain().startsWith("org.apache.cassandra"))
                        continue;
                    MBeanInfo info = mbsc.getMBeanInfo(name);
                    for (MBeanAttributeInfo a : info.getAttributes())
                    {
                        String fqn = String.format("%s:%s", name, a.getName());
                        if (!a.isReadable() || IGNORE_ATTRIBUTES.contains(fqn))
                            continue;
                        try
                        {
                            mbsc.getAttribute(name, a.getName());
                        }
                        catch (JMRuntimeException e)
                        {
                            errors.add(new Named(String.format("Attribute %s", fqn), e.getCause()));
                        }
                    }

                    for (MBeanOperationInfo o : info.getOperations())
                    {
                        String fqn = String.format("%s:%s", name, o.getName());
                        if (o.getSignature().length != 0 || IGNORE_OPERATIONS.contains(fqn))
                            continue;
                        try
                        {
                            mbsc.invoke(name, o.getName(), new Object[0], new String[0]);
                        }
                        catch (JMRuntimeException e)
                        {
                            errors.add(new Named(String.format("Operation %s", fqn), e.getCause()));
                        }
                    }
                }
            }
            if (!errors.isEmpty())
            {
                AssertionError root = new AssertionError();
                errors.forEach(root::addSuppressed);
                throw root;
            }
        }
    }

    /**
     * This class is meant to make new errors easier to read, by adding the JMX endpoint, and cleaning up the unneded JMX/Reflection logic cluttering the stacktrace
     */
    private static class Named extends RuntimeException
    {
        public Named(String msg, Throwable cause)
        {
            super(msg + "\nCaused by: " + cause.getClass().getCanonicalName() + ": " + cause.getMessage(), cause.getCause());
            StackTraceElement[] stack = cause.getStackTrace();
            List<StackTraceElement> copy = new ArrayList<>();
            for (StackTraceElement s : stack)
            {
                if (!s.getClassName().startsWith("org.apache.cassandra"))
                    break;
                copy.add(s);
            }
            Collections.reverse(copy);
            setStackTrace(copy.toArray(new StackTraceElement[0]));
        }
    }
}
