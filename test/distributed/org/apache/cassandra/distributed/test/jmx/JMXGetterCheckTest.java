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
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import javax.management.JMRuntimeException;
import javax.management.MBeanAttributeInfo;
import javax.management.MBeanInfo;
import javax.management.MBeanServerConnection;
import javax.management.ObjectName;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXConnectorServer;
import javax.management.remote.JMXServiceURL;

import org.junit.Test;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.utils.JMXServerUtils;

import static org.apache.cassandra.config.CassandraRelevantProperties.IS_DISABLED_MBEAN_REGISTRATION;
import static org.apache.cassandra.cql3.CQLTester.getAutomaticallyAllocatedPort;

public class JMXGetterCheckTest extends TestBaseImpl
{
    private static final List<String> METRIC_PACKAGES = Arrays.asList("org.apache.cassandra.metrics",
                                                                      "org.apache.cassandra.db",
                                                                      "org.apache.cassandra.hints",
                                                                      "org.apache.cassandra.internal",
                                                                      "org.apache.cassandra.net",
                                                                      "org.apache.cassandra.request",
                                                                      "org.apache.cassandra.service");

    @Test
    public void test() throws Exception
    {
        InetAddress loopback = InetAddress.getLoopbackAddress();
        String jmxHost = loopback.getHostAddress();
        int jmxPort = getAutomaticallyAllocatedPort(loopback);
        JMXConnectorServer jmxServer = JMXServerUtils.createJMXServer(jmxPort, true);
        jmxServer.start();
        

        IS_DISABLED_MBEAN_REGISTRATION.setBoolean(false);
        try (Cluster cluster = Cluster.build(1).withConfig(c -> c.with(Feature.values())).start())
        {
            String url = "service:jmx:rmi:///jndi/rmi://" + jmxHost + ":" + jmxPort + "/jmxrmi";
            
            List<Named> errors = new ArrayList<>();
            try (JMXConnector jmxc = JMXConnectorFactory.connect(new JMXServiceURL(url), null))
            {
                MBeanServerConnection mbsc = jmxc.getMBeanServerConnection();
                for (String pkg : new TreeSet<>(METRIC_PACKAGES))
                {
                    Set<ObjectName> metricNames = new TreeSet<>(mbsc.queryNames(new ObjectName(pkg + ":*"), null));
                    for (ObjectName name : metricNames)
                    {
                        if (mbsc.isRegistered(name))
                        {
                            MBeanInfo info = mbsc.getMBeanInfo(name);
                            for (MBeanAttributeInfo a : info.getAttributes())
                            {
                                if (!a.isReadable())
                                    continue;
                                try
                                {
                                    mbsc.getAttribute(name, a.getName());
                                }
                                catch (JMRuntimeException e)
                                {
                                    errors.add(new Named(String.format("Attribute %s:%s", name, a.getName()), e.getCause()));
                                }
                            }
                        }
                    }
                }
            }
            if (errors != null)
            {
                AssertionError root = new AssertionError();
                errors.forEach(root::addSuppressed);
                throw root;
            }
        }
    }

    private static class Named extends RuntimeException
    {
        public Named(String msg, Throwable cause)
        {
            super(msg + "\nCaused by: " + cause.getMessage());
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
