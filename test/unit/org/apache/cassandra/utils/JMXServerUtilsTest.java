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
package org.apache.cassandra.utils;

import java.io.IOException;
import java.net.InetAddress;
import javax.management.AttributeNotFoundException;
import javax.management.InstanceNotFoundException;
import javax.management.MBeanException;
import javax.management.MBeanServerConnection;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import javax.management.ReflectionException;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXConnectorServer;
import javax.management.remote.JMXServiceURL;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.assertj.core.api.ThrowableAssert;

import static org.apache.cassandra.config.CassandraRelevantProperties.IS_DISABLED_MBEAN_REGISTRATION;

public class JMXServerUtilsTest
{
    @BeforeClass
    public static void setup()
    {
        IS_DISABLED_MBEAN_REGISTRATION.setBoolean(false);
    }

    @Test
    public void test() throws IOException, MalformedObjectNameException, InstanceNotFoundException, AttributeNotFoundException, ReflectionException, MBeanException
    {
        InetAddress loopback = InetAddress.getLoopbackAddress();
        String host = loopback.getHostAddress();
        int port = CQLTester.getAutomaticallyAllocatedPort(loopback);

        JMXConnectorServer server = JMXServerUtils.createJMXServer(port, host, true);
        try
        {
            // register an MBean that will fail
            ObjectName mbeanName = new ObjectName("org.apache.cassandra.test:type=Broken");
            MBeanWrapper.instance.registerMBean(new Broken(), mbeanName);

            String url = "service:jmx:rmi:///jndi/rmi://" + host + ":" + port + "/jmxrmi";
            try (JMXConnector jmxc = JMXConnectorFactory.connect(new JMXServiceURL(url), null))
            {
                MBeanServerConnection mbsc = jmxc.getMBeanServerConnection();

                assertThatThrownByIsAllowed(() -> mbsc.getAttribute(mbeanName, "Value"));
                assertThatThrownByIsAllowed(() -> mbsc.getAttribute(mbeanName, "ValueNested"));
            }
        }
        finally
        {
            server.stop();
        }
    }

    private static void assertThatThrownByIsAllowed(ThrowableAssert.ThrowingCallable fn)
    {
        try
        {
            fn.call();
        }
        catch (Throwable throwable)
        {
            if (!JMXServerUtils.isAllowed(throwable))
                throw new AssertionError("Non-java exception found", throwable);
        }
    }

    public interface BrokenMBean
    {
        String getValue();

        String getValueNested();
    }

    public static class Broken implements BrokenMBean
    {
        @Override
        public String getValue()
        {
            throw new ConfigurationException("Not allowed");
        }

        @Override
        public String getValueNested()
        {
            int levels = 10;
            RuntimeException e = new ConfigurationException("bottom level");
            for (int i = 0; i < levels; i++)
                e = new RuntimeException("level " + i, e);
            throw e;
        }
    }
}