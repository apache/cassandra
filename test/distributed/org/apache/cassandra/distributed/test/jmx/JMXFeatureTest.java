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

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import javax.management.MBeanServerConnection;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;

import org.junit.Assert;
import org.junit.Test;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.test.TestBaseImpl;

import static org.hamcrest.Matchers.startsWith;

public class JMXFeatureTest extends TestBaseImpl
{

    public static final String JMX_SERVICE_URL_FMT = "service:jmx:rmi:///jndi/rmi://%s:%d/jmxrmi";

    /**
     * Test the in-jvm dtest JMX feature.
     * - Create a cluster with multiple JMX servers, one per instance
     * - Test that when connecting, we get the correct MBeanServer by checking the default domain, which is set to the IP of the instance
     * - Run the test multiple times to ensure cleanup of the JMX servers is complete so the next test can run successfully using the same host/port.
     * NOTE: In later versions of Cassandra, there is also a `testOneNetworkInterfaceProvisioning` that leverages the ability to specify
     * ports in addition to IP/Host for binding, but this version does not support that feature. Keeping the test name the same
     * so that it's consistent across versions.
     *
     * @throws Exception
     */
    @Test
    public void testMultipleNetworkInterfacesProvisioning() throws Exception
    {
        int iterations = 2; // Make sure the JMX infrastructure all cleans up properly by running this multiple times.
        Set<String> allInstances = new HashSet<>();
        for (int i = 0; i < iterations; i++)
        {
            try (Cluster cluster = Cluster.build(2).withConfig(c -> c.with(Feature.values())).start())
            {
                Set<String> instancesContacted = new HashSet<>();
                for (IInvokableInstance instance : cluster.get(1, 2))
                {
                    testInstance(instancesContacted, instance);
                }
                Assert.assertEquals("Should have connected with both JMX instances.", 2, instancesContacted.size());
                allInstances.addAll(instancesContacted);
            }
        }
        Assert.assertEquals("Each instance from each cluster should have been unique", iterations * 2, allInstances.size());
    }

    private void testInstance(Set<String> instancesContacted, IInvokableInstance instance) throws IOException
    {
        // NOTE: At some point, the hostname of the broadcastAddress can be resolved
        // and then the `getHostString`, which would otherwise return the IP address,
        // starts returning `localhost` - use `.getAddress().getHostAddress()` to work around this.
        String jmxHost = instance.config().broadcastAddress().getAddress().getHostAddress();
        int jmxPort = instance.config().jmxPort();
        String url = String.format(JMX_SERVICE_URL_FMT, jmxHost, jmxPort);
        try (JMXConnector jmxc = JMXConnectorFactory.connect(new JMXServiceURL(url), null))
        {
            MBeanServerConnection mbsc = jmxc.getMBeanServerConnection();
            // instances get their default domain set to their IP address, so us it
            // to check that we are actually connecting to the correct instance
            String defaultDomain = mbsc.getDefaultDomain();
            instancesContacted.add(defaultDomain);
            Assert.assertThat(defaultDomain, startsWith(jmxHost + ":" + jmxPort));
        }
    }
}
