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

import java.util.HashSet;
import java.util.Set;
import javax.management.MBeanServerConnection;
import javax.management.remote.JMXConnector;

import org.junit.Assert;
import org.junit.Test;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.api.IInstanceConfig;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.api.NodeToolResult;
import org.apache.cassandra.distributed.impl.INodeProvisionStrategy;
import org.apache.cassandra.distributed.shared.ClusterUtils;
import org.apache.cassandra.distributed.shared.JMXUtil;
import org.apache.cassandra.distributed.test.TestBaseImpl;

import static org.apache.cassandra.distributed.test.jmx.JMXGetterCheckTest.testAllValidGetters;
import static org.hamcrest.Matchers.blankOrNullString;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.startsWith;

public class JMXFeatureTest extends TestBaseImpl
{

    /**
     * Test the in-jvm dtest JMX feature.
     * - Create a cluster with multiple JMX servers, one per instance
     * - Test that when connecting, we get the correct MBeanServer by checking the default domain, which is set to the IP of the instance
     * - Run the test multiple times to ensure cleanup of the JMX servers is complete so the next test can run successfully using the same host/port.
     *
     * @throws Exception it's a test that calls JMX endpoints - lots of different Jmx exceptions are possible
     */
    @Test
    public void testMultipleNetworkInterfacesProvisioning() throws Exception
    {
        testJmxFeatures(INodeProvisionStrategy.Strategy.MultipleNetworkInterfaces);
    }

    @Test
    public void testOneNetworkInterfaceProvisioning() throws Exception
    {
        testJmxFeatures(INodeProvisionStrategy.Strategy.OneNetworkInterface);
    }

    private void testJmxFeatures(INodeProvisionStrategy.Strategy provisionStrategy) throws Exception
    {
        Set<String> allInstances = new HashSet<>();
        int iterations = 2; // Make sure the JMX infrastructure all cleans up properly by running this multiple times.
        for (int i = 0; i < iterations; i++)
        {
            try (Cluster cluster = Cluster.build(2)
                                          .withDynamicPortAllocation(true)
                                          .withNodeProvisionStrategy(provisionStrategy)
                                          .withConfig(c -> c.with(Feature.values())).start())
            {
                Set<String> instancesContacted = new HashSet<>();
                for (IInvokableInstance instance : cluster)
                {
                    testInstance(instancesContacted, instance);
                }
                Assert.assertEquals("Should have connected with both JMX instances.", 2, instancesContacted.size());
                allInstances.addAll(instancesContacted);
                // Make sure we actually exercise the mbeans by testing a bunch of getters.
                // Without this it's possible for the test to pass as we don't touch any mBeans that we register.
                testAllValidGetters(cluster);
            }
        }
        Assert.assertEquals("Each instance from each cluster should have been unique", iterations * 2, allInstances.size());
    }

    @Test
    public void testShutDownAndRestartInstances() throws Exception
    {
        HashSet<String> instances = new HashSet<>();
        try (Cluster cluster = Cluster.build(2).withConfig(c -> c.with(Feature.values())).start())
        {
            IInvokableInstance instanceToStop = cluster.get(1);
            IInvokableInstance otherInstance = cluster.get(2);
            testInstance(instances, cluster.get(1));
            ClusterUtils.stopUnchecked(instanceToStop);
            // NOTE: This would previously fail because we cleared everything from the TCPEndpoint map in IsolatedJmx.
            // Now, we only clear the endpoints related to that instance, which prevents this code from
            // breaking with a `java.net.BindException: Address already in use (Bind failed)`
            ClusterUtils.awaitRingStatus(otherInstance, instanceToStop, "Down");
            NodeToolResult statusResult = cluster.get(2).nodetoolResult("status");
            Assert.assertEquals(0, statusResult.getRc());
            Assert.assertThat(statusResult.getStderr(), is(blankOrNullString()));
            Assert.assertThat(statusResult.getStdout(), containsString("DN  127.0.0.1"));
            testInstance(instances, cluster.get(2));
            ClusterUtils.start(instanceToStop, props -> {
            });
            ClusterUtils.awaitRingState(otherInstance, instanceToStop, "Normal");
            ClusterUtils.awaitRingStatus(otherInstance, instanceToStop, "Up");
            statusResult = cluster.get(1).nodetoolResult("status");
            Assert.assertEquals(0, statusResult.getRc());
            Assert.assertThat(statusResult.getStderr(), is(blankOrNullString()));
            Assert.assertThat(statusResult.getStdout(), containsString("UN  127.0.0.1"));
            statusResult = cluster.get(2).nodetoolResult("status");
            Assert.assertEquals(0, statusResult.getRc());
            Assert.assertThat(statusResult.getStderr(), is(blankOrNullString()));
            Assert.assertThat(statusResult.getStdout(), containsString("UN  127.0.0.1"));
            testInstance(instances, cluster.get(1));
            testAllValidGetters(cluster);
        }
    }

    private void testInstance(Set<String> instancesContacted, IInvokableInstance instance)
    {
        IInstanceConfig config = instance.config();
        try (JMXConnector jmxc = JMXUtil.getJmxConnector(config))
        {
            MBeanServerConnection mbsc = jmxc.getMBeanServerConnection();
            // instances get their default domain set to their IP address, so us it
            // to check that we are actually connecting to the correct instance
            String defaultDomain = mbsc.getDefaultDomain();
            instancesContacted.add(defaultDomain);
            Assert.assertThat(defaultDomain, startsWith(JMXUtil.getJmxHost(config) + ':' + config.jmxPort()));
        }
        catch (Throwable t)
        {
            throw new RuntimeException("Could not connect to JMX", t);
        }
    }
}
