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
import java.util.HashMap;
import java.util.List;
import javax.management.InstanceNotFoundException;
import javax.management.MBeanException;
import javax.management.MBeanServerConnection;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import javax.management.ReflectionException;
import javax.management.remote.JMXConnector;

import org.junit.Test;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.shared.JMXUtil;
import org.apache.cassandra.distributed.test.TestBaseImpl;

import static org.assertj.core.api.Assertions.assertThat;

public class StorageServiceJmxTest extends TestBaseImpl
{
    @Test
    public void testGetRangeToEndpointMap() throws IOException, MalformedObjectNameException, ReflectionException, InstanceNotFoundException, MBeanException
    {
        try (Cluster cluster = Cluster.build(3).withConfig(c -> c.with(Feature.values())).start())
        {
            fixDistributedSchemas(cluster); // Converts system_auth & system_traces to NetworkTopologyStrategy

            IInvokableInstance instance = cluster.get(1);
            JMXConnector connector = JMXUtil.getJmxConnector(instance.config());
            MBeanServerConnection mbsc = connector.getMBeanServerConnection();

            ObjectName objectName = ObjectName.getInstance("org.apache.cassandra.db:type=StorageService");
            String[] operations = { "getRangeToEndpointMap", "getRangeToEndpointWithPortMap",
                                    "getRangeToRpcaddressMap", "getRangeToNativeaddressWithPortMap" };
            List<Object[]> paramsList = List.of(new Object[]{ "system" }, // LocalStrategy
                                                new Object[]{ "system_distributed" }, // SimpleStrategy
                                                new Object[]{ "system_auth" }, // NetworkTopologyStrategy
                                                new Object[]{ "system_cluster_metadata" }); // MetaStrategy
            String[] signature = new String[]{ String.class.getName() };

            for (String operationName : operations)
            {
                for (Object[] params : paramsList)
                {
                    Object resp = mbsc.invoke(objectName, operationName, params, signature);
                    assertThat(resp).isNotNull();
                    HashMap hashMap = (HashMap) resp;
                    assertThat(hashMap).size().isGreaterThan(0);
                }
            }
        }
    }
}
