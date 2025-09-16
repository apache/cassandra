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

package org.apache.cassandra.distributed.test.guardrails;

import javax.management.Attribute;
import javax.management.MBeanServerConnection;
import javax.management.ObjectName;
import javax.management.remote.JMXConnector;

import org.junit.Test;

import org.apache.cassandra.db.guardrails.CustomGuardrailConfig;
import org.apache.cassandra.db.guardrails.UUIDRoleNameGenerator;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.shared.JMXUtil;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.exceptions.ConfigurationException;

import static org.apache.cassandra.db.guardrails.Guardrails.MBEAN_NAME;
import static org.apache.cassandra.distributed.Cluster.build;
import static org.apache.cassandra.distributed.api.Feature.JMX;
import static org.apache.cassandra.utils.JsonUtils.JSON_OBJECT_MAPPER;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;

public class GuardrailsRoleNamePolicyTest extends TestBaseImpl
{
    @Test
    public void testInvalidConfigurationPreventsNodeFromStart()
    {
        CustomGuardrailConfig config = new CustomGuardrailConfig();
        config.put("generator_class_name", UUIDRoleNameGenerator.class.getName());
        config.put("min_generated_name_size", 5);

        try (Cluster ignored = build(1)
                               .withConfig(c -> c.set("role_name_policy", config))
                               .start())
        {
            fail("should throw ConfigurationException");
        }
        catch (Exception ex)
        {
            assertEquals(ConfigurationException.class.getName(), ex.getClass().getName());
        }
    }

    @Test
    public void testDisabledReconfiguration() throws Throwable
    {
        CustomGuardrailConfig config = new CustomGuardrailConfig();
        config.put("generator_class_name", UUIDRoleNameGenerator.class.getName());

        try (Cluster cluster = build(1)
                               .withConfig(c -> c.with(JMX)
                                                 .set("role_name_policy_reconfiguration_enabled", false)
                                                 .set("role_name_policy", config))
                               .start();
             JMXConnector connector = JMXUtil.getJmxConnector(cluster.get(1).config()))
        {
            MBeanServerConnection mbsc = connector.getMBeanServerConnection();

            mbsc.setAttribute(ObjectName.getInstance(MBEAN_NAME), new Attribute("RoleNamePolicy", JSON_OBJECT_MAPPER.writeValueAsString(config)));

            assertFalse(cluster.get(1)
                               .logs()
                               .watchFor("It is not possible to reconfigure role_name_policy guardrail because " +
                                         "property 'role_name_policy_reconfiguration_enabled' is set to false.")
                               .getResult()
                               .isEmpty());
        }
    }
}
