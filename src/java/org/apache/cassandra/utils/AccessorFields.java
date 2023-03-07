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

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.AbstractMap;
import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.cassandra.audit.AuditLogManagerMBean;
import org.apache.cassandra.auth.AuthCacheMBean;
import org.apache.cassandra.auth.AuthPropertiesMXBean;
import org.apache.cassandra.auth.NetworkPermissionsCacheMBean;
import org.apache.cassandra.auth.PermissionsCacheMBean;
import org.apache.cassandra.auth.RolesCacheMBean;
import org.apache.cassandra.batchlog.BatchlogManagerMBean;
import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.GuardrailsOptions;
import org.apache.cassandra.db.ColumnFamilyStoreMBean;
import org.apache.cassandra.db.DisallowedDirectoriesMBean;
import org.apache.cassandra.db.commitlog.CommitLogMBean;
import org.apache.cassandra.db.compaction.CompactionManagerMBean;
import org.apache.cassandra.db.guardrails.GuardrailsMBean;
import org.apache.cassandra.db.memtable.ShardedMemtableConfigMXBean;
import org.apache.cassandra.diag.DiagnosticEventServiceMBean;
import org.apache.cassandra.diag.LastEventIdBroadcasterMBean;
import org.apache.cassandra.gms.FailureDetectorMBean;
import org.apache.cassandra.gms.GossiperMBean;
import org.apache.cassandra.hints.HintsServiceMBean;
import org.apache.cassandra.io.sstable.IndexSummaryManagerMBean;
import org.apache.cassandra.locator.EndpointSnitchInfoMBean;
import org.apache.cassandra.net.MessagingServiceMBean;
import org.apache.cassandra.service.ActiveRepairServiceMBean;
import org.apache.cassandra.service.CacheServiceMBean;
import org.apache.cassandra.service.GCInspectorMXBean;
import org.apache.cassandra.service.StorageProxyMBean;

/**
 * This class is used to count the number of fields in the Config that match DatabaseDescriptor and GuardrailsOptions
 * get and set methods. This could use to ensure that all fields in Config that are accessible via JMX match the camel
 * case of the get and set methods also.
 * <p>
 * This class must be removed from the final version of the PR.
 */
public class AccessorFields
{
    public static void main(String[] args)
    {
        Class<?> cfg = Config.class;
        Class<?> dbd = DatabaseDescriptor.class;
        Class<?> go = GuardrailsOptions.class;

        Set<Field> configFields = Arrays.stream(cfg.getDeclaredFields())
                                        .filter(f -> !Modifier.isStatic(f.getModifiers()))
                                        .filter(f -> !Modifier.isFinal(f.getModifiers()))
                                        .filter(f -> f.getAnnotation(Deprecated.class) == null)
                                        .collect(Collectors.toSet());

        Set<AbstractMap.SimpleEntry<String, Class<?>>> getMethods = Stream.concat(Arrays.stream(dbd.getMethods()), Arrays.stream(go.getMethods()))
                                                                          .filter(m -> m.getParameterTypes().length == 0)
                                                                          .filter(m -> m.getName().startsWith("get"))
                                                                          .map(m -> new AbstractMap.SimpleEntry<String, Class<?>>(m.getName().substring(3), m.getReturnType()))
                                                                          .collect(Collectors.toSet());
        Set<AbstractMap.SimpleEntry<String, Class<?>>> setMethods = Stream.concat(Arrays.stream(dbd.getMethods()), Arrays.stream(go.getMethods()))
                                                                          .filter(m -> m.getParameterTypes().length == 1)
                                                                          .filter(m -> m.getName().startsWith("set"))
                                                                          .map(m -> new AbstractMap.SimpleEntry<String, Class<?>>(m.getName().substring(3), m.getParameterTypes()[0]))
                                                                          .collect(Collectors.toSet());
        Set<String> jmxMethodsCamelCase = getJMXMethods().stream()
                                                         .map(Method::getName)
                                                         .filter(m -> m.startsWith("get") || m.startsWith("set"))
                                                         .collect(Collectors.toSet());

        int getters = 0;
        int setters = 0;
        int volatileGetters = 0;
        int volatileSetters = 0;
        int jmxGetters = 0;
        int jmxSetters = 0;
        for (Field f : configFields)
        {
            AbstractMap.SimpleEntry<String, Class<?>> pair = new AbstractMap.SimpleEntry<>(convertToCamelCase(f.getName()), f.getType());

            if (getMethods.contains(pair))
            {
                getters++;
                if (Modifier.isVolatile(f.getModifiers()))
                    volatileGetters++;
            }

            if (setMethods.contains(pair))
            {
                setters++;
                if (Modifier.isVolatile(f.getModifiers()))
                    volatileSetters++;
            }

            if (jmxMethodsCamelCase.contains("get" + pair.getKey()))
                jmxGetters++;

            if (jmxMethodsCamelCase.contains("set" + pair.getKey()))
                jmxSetters++;
        }

        System.out.println("total: " + configFields.size());
        System.out.println("setters: " + setters + ", missed " + (configFields.size() - setters));
        System.out.println("volatile setters: " + volatileSetters + ", missed " + (configFields.size() - volatileSetters));
        System.out.println("jmx setters: " + jmxSetters + ", missed " + (configFields.size() - jmxSetters));
        System.out.println("getters: " + getters + ", missed " + (configFields.size() - getters));
        System.out.println("volatile getters: " + volatileGetters + ", missed " + (configFields.size() - volatileGetters));
        System.out.println("jmx getters: " + jmxGetters + ", missed " + (configFields.size() - jmxGetters));
    }

    private static String convertToCamelCase(String name)
    {
        String[] parts = name.split("_");
        StringBuilder camelCaseString = new StringBuilder();
        for (String part : parts)
        {
            camelCaseString.append(part.substring(0, 1).toUpperCase());
            camelCaseString.append(part.substring(1).toLowerCase());
        }
        return camelCaseString.toString();
    }

    private static Set<Method> getJMXMethods()
    {
        Method[] jmxMethods = concatArrays(StorageProxyMBean.class.getMethods(),
                                           CacheServiceMBean.class.getMethods(),
                                           ActiveRepairServiceMBean.class.getMethods(),
                                           MessagingServiceMBean.class.getMethods(),
                                           EndpointSnitchInfoMBean.class.getMethods(),
                                           IndexSummaryManagerMBean.class.getMethods(),
                                           HintsServiceMBean.class.getMethods(),
                                           GossiperMBean.class.getMethods(),
                                           FailureDetectorMBean.class.getMethods(),
                                           LastEventIdBroadcasterMBean.class.getMethods(),
                                           DiagnosticEventServiceMBean.class.getMethods(),
                                           GuardrailsMBean.class.getMethods(),
                                           CompactionManagerMBean.class.getMethods(),
                                           CommitLogMBean.class.getMethods(),
                                           ColumnFamilyStoreMBean.class.getMethods(),
                                           BatchlogManagerMBean.class.getMethods(),
                                           RolesCacheMBean.class.getMethods(),
                                           PermissionsCacheMBean.class.getMethods(),
                                           NetworkPermissionsCacheMBean.class.getMethods(),
                                           AuthCacheMBean.class.getMethods(),
                                           AuditLogManagerMBean.class.getMethods(),
                                           DisallowedDirectoriesMBean.class.getMethods(),
                                           ShardedMemtableConfigMXBean.class.getMethods(),
                                           AuthPropertiesMXBean.class.getMethods(),
                                           GCInspectorMXBean.class.getMethods());

        return Stream.of(jmxMethods).collect(Collectors.toSet());
    }
    private static Method[] concatArrays(final Method[]... arrays) {
        return Arrays.stream(arrays)
                     .flatMap(Arrays::stream)
                     .toArray(Method[]::new);
    }
}
