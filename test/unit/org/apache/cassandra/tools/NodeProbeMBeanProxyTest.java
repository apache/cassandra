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

package org.apache.cassandra.tools;

import java.util.List;
import java.util.Map;

import javax.management.InstanceNotFoundException;

import org.junit.Test;

import org.apache.cassandra.db.ColumnFamilyStoreMBean;
import org.apache.cassandra.db.compression.CompressionDictionaryManagerMBean;
import org.apache.cassandra.management.MBeanAccessor;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class NodeProbeMBeanProxyTest
{
    @Test
    public void testMissingMBeanSurfacesAsInstanceNotFound()
    {
        NodeProbe probe = new NodeProbe(new NullMBeanAccessor());

        assertThatThrownBy(probe::stopCassandraDaemon)
        .isInstanceOf(RuntimeException.class)
        .hasRootCauseInstanceOf(InstanceNotFoundException.class)
        .hasRootCauseMessage("StorageServiceMBean is not available on this node");
    }

    private static class NullMBeanAccessor implements MBeanAccessor
    {
        @Override
        public <T> T findMBean(Class<T> clazz)
        {
            return null;
        }

        @Override
        public <T> T findMBeanMetric(Class<T> clazz, Props props)
        {
            return null;
        }

        @Override
        public boolean isMBeanMetricRegistered(Props props)
        {
            return false;
        }

        @Override
        public ColumnFamilyStoreMBean findColumnFamily(String type, String keyspace, String columnFamily)
        {
            return null;
        }

        @Override
        public CompressionDictionaryManagerMBean findCompressionDictionary(String keyspace, String table)
        {
            return null;
        }

        @Override
        public List<ThreadPoolInfo> threadPoolInfos()
        {
            return List.of();
        }

        @Override
        public List<Map.Entry<String, ColumnFamilyStoreMBean>> findColumnFamilies(String type)
        {
            return List.of();
        }
    }
}
