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

package org.apache.cassandra.tools.nodetool;

import java.util.HashMap;
import java.util.Map;
import javax.management.NotificationBroadcasterSupport;
import javax.management.NotificationEmitter;
import javax.management.StandardEmitterMBean;
import javax.management.StandardMBean;

import org.junit.After;
import org.junit.Before;

import org.apache.cassandra.db.compaction.CompactionManagerMBean;
import org.apache.cassandra.service.StorageServiceMBean;
import org.apache.cassandra.utils.MBeanWrapper;
import org.mockito.Mockito;

public abstract class NodetoolMockTester extends NodetoolRunnerTester
{
    public static final String STORAGE_SERVICE_MBEAN = "org.apache.cassandra.db:type=StorageService";
    public static final String COMPACTION_MANAGER_MBEAN = "org.apache.cassandra.db:type=CompactionManager";

    private static final MBeanWrapper mbeanServer = MBeanWrapper.instance;

    private MBeanMockHodler mbeanMockHodler;


    @Before
    public void prepareMocks()
    {
        mbeanMockHodler = new MBeanMockHodler();
        mbeanMockHodler.unregisterAll(mbeanServer);
        mbeanMockHodler.registerAll(mbeanServer);
    }

    @After
    public void unregisterMocks()
    {
        mbeanMockHodler.unregisterAll(mbeanServer);
    }


    protected <T> T getMock(String mBeanName)
    {
        return mbeanMockHodler.getMock(mBeanName);
    }

    private static class MBeanMockHodler
    {
        private static final Map<String, Class<?>> mbeans = Map.of(
            STORAGE_SERVICE_MBEAN, StorageServiceMBean.class,
            COMPACTION_MANAGER_MBEAN, CompactionManagerMBean.class);
        private final Map<String, StandardMBean> mocks = new HashMap<>();

        MBeanMockHodler()
        {
            mbeans.forEach((name, clz) -> mocks.put(name, newMock(clz)));
        }

        public static <T> StandardMBean newMock(Class<T> clz)
        {
            try
            {
                if (NotificationEmitter.class.isAssignableFrom(clz))
                    return new StandardEmitterMBean(Mockito.mock(clz), clz, new NotificationBroadcasterSupport());

                return new StandardMBean(Mockito.mock(clz), clz);
            }
            catch (Exception e)
            {
                throw new RuntimeException(e);
            }
        }

        @SuppressWarnings("unchecked")
        public <T> T getMock(String mBeanName)
        {
            return (T) mocks.get(mBeanName).getImplementation();
        }

        public void registerAll(MBeanWrapper mbeanMockInstance)
        {
            mocks.forEach((name, mock) -> mbeanMockInstance.registerMBean(mock, name));
        }

        public void unregisterAll(MBeanWrapper mbeanMockInstance)
        {
            mocks.keySet().forEach(name -> mbeanMockInstance.unregisterMBean(name, MBeanWrapper.OnException.IGNORE));
        }
    }
}
