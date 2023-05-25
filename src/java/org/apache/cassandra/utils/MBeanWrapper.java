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

import java.lang.management.ManagementFactory;
import java.util.Collections;
import java.util.Set;
import java.util.UUID;
import java.util.function.Consumer;
import javax.management.MBeanServer;
import javax.management.MBeanServerFactory;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import javax.management.QueryExp;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.cassandra.config.CassandraRelevantProperties.ORG_APACHE_CASSANDRA_DISABLE_MBEAN_REGISTRATION;
import static org.apache.cassandra.config.CassandraRelevantProperties.DTEST_IS_IN_JVM_DTEST;
import static org.apache.cassandra.config.CassandraRelevantProperties.MBEAN_REGISTRATION_CLASS;

/**
 * Helper class to avoid catching and rethrowing checked exceptions on MBean and
 * allow turning of MBean registration for test purposes.
 */
public interface MBeanWrapper
{
    Logger logger = LoggerFactory.getLogger(MBeanWrapper.class);

    MBeanWrapper instance = create();

    static MBeanWrapper create()
    {
        // If we're running in the in-jvm dtest environment, always use the delegating
        // mbean wrapper even if we start off with no-op, so it can be switched later
        if (DTEST_IS_IN_JVM_DTEST.getBoolean())
        {
            return new DelegatingMbeanWrapper(getMBeanWrapper());
        }

        return getMBeanWrapper();
    }

    static MBeanWrapper getMBeanWrapper()
    {
        if (ORG_APACHE_CASSANDRA_DISABLE_MBEAN_REGISTRATION.getBoolean())
        {
            return new NoOpMBeanWrapper();
        }

        String klass = MBEAN_REGISTRATION_CLASS.getString();
        if (klass == null)
        {
            if (DTEST_IS_IN_JVM_DTEST.getBoolean())
            {
                return new NoOpMBeanWrapper();
            }
            else
            {
                return new PlatformMBeanWrapper();
            }
        }
        return FBUtilities.construct(klass, "mbean");
    }

    // Passing true for graceful will log exceptions instead of rethrowing them
    void registerMBean(Object obj, ObjectName mbeanName, OnException onException);
    default void registerMBean(Object obj, ObjectName mbeanName)
    {
        registerMBean(obj, mbeanName, OnException.THROW);
    }

    default void registerMBean(Object obj, String mbeanName, OnException onException)
    {
        ObjectName name = create(mbeanName, onException);
        if (name == null)
        {
            return;
        }
        registerMBean(obj, name, onException);
    }
    default void registerMBean(Object obj, String mbeanName)
    {
        registerMBean(obj, mbeanName, OnException.THROW);
    }

    boolean isRegistered(ObjectName mbeanName, OnException onException);
    default boolean isRegistered(ObjectName mbeanName)
    {
        return isRegistered(mbeanName, OnException.THROW);
    }

    default boolean isRegistered(String mbeanName, OnException onException)
    {
        ObjectName name = create(mbeanName, onException);
        if (name == null)
        {
            return false;
        }
        return isRegistered(name, onException);
    }
    default boolean isRegistered(String mbeanName)
    {
        return isRegistered(mbeanName, OnException.THROW);
    }

    void unregisterMBean(ObjectName mbeanName, OnException onException);
    default void unregisterMBean(ObjectName mbeanName)
    {
        unregisterMBean(mbeanName, OnException.THROW);
    }

    default void unregisterMBean(String mbeanName, OnException onException)
    {
        ObjectName name = create(mbeanName, onException);
        if (name == null)
        {
            return;
        }
        unregisterMBean(name, onException);
    }
    default void unregisterMBean(String mbeanName)
    {
        unregisterMBean(mbeanName, OnException.THROW);
    }

    static ObjectName create(String mbeanName, OnException onException)
    {
        try
        {
            return new ObjectName(mbeanName);
        }
        catch (MalformedObjectNameException e)
        {
            onException.handler.accept(e);
            return null;
        }
    }

    Set<ObjectName> queryNames(ObjectName name, QueryExp query);

    MBeanServer getMBeanServer();

    class NoOpMBeanWrapper implements MBeanWrapper
    {
        public void registerMBean(Object obj, ObjectName mbeanName, OnException onException) {}
        public void registerMBean(Object obj, String mbeanName, OnException onException) {}
        public boolean isRegistered(ObjectName mbeanName, OnException onException) { return false; }
        public boolean isRegistered(String mbeanName, OnException onException) { return false; }
        public void unregisterMBean(ObjectName mbeanName, OnException onException) {}
        public void unregisterMBean(String mbeanName, OnException onException) {}
        public Set<ObjectName> queryNames(ObjectName name, QueryExp query) {return Collections.emptySet(); }
        public MBeanServer getMBeanServer() { return null; }
    }

    class PlatformMBeanWrapper implements MBeanWrapper
    {
        private final MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
        public void registerMBean(Object obj, ObjectName mbeanName, OnException onException)
        {
            try
            {
                mbs.registerMBean(obj, mbeanName);
            }
            catch (Exception e)
            {
                onException.handler.accept(e);
            }
        }

        public boolean isRegistered(ObjectName mbeanName, OnException onException)
        {
            try
            {
                return mbs.isRegistered(mbeanName);
            }
            catch (Exception e)
            {
                onException.handler.accept(e);
            }
            return false;
        }

        public void unregisterMBean(ObjectName mbeanName, OnException onException)
        {
            try
            {
                mbs.unregisterMBean(mbeanName);
            }
            catch (Exception e)
            {
                onException.handler.accept(e);
            }
        }

        public Set<ObjectName> queryNames(ObjectName name, QueryExp query)
        {
            return mbs.queryNames(name, query);
        }

        public MBeanServer getMBeanServer()
        {
            return mbs;
        }
    }

    class InstanceMBeanWrapper implements MBeanWrapper
    {
        private MBeanServer mbs;
        public final UUID id = UUID.randomUUID();

        public InstanceMBeanWrapper(String hostname)
        {
            mbs = MBeanServerFactory.createMBeanServer(hostname + "-" + id);
        }

        public void registerMBean(Object obj, ObjectName mbeanName, OnException onException)
        {
            try
            {
                mbs.registerMBean(obj, mbeanName);
            }
            catch (Exception e)
            {
                onException.handler.accept(e);
            }
        }

        public boolean isRegistered(ObjectName mbeanName, OnException onException)
        {
            try
            {
                return mbs.isRegistered(mbeanName);
            }
            catch (Exception e)
            {
                onException.handler.accept(e);
            }
            return false;
        }

        public void unregisterMBean(ObjectName mbeanName, OnException onException)
        {
            try
            {
                mbs.unregisterMBean(mbeanName);
            }
            catch (Exception e)
            {
                onException.handler.accept(e);
            }
        }

        public Set<ObjectName> queryNames(ObjectName name, QueryExp query)
        {
            return mbs.queryNames(name, query);
        }

        public MBeanServer getMBeanServer()
        {
            return mbs;
        }

        public void close() {
            mbs.queryNames(null, null).forEach(name -> {
                try {
                    if (!name.getCanonicalName().contains("MBeanServerDelegate"))
                    {
                        mbs.unregisterMBean(name);
                    }
                } catch (Throwable e) {
                    logger.debug("Could not unregister mbean {}", name.getCanonicalName());
                }
            });
            MBeanServerFactory.releaseMBeanServer(mbs);
            mbs = null;
        }
    }

    class DelegatingMbeanWrapper implements MBeanWrapper
    {
        MBeanWrapper delegate;

        public DelegatingMbeanWrapper(MBeanWrapper mBeanWrapper)
        {
            delegate = mBeanWrapper;
        }

        public void setDelegate(MBeanWrapper wrapper) {
            delegate = wrapper;
        }

        public MBeanWrapper getDelegate()
        {
            return delegate;
        }
        
        public void registerMBean(Object obj, ObjectName mbeanName, OnException onException)
        {
            try
            {
                delegate.registerMBean(obj, mbeanName);
            }
            catch (Exception e)
            {
                onException.handler.accept(e);
            }
        }

        public boolean isRegistered(ObjectName mbeanName, OnException onException)
        {
            try
            {
                return delegate.isRegistered(mbeanName);
            }
            catch (Exception e)
            {
                onException.handler.accept(e);
            }
            return false;
        }

        public void unregisterMBean(ObjectName mbeanName, OnException onException)
        {
            try
            {
                delegate.unregisterMBean(mbeanName);
            }
            catch (Exception e)
            {
                onException.handler.accept(e);
            }
        }

        public Set<ObjectName> queryNames(ObjectName name, QueryExp query)
        {
            return delegate.queryNames(name, query);
        }

        public MBeanServer getMBeanServer()
        {
            return delegate.getMBeanServer();
        }
    }

    enum OnException
    {
        THROW(e -> { throw new RuntimeException(e); }),
        LOG(e -> { logger.error("Error in MBean wrapper: ", e); }),
        IGNORE(e -> {});

        public final Consumer<Exception> handler;
        OnException(Consumer<Exception> handler)
        {
            this.handler = handler;
        }
    }
}
