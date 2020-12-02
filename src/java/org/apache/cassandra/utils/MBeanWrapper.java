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
import java.util.function.Consumer;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.cassandra.config.CassandraRelevantProperties.IS_DISABLED_MBEAN_REGISTRATION;
import static org.apache.cassandra.config.CassandraRelevantProperties.MBEAN_REGISTRATION_CLASS;

/**
 * Helper class to avoid catching and rethrowing checked exceptions on MBean and
 * allow turning of MBean registration for test purposes.
 */
public interface MBeanWrapper
{
    static final Logger logger = LoggerFactory.getLogger(MBeanWrapper.class);

    static final MBeanWrapper instance = create();

    static MBeanWrapper create()
    {
        if (IS_DISABLED_MBEAN_REGISTRATION.getBoolean())
            return new NoOpMBeanWrapper();

        String klass = MBEAN_REGISTRATION_CLASS.getString();
        if (klass == null)
            return new PlatformMBeanWrapper();
        return FBUtilities.construct(klass, "mbean");
    }

    // Passing true for graceful will log exceptions instead of rethrowing them
    public void registerMBean(Object obj, ObjectName mbeanName, OnException onException);
    default void registerMBean(Object obj, ObjectName mbeanName)
    {
        registerMBean(obj, mbeanName, OnException.THROW);
    }

    default void registerMBean(Object obj, String mbeanName, OnException onException)
    {
        ObjectName name = create(mbeanName, onException);
        if (name == null)
            return;
        registerMBean(obj, name, onException);
    }
    default void registerMBean(Object obj, String mbeanName)
    {
        registerMBean(obj, mbeanName, OnException.THROW);
    }

    public boolean isRegistered(ObjectName mbeanName, OnException onException);
    default boolean isRegistered(ObjectName mbeanName)
    {
        return isRegistered(mbeanName, OnException.THROW);
    }

    default boolean isRegistered(String mbeanName, OnException onException)
    {
        ObjectName name = create(mbeanName, onException);
        if (name == null)
            return false;
        return isRegistered(name, onException);
    }
    default boolean isRegistered(String mbeanName)
    {
        return isRegistered(mbeanName, OnException.THROW);
    }

    public void unregisterMBean(ObjectName mbeanName, OnException onException);
    default void unregisterMBean(ObjectName mbeanName)
    {
        unregisterMBean(mbeanName, OnException.THROW);
    }

    default void unregisterMBean(String mbeanName, OnException onException)
    {
        ObjectName name = create(mbeanName, onException);
        if (name == null)
            return;
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

    static class NoOpMBeanWrapper implements MBeanWrapper
    {
        public void registerMBean(Object obj, ObjectName mbeanName, OnException onException) {}
        public void registerMBean(Object obj, String mbeanName, OnException onException) {}
        public boolean isRegistered(ObjectName mbeanName, OnException onException) { return false; }
        public boolean isRegistered(String mbeanName, OnException onException) { return false; }
        public void unregisterMBean(ObjectName mbeanName, OnException onException) {}
        public void unregisterMBean(String mbeanName, OnException onException) {}
    }

    static class PlatformMBeanWrapper implements MBeanWrapper
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
    }

    public enum OnException
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
