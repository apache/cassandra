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

package org.apache.cassandra.utils.logging;

import java.lang.management.ManagementFactory;
import java.security.AccessControlException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import javax.management.JMX;
import javax.management.ObjectName;

import org.apache.cassandra.security.ThreadAwareSecurityManager;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.jmx.JMXConfiguratorMBean;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.classic.spi.TurboFilterList;
import ch.qos.logback.classic.turbo.ReconfigureOnChangeFilter;
import ch.qos.logback.classic.turbo.TurboFilter;
import ch.qos.logback.core.Appender;
import ch.qos.logback.core.hook.DelayingShutdownHook;

/**
 * Encapsulates all logback-specific implementations in a central place.
 * Generally, the Cassandra code-base should be logging-backend agnostic and only use slf4j-api.
 * This class MUST NOT be used directly, but only via {@link LoggingSupportFactory} which dynamically loads and
 * instantiates an appropriate implementation according to the used slf4j binding.
 */
public class LogbackLoggingSupport implements LoggingSupport
{

    private static final org.slf4j.Logger logger = LoggerFactory.getLogger(LogbackLoggingSupport.class);

    @Override
    public void onStartup()
    {
        checkOnlyOneVirtualTableAppender();

        // The default logback configuration in conf/logback.xml allows reloading the
        // configuration when the configuration file has changed (every 60 seconds by default).
        // This requires logback to use file I/O APIs. But file I/O is not allowed from UDFs.
        // I.e. if logback decides to check for a modification of the config file while
        // executing a sandbox thread, the UDF execution and therefore the whole request
        // execution will fail with an AccessControlException.
        // To work around this, a custom ReconfigureOnChangeFilter is installed, that simply
        // prevents this configuration file check and possible reload of the configuration,
        // while executing sandboxed UDF code.
        //
        // NOTE: this is obsolte with logback versions (at least since 1.2.3)
        Logger logbackLogger = (Logger) LoggerFactory.getLogger(ThreadAwareSecurityManager.class);
        LoggerContext ctx = logbackLogger.getLoggerContext();

        TurboFilterList turboFilterList = ctx.getTurboFilterList();
        for (int i = 0; i < turboFilterList.size(); i++)
        {
            TurboFilter turboFilter = turboFilterList.get(i);
            if (turboFilter instanceof ReconfigureOnChangeFilter)
            {
                ReconfigureOnChangeFilter reconfigureOnChangeFilter = (ReconfigureOnChangeFilter) turboFilter;
                turboFilterList.set(i, new SMAwareReconfigureOnChangeFilter(reconfigureOnChangeFilter));
                break;
            }
        }
    }

    @Override
    public void onShutdown()
    {
        DelayingShutdownHook logbackHook = new DelayingShutdownHook();
        logbackHook.setContext((LoggerContext) LoggerFactory.getILoggerFactory());
        logbackHook.run();
    }

    @Override
    public void setLoggingLevel(String classQualifier, String rawLevel) throws Exception
    {
        Logger logBackLogger = (Logger) LoggerFactory.getLogger(classQualifier);

        // if both classQualifier and rawLevel are empty, reload from configuration
        if (StringUtils.isBlank(classQualifier) && StringUtils.isBlank(rawLevel))
        {
            JMXConfiguratorMBean jmxConfiguratorMBean = JMX.newMBeanProxy(ManagementFactory.getPlatformMBeanServer(),
                                                                          new ObjectName("ch.qos.logback.classic:Name=default,Type=ch.qos.logback.classic.jmx.JMXConfigurator"),
                                                                          JMXConfiguratorMBean.class);
            jmxConfiguratorMBean.reloadDefaultConfiguration();
            return;
        }
        // classQualifier is set, but blank level given
        else if (StringUtils.isNotBlank(classQualifier) && StringUtils.isBlank(rawLevel))
        {
            if (logBackLogger.getLevel() != null || hasAppenders(logBackLogger))
                logBackLogger.setLevel(null);
            return;
        }

        Level level = Level.toLevel(rawLevel);
        logBackLogger.setLevel(level);
        logger.info("set log level to {} for classes under '{}' (if the level doesn't look like '{}' then the logger couldn't parse '{}')", level, classQualifier, rawLevel, rawLevel);
    }

    @Override
    public Map<String, String> getLoggingLevels()
    {
        Map<String, String> logLevelMaps = Maps.newLinkedHashMap();
        LoggerContext lc = (LoggerContext) LoggerFactory.getILoggerFactory();
        for (Logger logBackLogger : lc.getLoggerList())
        {
            if (logBackLogger.getLevel() != null || hasAppenders(logBackLogger))
                logLevelMaps.put(logBackLogger.getName(), logBackLogger.getLevel().toString());
        }
        return logLevelMaps;
    }

    @Override
    public Optional<Appender<?>> getAppender(Class<?> appenderClass, String name)
    {
        LoggerContext lc = (LoggerContext) LoggerFactory.getILoggerFactory();
        for (Logger logBackLogger : lc.getLoggerList())
        {
            for (Iterator<Appender<ILoggingEvent>> iterator = logBackLogger.iteratorForAppenders(); iterator.hasNext();)
            {
                Appender<ILoggingEvent> appender = iterator.next();
                if (appender.getClass() == appenderClass && appender.getName().equals(name))
                    return Optional.of(appender);
            }
        }

        return Optional.empty();
    }

    private void checkOnlyOneVirtualTableAppender()
    {
        int count = 0;
        LoggerContext lc = (LoggerContext) LoggerFactory.getILoggerFactory();
        List<String> virtualAppenderNames = new ArrayList<>();
        for (Logger logBackLogger : lc.getLoggerList())
        {
            for (Iterator<Appender<ILoggingEvent>> iterator = logBackLogger.iteratorForAppenders(); iterator.hasNext();)
            {
                Appender<?> appender = iterator.next();
                if (appender instanceof VirtualTableAppender)
                {
                    virtualAppenderNames.add(appender.getName());
                    count += 1;
                }
            }
        }

        if (count > 1)
            throw new IllegalStateException(String.format("There are multiple appenders of class %s of names %s. There is only one appender of such class allowed.",
                                                          VirtualTableAppender.class.getName(), String.join(",", virtualAppenderNames)));
    }

    private boolean hasAppenders(Logger logBackLogger)
    {
        Iterator<Appender<ILoggingEvent>> it = logBackLogger.iteratorForAppenders();
        return it.hasNext();
    }

    /**
     * The purpose of this class is to prevent logback from checking for config file change,
     * if the current thread is executing a sandboxed thread to avoid {@link AccessControlException}s.
     *
     * This is obsolete with logback versions that replaced {@link ReconfigureOnChangeFilter}
     * with {@link ch.qos.logback.classic.joran.ReconfigureOnChangeTask} (at least logback since 1.2.3).
     */
    private static class SMAwareReconfigureOnChangeFilter extends ReconfigureOnChangeFilter
    {
        SMAwareReconfigureOnChangeFilter(ReconfigureOnChangeFilter reconfigureOnChangeFilter)
        {
            setRefreshPeriod(reconfigureOnChangeFilter.getRefreshPeriod());
            setName(reconfigureOnChangeFilter.getName());
            setContext(reconfigureOnChangeFilter.getContext());
            if (reconfigureOnChangeFilter.isStarted())
            {
                reconfigureOnChangeFilter.stop();
                start();
            }
        }

        protected boolean changeDetected(long now)
        {
            if (ThreadAwareSecurityManager.isSecuredThread())
                return false;
            return super.changeDetected(now);
        }
    }
}
