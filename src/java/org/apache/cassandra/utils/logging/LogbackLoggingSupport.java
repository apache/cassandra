package org.apache.cassandra.utils.logging;

import java.lang.management.ManagementFactory;
import java.security.AccessControlException;
import java.util.Iterator;
import java.util.Map;

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
        // The default logback configuration in conf/logback.xml allows reloading the
        // configuration when the configuration file has changed (every 60 seconds by default).
        // This requires logback to use file I/O APIs. But file I/O is not allowed from UDFs.
        // I.e. if logback decides to check for a modification of the config file while
        // executing a sandbox thread, the UDF execution and therefore the whole request
        // execution will fail with an AccessControlException.
        // To work around this, a custom ReconfigureOnChangeFilter is installed, that simply
        // prevents this configuration file check and possible reload of the configuration,
        // while executing sandboxed UDF code.
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

    private boolean hasAppenders(Logger logBackLogger)
    {
        Iterator<Appender<ILoggingEvent>> it = logBackLogger.iteratorForAppenders();
        return it.hasNext();
    }

    /**
     * The purpose of this class is to prevent logback from checking for config file change,
     * if the current thread is executing a sandboxed thread to avoid {@link AccessControlException}s.
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
