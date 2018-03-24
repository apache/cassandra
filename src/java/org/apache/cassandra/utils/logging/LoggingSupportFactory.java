package org.apache.cassandra.utils.logging;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.utils.FBUtilities;

/**
 * Dynamically loads and instantiates an appropriate {@link LoggingSupport} implementation according to the used slf4j binding.
 * For production use, this should always be {@link LogbackLoggingSupport}.
 */
public class LoggingSupportFactory
{
    private static final Logger logger = LoggerFactory.getLogger(LoggingSupportFactory.class);

    private static volatile LoggingSupport loggingSupport;

    private LoggingSupportFactory() {}

    /**
     * @return An appropriate {@link LoggingSupport} implementation according to the used slf4j binding.
     */
    public static LoggingSupport getLoggingSupport()
    {
        if (loggingSupport == null)
        {
            // unfortunately, this is the best way to determine if logback is being used for logger
            String loggerFactoryClass = LoggerFactory.getILoggerFactory().getClass().getName();
            if (loggerFactoryClass.contains("logback"))
            {
                loggingSupport = FBUtilities.instanceOrConstruct("org.apache.cassandra.utils.logging.LogbackLoggingSupport", "LogbackLoggingSupport");
            }
            else
            {
                loggingSupport = new NoOpFallbackLoggingSupport();
                logger.warn("You are using Cassandra with an unsupported deployment. The intended logging implementation library logback is not used by slf4j. Detected slf4j logger factory: {}. "
                        + "You will not be able to dynamically manage log levels via JMX and may have performance or other issues.", loggerFactoryClass);
            }
        }
        return loggingSupport;
    }
}
