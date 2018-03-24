package org.apache.cassandra.utils.logging;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Dynamically loads and instantiates an appropriate {@link LoggingSupport} implementation according to the used slf4j binding.
 * For production use, this should always be {@link LogbackLoggingSupport}.
 */
public class LoggingSupportFactory
{
    private static final Logger logger = LoggerFactory.getLogger(LoggingSupportFactory.class);

    private static LoggingSupport selectedLoggingSupportImplementation;

    private LoggingSupportFactory() {}

    /**
     * @return An appropriate {@link LoggingSupport} implementation according to the used slf4j binding.
     */
    public static LoggingSupport getLoggingSupport()
    {
        if (selectedLoggingSupportImplementation == null)
        {
            String loggerFactoryClassName = LoggerFactory.getILoggerFactory().getClass().getName();
            if (loggerFactoryClassName.contains("logback"))
            {
                try {
                    selectedLoggingSupportImplementation = (LoggingSupport) Class.forName("org.apache.cassandra.utils.logging.LogbackLoggingSupport").newInstance();
                } catch (InstantiationException|ClassNotFoundException|IllegalAccessException e) {
                    throw new RuntimeException(e);
                }
            } else {
                selectedLoggingSupportImplementation = new NoOpFallbackLoggingSupport();
                logger.warn("You are using Cassandra with an unsupported deployment. The intended logging implementation library logback is not used by slf4j. Detected slf4j logger factory: {}. "
                        + "You will not be able to dynamically manage log levels via JMX and may have performance or other issues.", loggerFactoryClassName);
            }
        }
        return selectedLoggingSupportImplementation;
    }
}
