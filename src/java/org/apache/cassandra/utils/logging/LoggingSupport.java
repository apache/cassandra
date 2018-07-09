package org.apache.cassandra.utils.logging;

import java.util.Map;

/**
 * Common abstraction of functionality which can be implemented for different logging backend implementations (slf4j bindings).
 * Concrete implementations are dynamically loaded and instantiated by {@link LoggingSupportFactory#getLoggingSupport()}.
 */
public interface LoggingSupport
{
    /**
     * Hook used to execute logging implementation specific customization at Cassandra startup time.
     */
    default void onStartup() {}

    /**
     * Hook used to execute logging implementation specific customization at Cassandra shutdown time.
     */
    default void onShutdown() {}

    /**
     * Changes the given logger to the given log level.
     *
     * @param classQualifier the class qualifier or logger name
     * @param rawLevel the string representation of a log level
     * @throws Exception an exception which may occur while changing the given logger to the given log level.
     */
    void setLoggingLevel(String classQualifier, String rawLevel) throws Exception;

    /**
     * @return a map of logger names and their associated log level as string representations.
     */
    Map<String, String> getLoggingLevels();
}
