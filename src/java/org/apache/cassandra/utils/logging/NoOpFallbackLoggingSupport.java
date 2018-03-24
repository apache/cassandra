package org.apache.cassandra.utils.logging;

import java.util.Collections;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A fallback implementation with empty implementations which ensures other slf4j bindings (logging implementations)
 * than the default supported framework can be used. This loses functionality, but is perfectly fine for most
 * integration test requirements of applications using an embedded cassandra server.
 */
public class NoOpFallbackLoggingSupport implements LoggingSupport
{
    private static final Logger logger = LoggerFactory.getLogger(NoOpFallbackLoggingSupport.class);

    @Override
    public void setLoggingLevel(String classQualifier, String rawLevel) throws Exception
    {
        logger.warn("The log level was not changed, because you are using an unsupported slf4j logging implementation for which this functionality was not implemented.");
    }

    @Override
    public Map<String, String> getLoggingLevels()
    {
        logger.warn("An empty map of logger names and their logging levels was returned, because you are using an unsupported slf4j logging implementation for which this functionality was not implemented.");
        return Collections.emptyMap();
    }
}
