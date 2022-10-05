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

import java.util.Map;
import java.util.Optional;

import ch.qos.logback.core.Appender;

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

    default Optional<Appender<?>> getAppender(Class<?> appenderClass, String appenderName)
    {
        return Optional.empty();
    }
}
