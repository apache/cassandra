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
