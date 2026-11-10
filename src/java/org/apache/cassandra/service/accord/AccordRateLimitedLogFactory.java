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

package org.apache.cassandra.service.accord;

import org.slf4j.Logger;

import accord.api.RateLimitedLogStatement;

import org.apache.cassandra.utils.NoSpamLogger.NoDuplicateSpamLogStatement;

import static java.util.concurrent.TimeUnit.MINUTES;
import static org.apache.cassandra.utils.NoSpamLogger.NoDuplicateSpamLogStatement.id;

public class AccordRateLimitedLogFactory implements RateLimitedLogStatement.Factory
{
    @Override
    public RateLimitedLogStatement info(String message, Logger logger)
    {
        NoDuplicateSpamLogStatement stmt = new NoDuplicateSpamLogStatement(logger, message, 1L, MINUTES);
        return params -> stmt.info(id(params), params);
    }

    @Override
    public RateLimitedLogStatement warn(String message, Logger logger)
    {
        NoDuplicateSpamLogStatement stmt = new NoDuplicateSpamLogStatement(logger, message, 1L, MINUTES);
        return params -> stmt.warn(id(params), params);
    }

    @Override
    public RateLimitedLogStatement error(String message, Logger logger)
    {
        NoDuplicateSpamLogStatement stmt = new NoDuplicateSpamLogStatement(logger, message, 1L, MINUTES);
        return params -> stmt.error(id(params), params);
    }
}
