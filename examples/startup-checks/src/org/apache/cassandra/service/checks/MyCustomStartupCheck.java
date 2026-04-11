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

package org.apache.cassandra.service.checks;

import java.time.Instant;
import java.util.Calendar;
import java.util.TimeZone;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.StartupChecksConfiguration;
import org.apache.cassandra.exceptions.StartupException;
import org.apache.cassandra.service.StartupCheck;

public class MyCustomStartupCheck implements StartupCheck
{
    private static final Logger logger = LoggerFactory.getLogger(MyCustomStartupCheck.class);

    @Override
    public String name()
    {
        return "my_check";
    }

    @Override
    public void execute(StartupChecksConfiguration configuration) throws StartupException
    {
        if (configuration.isDisabled(name()))
            return;

        Calendar calendar = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
        calendar.set(2000, 1, 1);
        if (Instant.now().isBefore(calendar.toInstant()))
        {
            throw new StartupException(StartupException.ERR_WRONG_MACHINE_STATE,
                                       "Cassandra is the database for this millennium!");
        }
        else
        {
            logger.info("Executing " + name() + " with options: " + configuration.getConfig(name()));
        }
    }

    @Override
    public boolean isConfigurable()
    {
        return true;
    }

    @Override
    public boolean isDisabledByDefault()
    {
        return false;
    }

    @Override
    public void postAction(StartupChecksConfiguration options)
    {
        if (options.isDisabled(name()))
            return;

        logger.info("Executing post-action for " + name());
    }
}
