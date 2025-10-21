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

package org.apache.cassandra.service;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.utils.FBUtilities;

import static org.apache.cassandra.service.DiskErrorsHandler.NoOpDiskErrorHandler.NO_OP;

public class DiskErrorsHandlerService
{
    private static final Logger logger = LoggerFactory.getLogger(DiskErrorsHandlerService.class);

    private static volatile DiskErrorsHandler instance = NO_OP;

    @VisibleForTesting
    public static synchronized void set(DiskErrorsHandler newInstance) throws ConfigurationException
    {
        if (newInstance == null)
            return;

        DiskErrorsHandler oldInstance = DiskErrorsHandlerService.instance;

        try
        {
            newInstance.init();
            instance = newInstance;

            try
            {
                oldInstance.close();
            }
            catch (Throwable t)
            {
                logger.warn("Exception occured while closing disk error handler of class " + oldInstance.getClass().getName(), t);
            }
        }
        catch (Throwable t)
        {
            throw new ConfigurationException("Exception occured while initializing disk error handler of class " + newInstance.getClass().getName(), t);
        }
    }

    public static DiskErrorsHandler get()
    {
        return instance;
    }

    public static void close() throws Throwable
    {
        get().close();
    }

    public static void configure() throws ConfigurationException
    {
        String fsErrorHandlerClass = CassandraRelevantProperties.CUSTOM_DISK_ERROR_HANDLER.getString();
        DiskErrorsHandler fsErrorHandler = fsErrorHandlerClass == null
                                           ? new DefaultDiskErrorsHandler()
                                           : FBUtilities.construct(fsErrorHandlerClass, "disk error handler");
        DiskErrorsHandlerService.set(fsErrorHandler);
    }
}
