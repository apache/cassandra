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

package org.apache.cassandra.tools;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.exceptions.ConfigurationException;

public final class Util
{
    private Util()
    {
    }

    /**
     * This is used by standalone tools to force static initialization of DatabaseDescriptor, and fail if configuration
     * is bad.
     */
    public static void initDatabaseDescriptor()
    {
        try
        {
            DatabaseDescriptor.forceStaticInitialization();
        }
        catch (ExceptionInInitializerError e)
        {
            Throwable cause = e.getCause();
            boolean logStackTrace = !(cause instanceof ConfigurationException) || ((ConfigurationException) cause).logStackTrace;
            System.out.println("Exception (" + cause.getClass().getName() + ") encountered during startup: " + cause.getMessage());

            if (logStackTrace)
            {
                cause.printStackTrace();
                System.exit(3);
            }
            else
            {
                System.err.println(cause.getMessage());
                System.exit(3);
            }
        }
    }
}
