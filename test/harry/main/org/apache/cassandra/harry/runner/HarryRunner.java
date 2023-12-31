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

package org.apache.cassandra.harry.runner;

import java.io.File;
import java.io.FileNotFoundException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.harry.core.Configuration;
import org.apache.cassandra.harry.core.Run;
import org.apache.cassandra.harry.util.ThrowingRunnable;

import static org.apache.cassandra.config.CassandraRelevantProperties.DISABLE_TCACTIVE_OPENSSL;
import static org.apache.cassandra.config.CassandraRelevantProperties.IO_NETTY_TRANSPORT_NONATIVE;
import static org.apache.cassandra.config.CassandraRelevantProperties.ORG_APACHE_CASSANDRA_DISABLE_MBEAN_REGISTRATION;

public abstract class HarryRunner
{
    public static final Logger logger = LoggerFactory.getLogger(HarryRunner.class);

    public void run(Configuration config) throws Throwable
    {
        DISABLE_TCACTIVE_OPENSSL.setBoolean(true);
        IO_NETTY_TRANSPORT_NONATIVE.setBoolean(true);
        ORG_APACHE_CASSANDRA_DISABLE_MBEAN_REGISTRATION.setBoolean(true);

        Runner runner = config.createRunner();
        Run run = runner.getRun();

        Throwable thrown = null;

        try
        {
            beforeRun(runner);
            runner.run();
        }
        catch (Throwable e)
        {
            logger.error("Failed due to exception: " + e.getMessage(), e);
            thrown = e;
        }
        finally
        {
            logger.info("Shutting down runner..");
            boolean failed = thrown != null;
            if (!failed)
            {
                logger.info("Shutting down cluster..");
                tryRun(run.sut::shutdown);
            }
            afterRun(runner, thrown);
            logger.info("Exiting...");
            if (failed)
                System.exit(1);
            else
                System.exit(0);
        }
    }

    public void tryRun(ThrowingRunnable runnable)
    {
        try
        {
            runnable.run();
        }
        catch (Throwable t)
        {
            logger.error("Encountered an error while shutting down, ignoring.", t);
        }
    }

    /**
     * Parses the command-line args and returns a File for the configuration YAML.
     * @param args Command-line args.
     * @return Configuration YAML file.
     * @throws Exception If file is not found or cannot be read.
     */
    public static File loadConfig(String... args) throws Exception {
        if (args == null || args.length == 0) {
            throw new Exception("Harry config YAML not provided.");
        }

        File configFile =  new File(args[0]);
        if (!configFile.exists()) {
            throw new FileNotFoundException(configFile.getAbsolutePath());
        }

        if (!configFile.canRead()) {
            throw new Exception("Cannot read config file, check your permissions on " + configFile.getAbsolutePath());
        }

        return configFile;
    }

    public abstract void beforeRun(Runner runner);
    public abstract void afterRun(Runner runner, Object result);
}