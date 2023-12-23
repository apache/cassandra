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

package org.apache.cassandra.harry.visitors;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.harry.core.Configuration;
import org.apache.cassandra.harry.operations.Query;

public interface QueryLogger
{
    static Configuration.QueryLoggerConfiguration thisOrDefault(Configuration.QueryLoggerConfiguration config)
    {
        if (config == null)
            return () -> NO_OP;
        return config;
    }

    QueryLogger NO_OP = new NoOpQueryLogger();

    void println(String a, Object... interpolate);
    default void logSelectQuery(int modifier, Query query)
    {
        println(String.format("PD: %d. Modifier: %d.\t%s", query.pd, modifier, query.toSelectStatement()));
    }
    interface QueryLoggerFactory
    {
        QueryLogger make();
    }

    class FileQueryLogger implements QueryLogger
    {
        private static final Logger logger = LoggerFactory.getLogger(FileQueryLogger.class);
        private final BufferedWriter log;

        public FileQueryLogger(String filename)
        {
            File f = new File(filename);
            try
            {
                log = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(f)));
            }
            catch (FileNotFoundException e)
            {
                throw new RuntimeException(e);
            }
        }

        public void println(String s, Object... interpolate)
        {
            try
            {
                log.write(String.format(s, interpolate));
            }
            catch (IOException e)
            {
               logger.error("Could not log line", e);
            }
        }
    }

    class NoOpQueryLogger implements QueryLogger
    {
        public void println(String a, Object... interpolate)
        {
        }

        public void logSelectQuery(int modifier, Query query)
        {

        }
    }
}
