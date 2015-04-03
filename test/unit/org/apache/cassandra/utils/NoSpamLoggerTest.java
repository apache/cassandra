/*
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing,
* software distributed under the License is distributed on an
* "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
* KIND, either express or implied.  See the License for the
* specific language governing permissions and limitations
* under the License.
*/
package org.apache.cassandra.utils;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.cassandra.utils.NoSpamLogger.Level;
import org.apache.cassandra.utils.NoSpamLogger.NoSpamLogStatement;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.helpers.SubstituteLogger;


public class NoSpamLoggerTest
{
    Map<Level, List<Pair<String, Object[]>>> logged = new HashMap<>();

   Logger mock = new SubstituteLogger(null)
   {

       @Override
       public void info(String statement, Object... args)
       {
           logged.get(Level.INFO).add(Pair.create(statement, args));
       }

       @Override
       public void warn(String statement, Object... args)
       {
           logged.get(Level.WARN).add(Pair.create(statement, args));
       }

       @Override
       public void error(String statement, Object... args)
       {
           logged.get(Level.ERROR).add(Pair.create(statement, args));
       }

       @Override
       public int hashCode()
       {
           return 42;//It's a valid hash code
       }

       @Override
       public boolean equals(Object o)
       {
           return this == o;
       }
   };


   static long now;

   @BeforeClass
   public static void setUpClass() throws Exception
   {
       NoSpamLogger.CLOCK = new NoSpamLogger.Clock()
       {
        @Override
        public long nanoTime()
        {
            return now;
        }
       };
   }

   @Before
   public void setUp() throws Exception
   {
       logged.put(Level.INFO, new ArrayList<Pair<String, Object[]>>());
       logged.put(Level.WARN, new ArrayList<Pair<String, Object[]>>());
       logged.put(Level.ERROR, new ArrayList<Pair<String, Object[]>>());
   }

   @Test
   public void testNoSpamLogger() throws Exception
   {
       testLevel(Level.INFO);
       testLevel(Level.WARN);
       testLevel(Level.ERROR);
   }

   private void testLevel(Level l) throws Exception
   {
       setUp();
       now = 5;
       NoSpamLogger.clearWrappedLoggersForTest();

       NoSpamLogger.log( mock, l, 5,  TimeUnit.NANOSECONDS, "swizzle{}", "a");

       assertEquals(1, logged.get(l).size());

       NoSpamLogger.log( mock, l, 5,  TimeUnit.NANOSECONDS, "swizzle{}", "a");

       assertEquals(1, logged.get(l).size());

       now += 5;

       NoSpamLogger.log( mock, l, 5,  TimeUnit.NANOSECONDS, "swizzle{}", "a");

       assertEquals(2, logged.get(l).size());
   }

   private void assertLoggedSizes(int info, int warn, int error)
   {
       assertEquals(info, logged.get(Level.INFO).size());
       assertEquals(warn, logged.get(Level.WARN).size());
       assertEquals(error, logged.get(Level.ERROR).size());
   }

   @Test
   public void testNoSpamLoggerDirect() throws Exception
   {
       now = 5;
       NoSpamLogger logger = NoSpamLogger.getLogger( mock, 5, TimeUnit.NANOSECONDS);

       logger.info("swizzle{}", "a");
       logger.info("swizzle{}", "a");
       logger.warn("swizzle{}", "a");
       logger.error("swizzle{}", "a");

       assertLoggedSizes(1, 0, 0);

       NoSpamLogStatement statement = logger.getStatement("swizzle2{}", 10, TimeUnit.NANOSECONDS);
       statement.warn("a");
       //now is 5 so it won't log
       assertLoggedSizes(1, 0, 0);

       now = 10;
       statement.warn("a");
       assertLoggedSizes(1, 1, 0);

   }

   @Test
   public void testNoSpamLoggerStatementDirect() throws Exception
   {
       NoSpamLogger.NoSpamLogStatement statement = NoSpamLogger.getStatement( mock, "swizzle{}", 5, TimeUnit.NANOSECONDS);

       now = 5;

       statement.info("swizzle{}", "a");
       statement.info("swizzle{}", "a");
       statement.warn("swizzle{}", "a");
       statement.error("swizzle{}", "a");

       assertLoggedSizes(1, 0, 0);
   }
}
