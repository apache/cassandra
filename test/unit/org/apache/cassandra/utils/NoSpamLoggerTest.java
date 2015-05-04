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

import java.util.ArrayDeque;
import java.util.HashMap;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.TimeUnit;

import org.apache.cassandra.utils.NoSpamLogger.Level;
import org.apache.cassandra.utils.NoSpamLogger.NoSpamLogStatement;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.helpers.MarkerIgnoringBase;


public class NoSpamLoggerTest
{
    Map<Level, Queue<Pair<String, Object[]>>> logged = new HashMap<>();

   Logger mock = new MarkerIgnoringBase()
   {

       public boolean isTraceEnabled()
       {
           return false;
       }

       public void trace(String s)
       {

       }

       public void trace(String s, Object o)
       {

       }

       public void trace(String s, Object o, Object o1)
       {

       }

       public void trace(String s, Object... objects)
       {

       }

       public void trace(String s, Throwable throwable)
       {

       }

       public boolean isDebugEnabled()
       {
           return false;
       }

       public void debug(String s)
       {

       }

       public void debug(String s, Object o)
       {

       }

       public void debug(String s, Object o, Object o1)
       {

       }

       public void debug(String s, Object... objects)
       {

       }

       public void debug(String s, Throwable throwable)
       {

       }

       public boolean isInfoEnabled()
       {
           return false;
       }

       public void info(String s)
       {

       }

       public void info(String s, Object o)
       {

       }

       public void info(String s, Object o, Object o1)
       {

       }

       @Override
       public void info(String statement, Object... args)
       {
           logged.get(Level.INFO).offer(Pair.create(statement, args));
       }

       public void info(String s, Throwable throwable)
       {

       }

       public boolean isWarnEnabled()
       {
           return false;
       }

       public void warn(String s)
       {

       }

       public void warn(String s, Object o)
       {

       }

       @Override
       public void warn(String statement, Object... args)
       {
           logged.get(Level.WARN).offer(Pair.create(statement, args));
       }

       public void warn(String s, Object o, Object o1)
       {

       }

       public void warn(String s, Throwable throwable)
       {

       }

       public boolean isErrorEnabled()
       {
           return false;
       }

       public void error(String s)
       {

       }

       public void error(String s, Object o)
       {

       }

       public void error(String s, Object o, Object o1)
       {

       }

       @Override
       public void error(String statement, Object... args)
       {
           logged.get(Level.ERROR).offer(Pair.create(statement, args));
       }

       public void error(String s, Throwable throwable)
       {

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


   static final String statement = "swizzle{}";
   static final String param = "";
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
       logged.put(Level.INFO, new ArrayDeque<Pair<String, Object[]>>());
       logged.put(Level.WARN, new ArrayDeque<Pair<String, Object[]>>());
       logged.put(Level.ERROR, new ArrayDeque<Pair<String, Object[]>>());
       NoSpamLogger.clearWrappedLoggersForTest();
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

       NoSpamLogger.log( mock, l, 5,  TimeUnit.NANOSECONDS, statement, param);

       assertEquals(1, logged.get(l).size());

       NoSpamLogger.log( mock, l, 5,  TimeUnit.NANOSECONDS, statement, param);

       assertEquals(1, logged.get(l).size());

       now += 5;

       NoSpamLogger.log(mock, l, 5, TimeUnit.NANOSECONDS, statement, param);

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

       logger.info(statement, param);
       logger.info(statement, param);
       logger.warn(statement, param);
       logger.error(statement, param);

       assertLoggedSizes(1, 0, 0);

       NoSpamLogStatement statement = logger.getStatement("swizzle2{}", 10, TimeUnit.NANOSECONDS);
       statement.warn(param);
       //now is 5 so it won't log
       assertLoggedSizes(1, 0, 0);

       now = 10;
       statement.warn(param);
       assertLoggedSizes(1, 1, 0);

   }

   @Test
   public void testNoSpamLoggerStatementDirect() throws Exception
   {
       NoSpamLogger.NoSpamLogStatement nospam = NoSpamLogger.getStatement( mock, statement, 5, TimeUnit.NANOSECONDS);

       now = 5;

       nospam.info(statement, param);
       nospam.info(statement, param);
       nospam.warn(statement, param);
       nospam.error(statement, param);

       assertLoggedSizes(1, 0, 0);
   }

   private void checkMock(Level l)
   {
       Pair<String, Object[]> p = logged.get(l).poll();
       assertNotNull(p);
       assertEquals(statement, p.left);
       Object objs[] = p.right;
       assertEquals(1, objs.length);
       assertEquals(param, objs[0]);
       assertTrue(logged.get(l).isEmpty());
   }

   /*
    * Make sure that what is passed to the underlying logger is the correct set of objects
    */
   @Test
   public void testLoggedResult() throws Exception
   {
       now = 5;

       NoSpamLogger.log( mock, Level.INFO, 5,  TimeUnit.NANOSECONDS, statement, param);
       checkMock(Level.INFO);

       now = 10;

       NoSpamLogger.log( mock, Level.WARN, 5,  TimeUnit.NANOSECONDS, statement, param);
       checkMock(Level.WARN);

       now = 15;

       NoSpamLogger.log( mock, Level.ERROR, 5,  TimeUnit.NANOSECONDS, statement, param);
       checkMock(Level.ERROR);

       now = 20;

       NoSpamLogger logger = NoSpamLogger.getLogger(mock, 5, TimeUnit.NANOSECONDS);

       logger.info(statement, param);
       checkMock(Level.INFO);

       now = 25;

       logger.warn(statement, param);
       checkMock(Level.WARN);

       now = 30;

       logger.error(statement, param);
       checkMock(Level.ERROR);

       NoSpamLogger.NoSpamLogStatement nospamStatement = logger.getStatement(statement);

       now = 35;

       nospamStatement.info(param);
       checkMock(Level.INFO);

       now = 40;

       nospamStatement.warn(param);
       checkMock(Level.WARN);

       now = 45;

       nospamStatement.error(param);
       checkMock(Level.ERROR);
   }
}
