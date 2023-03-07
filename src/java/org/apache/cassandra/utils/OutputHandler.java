/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.cassandra.utils;

import java.io.PrintStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public interface OutputHandler
{
    // called when an important info need to be displayed
    void output(String msg);
    default void output(String msg, Object ... args)
    {
        output(String.format(msg, args));
    }

    // called when a less important info need to be displayed
    void debug(String msg);
    default void debug(String msg, Object ... args)
    {
        debug(String.format(msg, args));
    }

    // called when the user needs to be warn
    void warn(String msg);
    void warn(Throwable th, String msg);
    default void warn(Throwable th)
    {
        warn(th, th.getMessage());
    }
    default void warn(Throwable th, String msg, Object... args)
    {
        warn(th, String.format(msg, args));
    }
    default void warn(String msg, Object ... args)
    {
        warn(String.format(msg, args));
    }

    class LogOutput implements OutputHandler
    {
        private static Logger logger = LoggerFactory.getLogger(LogOutput.class);

        public void output(String msg)
        {
            logger.info(msg);
        }

        public void debug(String msg)
        {
            logger.trace(msg);
        }

        public void warn(String msg)
        {
            logger.warn(msg);
        }

        public void warn(Throwable th, String msg)
        {
            logger.warn(msg, th);
        }
    }

    class SystemOutput implements OutputHandler
    {
        public final boolean debug;
        public final boolean printStack;
        public final PrintStream warnOut;

        public SystemOutput(boolean debug, boolean printStack)
        {
            this(debug, printStack, false);
        }

        public SystemOutput(boolean debug, boolean printStack, boolean logWarnToStdErr)
        {
            this.debug = debug;
            this.printStack = printStack;
            this.warnOut = logWarnToStdErr ? System.err : System.out;
        }

        public void output(String msg)
        {
            System.out.println(msg);
        }

        public void debug(String msg)
        {
            if (debug)
                System.out.println(msg);
        }

        public void warn(String msg)
        {
            warn((Throwable) null, msg);
        }

        public void warn(Throwable th, String msg)
        {
            warnOut.println("WARNING: " + msg);
            if (printStack && th != null)
                th.printStackTrace(warnOut);
        }
    }
}