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
package org.apache.cassandra;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.util.Locale;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ch.qos.logback.core.status.Status;
import ch.qos.logback.core.status.StatusListener;

/*
 * Listen for logback readiness and then redirect stdout/stderr to logback
 */
public class LogbackStatusListener implements StatusListener
{

    public static final PrintStream originalOut = System.out;

    public static final PrintStream originalErr = System.err;

    private boolean hadError = false;

    private PrintStream replacementOut;

    private PrintStream replacementErr;

    @Override
    public void addStatusEvent(Status s)
    {
        if (s.getLevel() != 0 || s.getEffectiveLevel() != 0)
            hadError = true;

        if (!hadError && s.getMessage().equals("Registering current configuration as safe fallback point"))
        {
            try
            {
                installReplacementStreams();
            }
            catch (Exception e)
            {
                throw new RuntimeException(e);
            }
        }

        if (s.getMessage().equals("Logback context being closed via shutdown hook"))
        {
            if (replacementOut != null) replacementOut.flush();
            if (replacementErr != null) replacementErr.flush();
            System.setErr(originalErr);
            System.setOut(originalOut);
        }
    }

    private void installReplacementStreams() throws Exception
    {
        Logger stdoutLogger = LoggerFactory.getLogger("stdout");
        Logger stderrLogger = LoggerFactory.getLogger("stderr");

        replacementOut = wrapLogger(stdoutLogger, originalOut, "sun.stdout.encoding", false);
        System.setOut(replacementOut);
        replacementErr = wrapLogger(stderrLogger, originalErr, "sun.stderr.encoding", true);
        System.setErr(replacementErr);
    }

    private static PrintStream wrapLogger(final Logger logger, final PrintStream original, String encodingProperty, boolean error) throws Exception
    {
        final String encoding = System.getProperty(encodingProperty);
        OutputStream os = new OutputStream()
        {

            ByteArrayOutputStream baos = new ByteArrayOutputStream();

            @Override
            public void write(int b) throws IOException
            {
                baos.write(b);
            }

            @Override
            public void write(byte[] b, int offset, int length)
            {
                baos.write(b,  offset, length);
            }

            @Override
            public void write(byte[] b)
            {
                write(b, 0, b.length);
            }

            @Override
            public void flush() throws IOException
            {
                try
                {
                    //Filter out stupid PrintStream empty flushes
                    if (baos.size() == 0) return;

                    //Filter out newlines, log framework provides its own
                    if (baos.size() == 1)
                    {
                        byte[] bytes = baos.toByteArray();
                        if (bytes[0] == 0xA)
                            return;
                    }

                    //Filter out Windows newline
                    if (baos.size() == 2)
                    {
                        byte[] bytes = baos.toByteArray();
                        if (bytes[0] == 0xD && bytes[1] == 0xA)
                            return;
                    }

                    String statement;
                    if (encoding != null)
                        statement = new String(baos.toByteArray(), encoding);
                    else
                        statement = new String(baos.toByteArray());

                    if (error)
                        logger.error(statement);
                    else
                        logger.info(statement);
                }
                finally
                {
                    baos.reset();
                }
            }
        };

        if (encoding != null)
            return new PrintStream(os, true, encoding);
        return new PrintStream(os, true)
        {

            private long asyncAppenderThreadId = Long.MIN_VALUE;

            /*
             * Long and the short of it is that we don't want to serve logback a fake System.out/err.
             * ConsoleAppender is replaced so it always goes to the real System.out/err, but logback itself
             * will at times try to log to System.out/err when it has issues.
             *
             * Now here is the problem. There is a deadlock if a thread logs to System.out, blocks on the async
             * appender queue, and the async appender thread tries to log to System.out directly as part of some
             * internal logback issue.
             *
             * So to prevent this we have to exhaustively check before locking in the PrintStream and forward
             * to real System.out/err if it is the async appender
             */
            private boolean isAsyncAppender()
            {
                //Set the thread id based on the name
                if (asyncAppenderThreadId == Long.MIN_VALUE)
                        asyncAppenderThreadId = Thread.currentThread().getName().equals("AsyncAppender-Worker-ASYNC") ? Thread.currentThread().getId() : asyncAppenderThreadId;
                if (Thread.currentThread().getId() == asyncAppenderThreadId)
                    original.println("Was in async appender");
                return Thread.currentThread().getId() == asyncAppenderThreadId;
            }

            @Override
            public void flush()
            {
                if (isAsyncAppender())
                    original.flush();
                else
                    super.flush();
            }

            @Override
            public void close()
            {
                if (isAsyncAppender())
                    original.close();
                else
                    super.flush();
            }

            @Override
            public void write(int b)
            {
                if (isAsyncAppender())
                    original.write(b);
                else
                    super.write(b);
            }

            @Override
            public void write(byte[] buf, int off, int len)
            {
                if (isAsyncAppender())
                    original.write(buf, off, len);
                else
                    super.write(buf, off, len);
            }

            @Override
            public void print(boolean b)
            {
                if (isAsyncAppender())
                    original.print(b);
                else
                    super.print(b);
            }

            @Override
            public void print(char c)
            {
                if (isAsyncAppender())
                    original.print(c);
                else
                    super.print(c);
            }

            @Override
            public void print(int i)
            {
                if (isAsyncAppender())
                    original.print(i);
                else
                    super.print(i);
            }

            @Override
            public void print(long l)
            {
                if (isAsyncAppender())
                    original.print(l);
                else
                    super.print(l);
            }

            @Override
            public void print(float f)
            {
                if (isAsyncAppender())
                    original.print(f);
                else
                    super.print(f);
            }

            @Override
            public void print(double d)
            {
                if (isAsyncAppender())
                    original.print(d);
                else
                    super.print(d);
            }

            @Override
            public void print(char[] s)
            {
                if(isAsyncAppender())
                    original.println(s);
                else
                    super.print(s);
            }

            @Override
            public void print(String s)
            {
                if (isAsyncAppender())
                    original.print(s);
                else
                    super.print(s);
            }

            @Override
            public void print(Object obj)
            {
                if (isAsyncAppender())
                    original.print(obj);
                else
                    super.print(obj);
            }

            @Override
            public void println()
            {
                if (isAsyncAppender())
                    original.println();
                else
                    super.println();
            }

            @Override
            public void println(boolean v)
            {
                if (isAsyncAppender())
                    original.println(v);
                else
                    super.println(v);
            }

            @Override
            public void println(char v)
            {
                if (isAsyncAppender())
                    original.println(v);
                else
                    super.println(v);
            }

            @Override
            public void println(int v)
            {
                if (isAsyncAppender())
                    original.println(v);
                else
                    super.println(v);
            }

            @Override
            public void println(long v)
            {
                if (isAsyncAppender())
                    original.println(v);
                else
                    super.println(v);
            }

            @Override
            public void println(float v)
            {
                if (isAsyncAppender())
                    original.println(v);
                else
                    super.println(v);
            }

            @Override
            public void println(double v)
            {
                if (isAsyncAppender())
                    original.println(v);
                else
                    super.println(v);
            }

            @Override
            public void println(char[] v)
            {
                if (isAsyncAppender())
                    original.println(v);
                else
                    super.println(v);
            }

            @Override
            public void println(String v)
            {
                if (isAsyncAppender())
                    original.println(v);
                else
                    super.println(v);
            }

            @Override
            public void println(Object v)
            {
                if (isAsyncAppender())
                    original.println(v);
                else
                    super.println(v);
            }

            @Override
            public PrintStream printf(String format, Object... args)
            {
                if (isAsyncAppender())
                    return original.printf(format, args);
                else
                    return super.printf(format, args);
            }

            @Override
            public PrintStream printf(Locale l, String format, Object... args)
            {
                if (isAsyncAppender())
                    return original.printf(l, format, args);
                else
                    return super.printf(l, format, args);
            }

            @Override
            public PrintStream format(String format, Object... args)
            {
                if (isAsyncAppender())
                    return original.format(format, args);
                else
                    return super.format(format, args);
            }

            @Override
            public PrintStream format(Locale l, String format, Object... args)
            {
                if (isAsyncAppender())
                    return original.format(l, format, args);
                else
                    return super.format(l, format, args);
            }

            @Override
            public PrintStream append(CharSequence csq)
            {
                if (isAsyncAppender())
                    return original.append(csq);
                else
                    return super.append(csq);
            }

            @Override
            public PrintStream append(CharSequence csq, int start, int end)
            {
                if (isAsyncAppender())
                    return original.append(csq, start, end);
                else
                    return super.append(csq, start, end);
            }

            @Override
            public PrintStream append(char c)
            {
                if (isAsyncAppender())
                    return original.append(c);
                else
                    return super.append(c);
            }
        };
    }
}
