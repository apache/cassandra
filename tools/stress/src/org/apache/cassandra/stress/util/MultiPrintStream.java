/*
 *
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
 *
 */
package org.apache.cassandra.stress.util;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

/** PrintStream that multiplexes to multiple streams */
public class MultiPrintStream extends PrintStream
{
    private List<PrintStream> newStreams;

    public MultiPrintStream(PrintStream baseStream)
    {
        super(baseStream);
        this.newStreams = new ArrayList();
    }

    public void addStream(PrintStream printStream)
    {
        newStreams.add(printStream);
    }

    @Override
    public void flush()
    {
        super.flush();
        for (PrintStream s : newStreams)
            s.flush();
    }

    @Override
    public void close()
    {
        super.close();
        for (PrintStream s : newStreams)
            s.close();
    }

    @Override
    public boolean checkError()
    {
        boolean error = super.checkError();
        for (PrintStream s : newStreams)
        {
            if (s.checkError())
                error = true;
        }
        return error;
    }

    @Override
    public void write(int b)
    {
        super.write(b);
        for (PrintStream s: newStreams)
            s.write(b);
    }

    @Override
    public void write(byte[] buf, int off, int len)
    {
        super.write(buf, off, len);
        for (PrintStream s: newStreams)
            s.write(buf, off, len);
    }

    @Override
    public void print(boolean b)
    {
        super.print(b);
        for (PrintStream s: newStreams)
            s.print(b);
    }

    @Override
    public void print(char c)
    {
        super.print(c);
        for (PrintStream s: newStreams)
            s.print(c);
    }

    @Override
    public void print(int i)
    {
        super.print(i);
        for (PrintStream s: newStreams)
            s.print(i);
    }

    @Override
    public void print(long l)
    {
        super.print(l);
        for (PrintStream s: newStreams)
            s.print(l);
    }

    @Override
    public void print(float f)
    {
        super.print(f);
        for (PrintStream s: newStreams)
            s.print(f);
    }

    @Override
    public void print(double d)
    {
        super.print(d);
        for (PrintStream s: newStreams)
            s.print(d);
    }

    @Override
    public void print(char[] s)
    {
        super.print(s);
        for (PrintStream stream: newStreams)
            stream.print(s);
    }

    @Override
    public void print(String s)
    {
        super.print(s);
        for (PrintStream stream: newStreams)
            stream.print(s);
    }

    @Override
    public void print(Object obj)
    {
        super.print(obj);
        for (PrintStream s: newStreams)
            s.print(obj);
    }

    @Override
    public void println()
    {
        super.println();
        for (PrintStream s: newStreams)
            s.println();
    }

    @Override
    public void println(boolean x)
    {
        super.println(x);
        for (PrintStream s: newStreams)
            s.println(x);
    }

    @Override
    public void println(char x)
    {
        super.println(x);
        for (PrintStream s: newStreams)
            s.println(x);
    }

    @Override
    public void println(int x)
    {
        super.println(x);
        for (PrintStream s: newStreams)
            s.println(x);
    }

    @Override
    public void println(long x)
    {
        super.println(x);
        for (PrintStream s: newStreams)
            s.println(x);
    }

    @Override
    public void println(float x)
    {
        super.println(x);
        for (PrintStream s: newStreams)
            s.println(x);
    }

    @Override
    public void println(double x)
    {
        super.println(x);
        for (PrintStream s: newStreams)
            s.println(x);
    }

    @Override
    public void println(char[] x)
    {
        super.println(x);
        for (PrintStream s: newStreams)
            s.println(x);
    }

    @Override
    public void println(String x)
    {
        super.println(x);
        for (PrintStream s: newStreams)
            s.println(x);
    }

    @Override
    public void println(Object x)
    {
        super.println(x);
        for (PrintStream s: newStreams)
            s.println(x);
    }

    @Override
    public PrintStream printf(String format, Object... args)
    {
        for (PrintStream s: newStreams)
            s.printf(format, args);
        return super.printf(format, args);
    }

    @Override
    public PrintStream printf(Locale l, String format, Object... args)
    {
        for (PrintStream s: newStreams)
            s.printf(l, format, args);
        return super.printf(l, format, args);
    }

    @Override
    public PrintStream append(CharSequence csq)
    {
        for (PrintStream s: newStreams)
            s.append(csq);
        return super.append(csq);
    }

    @Override
    public PrintStream append(CharSequence csq, int start, int end)
    {
        for (PrintStream s: newStreams)
            s.append(csq, start, end);
        return super.append(csq, start, end);
    }

    @Override
    public PrintStream append(char c)
    {
        for (PrintStream s: newStreams)
            s.append(c);
        return super.append(c);
    }

    @Override
    public void write(byte[] b) throws IOException
    {
        super.write(b);
        for (PrintStream s: newStreams)
            s.write(b);
    }

}
