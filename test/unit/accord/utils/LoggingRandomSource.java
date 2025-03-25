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

package accord.utils;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * This class is helpful for finding non-deterministic behavior as it tracks every call to {@link RandomSource} so that
 * different runs can be compared to find where they differ.  The most common way to use this is to replace the tests
 * {@link RandomSource} with this class and run it against 2 or more environments (that are experiencing a different outcome),
 * then compare the logs to find where the first difference is (and the previous access is likely where the non-deterministic fault is).
 */
@SuppressWarnings("unused")
public class LoggingRandomSource implements RandomSource
{
    private final RandomSource delegate;
    private final Writer writer;

    public LoggingRandomSource(File out, RandomSource delegate)
    {
        this.delegate = delegate;
        try
        {
            this.writer = new BufferedWriter(new FileWriter(out, StandardCharsets.UTF_8));
        }
        catch (IOException e)
        {
            throw new UncheckedIOException(e);
        }
    }

    private LoggingRandomSource(Writer writer, RandomSource delegate)
    {
        this.delegate = delegate;
        this.writer = writer;
    }

    @Override
    public void nextBytes(byte[] bytes)
    {
        delegate.nextBytes(bytes);
        append("nextBytes(length=" + bytes.length + ") -> " + Base64.getEncoder().encodeToString(bytes));
    }

    @Override
    public boolean nextBoolean()
    {
        boolean result = delegate.nextBoolean();
        append("nextBoolean -> " + result);
        return result;
    }

    @Override
    public int nextInt()
    {
        int result = delegate.nextInt();
        append("nextInt -> " + result);
        return result;
    }

    @Override
    public long nextLong()
    {
        long result = delegate.nextLong();
        append("nextLong -> " + result);
        return result;
    }

    @Override
    public float nextFloat()
    {
        float result = delegate.nextFloat();
        append("nextFloat -> " + result);
        return result;
    }

    @Override
    public double nextDouble()
    {
        double result = delegate.nextDouble();
        append("nextDouble -> " + result);
        return result;
    }

    @Override
    public double nextGaussian()
    {
        double result = delegate.nextGaussian();
        append("nextGaussian -> " + result);
        return result;
    }

    @Override
    public void setSeed(long seed)
    {
        append("setSeed(" + seed + ')');
        delegate.setSeed(seed);
    }

    @Override
    public RandomSource fork()
    {
        append("fork");
        return new LoggingRandomSource(writer, delegate.fork());
    }

    private void append(String line)
    {
        Thread thread = Thread.currentThread();
        StackTraceElement[] stack = normalize(thread.getStackTrace());
        try
        {
            writer.append("[thread=").append(thread.getName()).append("] ");
            writer.append(line).append(String.valueOf('\n'));
            for (int i = 0; i < stack.length; i++)
                writer.append("\t").append(stack[i].getClassName()).append("#").append(stack[i].getMethodName()).append(":").append(String.valueOf(stack[i].getLineNumber())).append("\n");
            writer.flush();
        }
        catch (IOException e)
        {
            throw new UncheckedIOException(e);
        }
    }

    private static StackTraceElement[] normalize(StackTraceElement[] stackTrace)
    {
        return Stream.of(stackTrace)
                     .filter(e -> !(e.getClassName().startsWith("com.intellij")
                                    || e.getClassName().startsWith("org.junit")
                                    || e.getClassName().startsWith("jdk.internal.reflect")
                                    || e.getClassName().startsWith("java.lang.reflect")))
                     .collect(Collectors.toList())
                     .toArray(StackTraceElement[]::new);
    }
}
