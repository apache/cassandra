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

package org.apache.cassandra.utils.concurrent;

import java.util.Collections;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collector;
import java.util.stream.Stream;

public class Threads
{
    public static class StackTraceCombiner implements Collector<StackTraceElement, StringBuilder, String>, Supplier<StringBuilder>, Function<StringBuilder, String>
    {
        final boolean printBriefPackages;
        final String prefix;
        final String delimiter;
        final String suffix;

        public StackTraceCombiner(boolean printBriefPackages, String prefix, String delimiter, String suffix)
        {
            this.printBriefPackages = printBriefPackages;
            this.prefix = prefix;
            this.delimiter = delimiter;
            this.suffix = suffix;
        }

        public Supplier<StringBuilder> supplier()
        {

            return this;
        }

        public BiConsumer<StringBuilder, StackTraceElement> accumulator()
        {
            return (sb, ste) ->
            {
                if (sb.length() > prefix.length())
                    sb.append(delimiter);

                String className = ste.getClassName();

                if (printBriefPackages)
                {
                    int afterPrevDot = 0;
                    while (true)
                    {
                        int dot = className.indexOf('.', afterPrevDot);
                        if (dot < 0)
                            break;

                        sb.append(className.charAt(afterPrevDot));
                        sb.append('.');
                        afterPrevDot = dot + 1;
                    }
                    sb.append(className, afterPrevDot, className.length());
                }
                else
                {
                    sb.append(className);
                }
                sb.append('.');
                sb.append(ste.getMethodName());
                sb.append(':');
                sb.append(ste.getLineNumber());
            };
        }

        public BinaryOperator<StringBuilder> combiner()
        {
            return (sb1, sb2) -> sb1.append("; ").append(sb2);
        }

        public Function<StringBuilder, String> finisher()
        {
            return this;
        }

        public Set<Characteristics> characteristics()
        {
            return Collections.emptySet();
        }

        public StringBuilder get()
        {
            return new StringBuilder(prefix);
        }

        public String apply(StringBuilder finish)
        {
            finish.append(suffix);
            return finish.toString();
        }
    }

    public static String prettyPrintStackTrace(Thread thread, boolean printBriefPackages, String delimiter)
    {
        return prettyPrint(thread.getStackTrace(), printBriefPackages, delimiter);
    }

    public static String prettyPrintStackTrace(Thread thread, boolean printBriefPackages, String prefix, String delimiter, String suffix)
    {
        return prettyPrint(thread.getStackTrace(), printBriefPackages, prefix, delimiter, suffix);
    }

    public static String prettyPrint(StackTraceElement[] st, boolean printBriefPackages, String delimiter)
    {
        return prettyPrint(st, printBriefPackages, "", delimiter, "");
    }

    public static String prettyPrint(StackTraceElement[] st, boolean printBriefPackages, String prefix, String delimiter, String suffix)
    {
        return Stream.of(st).collect(new StackTraceCombiner(printBriefPackages, prefix, delimiter, suffix));
    }

    public static String prettyPrint(Stream<StackTraceElement> st, boolean printBriefPackages, String prefix, String delimiter, String suffix)
    {
        return st.collect(new StackTraceCombiner(printBriefPackages, prefix, delimiter, suffix));
    }

}
