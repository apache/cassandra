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

package org.apache.cassandra.simulator.asm;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Pattern;

import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.util.Printer;
import org.objectweb.asm.util.Textifier;
import org.objectweb.asm.util.TraceMethodVisitor;

import static java.util.Arrays.stream;
import static org.apache.cassandra.simulator.asm.MethodLogger.Level.NONE;
import static org.apache.cassandra.simulator.asm.MethodLogger.Level.valueOf;

// checkstyle: suppress below 'blockSystemPropertyUsage'

// TODO (config): support logging only for packages/classes matching a pattern
interface MethodLogger
{
    static final Level LOG = valueOf(System.getProperty("cassandra.test.simulator.print_asm", "none").toUpperCase());
    static final Set<TransformationKind> KINDS = System.getProperty("cassandra.test.simulator.print_asm_opts", "").isEmpty()
                                                 ? EnumSet.allOf(TransformationKind.class)
                                                 : stream(System.getProperty("cassandra.test.simulator.print_asm_opts", "").split(","))
                                                   .map(TransformationKind::valueOf)
                                                   .collect(() -> EnumSet.noneOf(TransformationKind.class), Collection::add, Collection::addAll);
    static final Pattern LOG_CLASSES = System.getProperty("cassandra.test.simulator.print_asm_classes", "").isEmpty()
                                                 ? null
                                                 : Pattern.compile(System.getProperty("cassandra.test.simulator.print_asm_classes", ""));

    // debug the output of each class at most once
    static final Set<String> LOGGED_CLASS = LOG != NONE ? Collections.newSetFromMap(new ConcurrentHashMap<>()) : null;

    enum Level { NONE, CLASS_SUMMARY, CLASS_DETAIL, METHOD_SUMMARY, METHOD_DETAIL, ASM }

    MethodVisitor visitMethod(int access, String name, String descriptor, MethodVisitor parent);
    void witness(TransformationKind kind);
    void visitEndOfClass();

    static MethodLogger log(int api, String className)
    {
        switch (LOG)
        {
            default:
            case NONE:
                return None.INSTANCE;
            case ASM:
                return (LOG_CLASSES == null || LOG_CLASSES.matcher(className).matches()) && LOGGED_CLASS.add(className)
                       ? new Printing(api, className) : None.INSTANCE;
            case CLASS_DETAIL:
            case CLASS_SUMMARY:
            case METHOD_DETAIL:
            case METHOD_SUMMARY:
                return (LOG_CLASSES == null || LOG_CLASSES.matcher(className).matches()) && LOGGED_CLASS.add(className)
                       ? new Counting(api, className, LOG) : None.INSTANCE;
        }
    }

    static class None implements MethodLogger
    {
        static final None INSTANCE = new None();

        @Override
        public MethodVisitor visitMethod(int access, String name, String descriptor, MethodVisitor parent)
        {
            return parent;
        }

        @Override
        public void witness(TransformationKind kind)
        {
        }

        @Override
        public void visitEndOfClass()
        {
        }
    }

    static class Counting implements MethodLogger
    {
        final int api;
        final String className;
        final Level level;
        StringWriter buffer = new StringWriter();
        PrintWriter out = new PrintWriter(buffer);

        boolean isMethodInProgress;
        boolean printMethod;
        boolean printClass;

        int methodCount;
        final int[] methodCounts = new int[TransformationKind.VALUES.size()];
        final int[] classCounts = new int[TransformationKind.VALUES.size()];

        public Counting(int api, String className, Level level)
        {
            this.api = api;
            this.className = className;
            this.level = level;
        }

        @Override
        public MethodVisitor visitMethod(int access, String name, String descriptor, MethodVisitor parent)
        {
            ++methodCount;
            if (isMethodInProgress)
                return parent;

            return new MethodVisitor(api, parent) {
                @Override
                public void visitEnd()
                {
                    super.visitEnd();
                    if (printMethod)
                    {
                        for (int i = 0 ; i < methodCounts.length ; ++i)
                            classCounts[i] += methodCounts[i];

                        switch (level)
                        {
                            case METHOD_DETAIL:
                                out.printf("Transformed %s.%s %s\n", className, name, descriptor);
                                for (int i = 0 ; i < methodCounts.length ; ++i)
                                {
                                    if (methodCounts[i] > 0)
                                        out.printf("    %3d %s\n", methodCounts[i], TransformationKind.VALUES.get(i));
                                }
                                break;

                            case METHOD_SUMMARY:
                                out.printf("Transformed %s.%s %s with %d modifications\n", className, name, descriptor, stream(methodCounts).sum());
                                break;
                        }
                        printMethod = false;
                        Arrays.fill(methodCounts, 0);
                    }
                    isMethodInProgress = false;
                }
            };
        }

        public void visitEndOfClass()
        {
            if (!printClass)
                return;

            switch (level)
            {
                case CLASS_DETAIL:
                    out.printf("Transformed %s: %d methods\n", className, methodCount);
                    for (int i = 0 ; i < classCounts.length ; ++i)
                    {
                        if (classCounts[i] > 0)
                            out.printf("    %3d %s\n", classCounts[i], TransformationKind.VALUES.get(i));
                    }
                case CLASS_SUMMARY:
                    out.printf("Transformed %s: %d methods with %d modifications\n", className, methodCount, stream(classCounts).sum());
            }
            System.out.print(buffer.toString());
            buffer = null;
            out = null;
        }

        @Override
        public void witness(TransformationKind kind)
        {
            ++methodCounts[kind.ordinal()];
            if (KINDS.contains(kind))
            {
                printMethod = true;
                printClass = true;
            }
        }
    }

    static class Printing implements MethodLogger
    {
        final int api;
        final String className;
        final Textifier textifier = new Textifier();
        StringWriter buffer = new StringWriter();
        PrintWriter out = new PrintWriter(buffer);

        boolean printClass;
        boolean printMethod;
        boolean isMethodInProgress;

        public Printing(int api, String className)
        {
            this.api = api;
            this.className = className;
        }

        @Override
        public MethodVisitor visitMethod(int access, String name, String descriptor, MethodVisitor parent)
        {
            Printer printer = textifier.visitMethod(access, name, descriptor, null, null);
            boolean isOuter = !isMethodInProgress;
            if (isOuter) isMethodInProgress = true;
            return new TraceMethodVisitor(new MethodVisitor(api, parent) {
                @Override
                public void visitEnd()
                {
                    super.visitEnd();
                    if (printMethod)
                    {
                        out.println("====" + className + '.' + name + ' ' + descriptor + ' ');
                        printer.print(out);
                    }
                    if (isOuter) isMethodInProgress = false;
                }
            }, printer);
        }

        @Override
        public void witness(TransformationKind kind)
        {
            if (KINDS.contains(kind))
            {
                printMethod = true;
                printClass = true;
            }
        }

        @Override
        public void visitEndOfClass()
        {
            if (printClass)
                System.out.println(buffer.toString());
            buffer = null;
            out = null;
        }
    }

}
