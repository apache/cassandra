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
package org.apache.cassandra.anttasks;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

import org.junit.Test;

import org.apache.tools.ant.BuildException;
import org.apache.tools.ant.Task;
import org.reflections.Reflections;
import org.reflections.scanners.Scanners;
import org.reflections.util.ConfigurationBuilder;

import static java.util.stream.Collectors.toList;

public class TestNameCheckTask extends Task
{
    private static final Reflections reflections = new Reflections(new ConfigurationBuilder()
                                                                   .forPackage("org.apache.cassandra")
                                                                   .setScanners(Scanners.MethodsAnnotated, Scanners.SubTypes)
                                                                   .setExpandSuperTypes(true)
                                                                   .setParallel(true));

    public TestNameCheckTask()
    {
    }

    public static void main(String[] args)
    {
        new TestNameCheckTask().execute();
    }
    
    @Override
    public void execute() throws BuildException
    {
        Set<Method> methodsAnnotatedWith = reflections.getMethodsAnnotatedWith(Test.class);
        List<String> testFiles = methodsAnnotatedWith.stream().map(Method::getDeclaringClass).distinct()
                                                     .flatMap(TestNameCheckTask::expand)
                                                     .map(TestNameCheckTask::normalize)
                                                     .map(Class::getCanonicalName)
                                                     .filter(s -> !s.endsWith("Test"))
                                                     .distinct().sorted()
                                                     .collect(toList());

        if (!testFiles.isEmpty())
            throw new BuildException("Detected tests that have a bad naming convention. All tests have to end on 'Test': \n" + String.join("\n", testFiles));
    }

    private static Class<?> normalize(Class<?> klass)
    {
        for (; klass.getEnclosingClass() != null; klass = klass.getEnclosingClass())
        {
        }
        return klass;
    }

    private static Stream<Class<?>> expand(Class<?> klass)
    {
        Set<? extends Class<?>> subTypes = reflections.getSubTypesOf(klass);
        if (subTypes == null || subTypes.isEmpty())
            return Stream.of(klass);
        Stream<Class<?>> subs = (Stream<Class<?>>) subTypes.stream();
        // assume we include if not abstract
        if (!Modifier.isAbstract(klass.getModifiers()))
            subs = Stream.concat(Stream.of(klass), subs);
        return subs;
    }


}
