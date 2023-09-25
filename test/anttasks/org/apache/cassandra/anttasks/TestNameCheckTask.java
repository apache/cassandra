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

import java.io.File;
import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import org.apache.commons.lang3.function.Consumers;
import org.junit.Test;

import org.reflections.Reflections;
import org.reflections.scanners.Scanners;
import org.reflections.util.ConfigurationBuilder;

import static java.util.stream.Collectors.toList;

public class TestNameCheckTask
{
    private String scanClassPath = "build/test/classes";
    private String packageName = "org.apache.cassandra";
    private String annotationName = Test.class.getName();
    private boolean expand = true;
    private boolean normalize = true;
    private boolean verbose = false;
    private String regex = ".*Test$";

    public TestNameCheckTask()
    {
    }

    public void setScanClassPath(String scanClassPath)
    {
        this.scanClassPath = scanClassPath;
    }

    public void setPackageName(String packageName)
    {
        this.packageName = packageName;
    }

    public void setAnnotationName(String annotationName)
    {
        this.annotationName = annotationName;
    }

    public void setExpand(boolean expand)
    {
        this.expand = expand;
    }

    public void setNormalize(boolean normalize)
    {
        this.normalize = normalize;
    }

    public void setVerbose(boolean verbose)
    {
        this.verbose = verbose;
    }

    public void setRegex(String regex)
    {
        this.regex = regex;
    }

    public void execute()
    {
        List<URL> scanClassPathUrls = Arrays.stream(scanClassPath.split(File.pathSeparator)).map(Paths::get).map(path -> {
            try
            {
                return path.toUri().toURL();
            }
            catch (MalformedURLException e)
            {
                throw new RuntimeException(e);
            }
        }).collect(toList());

        Reflections reflections = new Reflections(new ConfigurationBuilder()
                                                  .forPackage(packageName)
                                                  .setScanners(Scanners.MethodsAnnotated, Scanners.SubTypes)
                                                  .setUrls(scanClassPathUrls)
                                                  .setExpandSuperTypes(true)
                                                  .setParallel(true));

        Class<? extends Annotation> annotationClass;
        try
        {
            annotationClass = (Class<? extends Annotation>) Class.forName(annotationName);
        }
        catch (ClassNotFoundException e)
        {
            throw new RuntimeException(e);
        }
        Set<Method> methodsAnnotatedWith = reflections.getMethodsAnnotatedWith(annotationClass);
        Stream<? extends Class<?>> stream = methodsAnnotatedWith.stream().map(Method::getDeclaringClass).distinct();

        if (expand)
            stream = stream.flatMap(c -> expand(c, reflections));
        if (normalize)
            stream = stream.map(this::normalize);

        Predicate<String> patternPredicate = Predicate.not(Pattern.compile(regex).asMatchPredicate());
        List<String> classes = stream.map(Class::getCanonicalName)
                                     .distinct()
                                     .sorted()
                                     .peek(verbose ? System.out::println : Consumers.nop())
                                     .filter(patternPredicate)
                                     .collect(toList());

        if (!classes.isEmpty())
            throw new RuntimeException(String.format("Detected classes that have a bad naming convention. All classes from the following locations %s which have methods annotated with %s should have names that match %s: \n%s", scanClassPath, annotationName, regex, String.join("\n", classes)));
    }

    /**
     * Get top outer class if it is an inner class
     */
    private Class<?> normalize(Class<?> klass)
    {
        while (klass.getEnclosingClass() != null)
            klass = klass.getEnclosingClass();

        return klass;
    }

    /**
     * Expand a class to all its subtypes. We need this because it possible that there is a top level class with
     * annotated test methods and there are subclasses which modifies some configuration but do not introduce
     * any additional test methods. In such case, those subclasses would not be included in the result of
     * {@link Reflections#getMethodsAnnotatedWith(Class)}.
     */
    private Stream<? extends Class<?>> expand(Class<?> klass, Reflections reflections)
    {
        Set<? extends Class<?>> subTypes = reflections.getSubTypesOf(klass);
        if (subTypes == null || subTypes.isEmpty())
            return Stream.of(klass);
        Stream<? extends Class<?>> subs = subTypes.stream();
        // assume we include if not abstract
        if (!Modifier.isAbstract(klass.getModifiers()))
            subs = Stream.concat(Stream.of(klass), subs);
        return subs;
    }

    public static void main(String[] args)
    {
        TestNameCheckTask check = new TestNameCheckTask();
        // checkstyle: suppress below 'blockSystemPropertyUsage'
        Optional.ofNullable(System.getProperty("scanClassPath")).ifPresent(check::setScanClassPath);
        Optional.ofNullable(System.getProperty("packageName")).ifPresent(check::setPackageName);
        Optional.ofNullable(System.getProperty("annotationName")).ifPresent(check::setAnnotationName);
        Optional.ofNullable(System.getProperty("regex")).ifPresent(check::setRegex);
        Optional.ofNullable(System.getProperty("expand")).map(Boolean::parseBoolean).ifPresent(check::setExpand);
        Optional.ofNullable(System.getProperty("normalize")).map(Boolean::parseBoolean).ifPresent(check::setNormalize);
        Optional.ofNullable(System.getProperty("verbose")).map(Boolean::parseBoolean).ifPresent(check::setVerbose);
        check.execute();
    }

}
