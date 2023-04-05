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
package org.apache.cassandra.tools;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.management.ObjectName;
import javax.management.openmbean.CompositeData;
import javax.management.openmbean.OpenDataException;
import javax.management.openmbean.TabularData;

import com.google.common.collect.ImmutableSet;
import org.junit.Assert;
import org.junit.Test;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.utils.BreaksJMX;
import org.assertj.core.api.Assertions;
import org.reflections.Reflections;
import org.reflections.scanners.Scanners;
import org.reflections.util.ConfigurationBuilder;

public class JMXStandardsTest
{
    private static final Logger logger = LoggerFactory.getLogger(JMXStandardsTest.class);

    /**
     * JMX typlically works well with java.* and javax.*, but not all types are serializable and will work, so this class
     * goes with a explicit approval list, new APIs may fail if a java.* or javax.* is used not in this allow list, if
     * that is the case it is fine to add here.
     * <p>
     * It is never fine to allow non java.* and javax.* types, they can not be handled by clients, so should never be
     * allowed.
     */
    private static final Set<Class<?>> ALLOWED_TYPES = ImmutableSet.<Class<?>>builder()
                                                       .add(Void.class).add(Void.TYPE)
                                                       .add(Boolean.class).add(Boolean.TYPE)
                                                       .add(Byte.class).add(Byte.TYPE)
                                                       .add(Short.class).add(Short.TYPE)
                                                       .add(Integer.class).add(Integer.TYPE)
                                                       .add(Long.class).add(Long.TYPE)
                                                       .add(Float.class).add(Float.TYPE)
                                                       .add(Double.class).add(Double.TYPE)
                                                       .add(String.class)
                                                       .add(ByteBuffer.class)
                                                       .add(InetAddress.class)
                                                       .add(File.class)
                                                       .add(List.class).add(Map.class).add(Set.class).add(SortedMap.class).add(Collection.class)
                                                       .add(ObjectName.class).add(TabularData.class).add(CompositeData.class)
                                                       // Exceptions
                                                       // https://www.oracle.com/java/technologies/javase/management-extensions-best-practices.html
                                                       // "It is recommended that exceptions thrown by MBeans be drawn from
                                                       // the standard set defined in the java.* and javax.* packages on the
                                                       // Java SE platform. If an MBean throws a non-standard exception, a
                                                       // client that does not have that exception class will likely see
                                                       // another exception such as ClassNotFoundException instead."
                                                       .add(ExecutionException.class)
                                                       .add(InterruptedException.class)
                                                       .add(UnknownHostException.class)
                                                       .add(IOException.class)
                                                       .add(TimeoutException.class)
                                                       .add(IllegalStateException.class)
                                                       .add(ClassNotFoundException.class)
                                                       .add(OpenDataException.class)
                                                       .build();
    /**
     * This list is a set of types under java.* and javax.*, but are too vague that could cause issues; this does not
     * mean issues will happen with JMX, only that issues may happen only after running and can not be detected at
     * compile time.
     */
    private static final Set<Class<?>> DANGEROUS_TYPES = ImmutableSet.<Class<?>>builder()
                                                         .add(Object.class)
                                                         .add(Comparable.class)
                                                         .add(Serializable.class)
                                                         .add(Exception.class)
                                                         .build();

    @Test
    public void interfaces() throws ClassNotFoundException
    {
        Reflections reflections = new Reflections(ConfigurationBuilder.build("org.apache.cassandra").setExpandSuperTypes(false));
        Pattern mbeanPattern = Pattern.compile(".*MBean$");
        Set<String> matches = reflections.getAll(Scanners.SubTypes).stream()
                                         .filter(s -> mbeanPattern.matcher(s).find())
                                         .collect(Collectors.toSet());

        List<String> warnings = new ArrayList<>();
        List<String> errors = new ArrayList<>();
        for (String className : matches)
        {
            for (Class<?> klass = Class.forName(className); klass != null && !Object.class.equals(klass); klass = klass.getSuperclass())
            {
                Assertions.assertThat(klass).isInterface();
                Method[] methods = klass.getDeclaredMethods();
                for (int i = 0; i < methods.length; i++)
                {
                    Method method = methods[i];
                    checkType(method, "return", method.getGenericReturnType(), warnings, errors);
                    Stream.of(method.getGenericParameterTypes()).forEach(t -> checkType(method, "parameter", t, warnings, errors));
                    Stream.of(method.getGenericExceptionTypes()).forEach(t -> checkType(method, "throws", t, warnings, errors));
                }
            }
        }
        if (!warnings.isEmpty())
            warnings.forEach(logger::warn);
        if (!errors.isEmpty())
            throw new AssertionError("Errors detected while validating MBeans\n" + String.join("\n", errors));
    }

    private static void checkType(Method method, String sig, Type type, Collection<String> warnings, Collection<String> errors)
    {
        if (type instanceof Class<?>)
        {
            Class<?> klass = (Class<?>) type;
            int numArrays = 0;
            while (klass.isArray())
            {
                numArrays++;
                klass = klass.getComponentType();
            }
            if (!ALLOWED_TYPES.contains(klass))
            {
                StringBuilder typeName = new StringBuilder(klass.getCanonicalName());
                for (int i = 0; i < numArrays; i++)
                    typeName.append("[]");
                if (DANGEROUS_TYPES.contains(klass))
                {
                    warnings.add(String.format("Dangerous type used at signature %s, type %s; method '%s'", sig, typeName, method));
                }
                else
                {
                    String msg = String.format("Error at signature %s; type %s is not in the supported set of types, method method '%s'", sig, typeName, method);
                    (method.isAnnotationPresent(BreaksJMX.class) ? warnings : errors).add(msg);
                }
            }
        }
        else if (type instanceof ParameterizedType)
        {
            ParameterizedType param = (ParameterizedType) type;
            Type klass = param.getRawType();
            Type[] args = param.getActualTypeArguments();
            checkType(method, sig + ": " + param, klass, warnings, errors);
            Stream.of(args).forEach(t -> checkType(method, sig + " of " + param, t, warnings, errors));
        }
        else
        {
            Assert.fail("Unknown type: " + type.getClass());
        }
    }
}
