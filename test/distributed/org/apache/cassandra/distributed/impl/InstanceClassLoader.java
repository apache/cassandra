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

package org.apache.cassandra.distributed.impl;

import com.google.common.base.Predicate;
import org.apache.cassandra.config.ParameterizedClass;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.utils.Pair;

import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

public class InstanceClassLoader extends URLClassLoader
{
    // Classes that have to be shared between instances, for configuration or returning values
    private static final Set<String> sharedClassNames = Arrays.stream(new Class[]
            {
                    Pair.class,
                    InetAddressAndPort.class,
                    ParameterizedClass.class,
                    IInvokableInstance.class,
                    NetworkTopology.class
            })
            .map(Class::getName)
            .collect(Collectors.toSet());

    private static final Predicate<String> sharePackage = name ->
               name.startsWith("org.apache.cassandra.distributed.api.")
            || name.startsWith("sun.")
            || name.startsWith("oracle.")
            || name.startsWith("com.intellij.")
            || name.startsWith("com.sun.")
            || name.startsWith("com.sun.")
            || name.startsWith("java.")
            || name.startsWith("javax.")
            || name.startsWith("jdk.")
            || name.startsWith("netscape.")
            || name.startsWith("org.xml.sax.");

    private static final Predicate<String> shareClass = name -> sharePackage.apply(name) || sharedClassNames.contains(name);

    public static interface Factory
    {
        InstanceClassLoader create(int id, URL[] urls, ClassLoader sharedClassLoader);
    }

    private volatile boolean isClosed = false;
    private final URL[] urls;
    private final int generation; // used to help debug class loader leaks, by helping determine which classloaders should have been collected
    private final int id;
    private final ClassLoader sharedClassLoader;

    InstanceClassLoader(int generation, int id, URL[] urls, ClassLoader sharedClassLoader)
    {
        super(urls, null);
        this.urls = urls;
        this.sharedClassLoader = sharedClassLoader;
        this.generation = generation;
        this.id = id;
    }

    @Override
    public Class<?> loadClass(String name) throws ClassNotFoundException
    {
        if (shareClass.apply(name))
            return sharedClassLoader.loadClass(name);

        return loadClassInternal(name);
    }

    Class<?> loadClassInternal(String name) throws ClassNotFoundException
    {
        if (isClosed)
            throw new IllegalStateException(String.format("Can't load %s. Instance class loader is already closed.", name));

        synchronized (getClassLoadingLock(name))
        {
            // First, check if the class has already been loaded
            Class<?> c = findLoadedClass(name);

            if (c == null)
                c = findClass(name);

            return c;
        }
    }

    /**
     * @return true iff this class was loaded by an InstanceClassLoader, and as such is used by a dtest node
     */
    public static boolean wasLoadedByAnInstanceClassLoader(Class<?> clazz)
    {
        return clazz.getClassLoader().getClass().getName().equals(InstanceClassLoader.class.getName());
    }

    public String toString()
    {
        return "InstanceClassLoader{" +
               "generation=" + generation +
               ", id = " + id +
               ", urls=" + Arrays.toString(urls) +
               '}';
    }

    public void close() throws IOException
    {
        isClosed = true;
        super.close();
    }
}
