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

package org.apache.cassandra.distributed;

import com.google.common.base.Predicate;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.utils.Pair;

import java.net.URL;
import java.net.URLClassLoader;
import java.util.HashSet;
import java.util.Set;
import java.util.function.IntFunction;

public class InstanceClassLoader extends URLClassLoader
{
    // Classes that have to be shared between instances, for configuration or returning values
    private final static Class<?>[] commonClasses = new Class[]
            {
                    Pair.class,
                    InstanceConfig.class,
                    Message.class,
                    InetAddressAndPort.class,
                    InvokableInstance.SerializableBiConsumer.class,
                    InvokableInstance.SerializableBiFunction.class,
                    InvokableInstance.SerializableCallable.class,
                    InvokableInstance.SerializableConsumer.class,
                    InvokableInstance.SerializableFunction.class,
                    InvokableInstance.SerializableRunnable.class,
                    InvokableInstance.SerializableTriFunction.class,
                    InvokableInstance.InstanceFunction.class
            };

    private final int id; // for debug purposes
    private final ClassLoader commonClassLoader;
    private final Predicate<String> isCommonClassName;

    InstanceClassLoader(int id, URL[] urls, Predicate<String> isCommonClassName, ClassLoader commonClassLoader)
    {
        super(urls, null);
        this.id = id;
        this.commonClassLoader = commonClassLoader;
        this.isCommonClassName = isCommonClassName;
    }

    @Override
    public Class<?> loadClass(String name) throws ClassNotFoundException
    {
        // Do not share:
        //  * yaml, which  is a rare exception because it does mess with loading org.cassandra...Config class instances
        //  * most of the rest of Cassandra classes (unless they were explicitly shared) g
        if (name.startsWith("org.slf4j") ||
                name.startsWith("ch.qos.logback") ||
                name.startsWith("org.yaml") ||
                (name.startsWith("org.apache.cassandra") && !isCommonClassName.apply(name)))
            return loadClassInternal(name);

        return commonClassLoader.loadClass(name);
    }

    Class<?> loadClassInternal(String name) throws ClassNotFoundException
    {
        synchronized (getClassLoadingLock(name))
        {
            // First, check if the class has already been loaded
            Class<?> c = findLoadedClass(name);

            if (c == null)
                c = findClass(name);

            return c;
        }
    }

    public static IntFunction<ClassLoader> createFactory(URLClassLoader contextClassLoader)
    {
        Set<String> commonClassNames = new HashSet<>();
        for (Class<?> k : commonClasses)
            commonClassNames.add(k.getName());

        URL[] urls = contextClassLoader.getURLs();
        return id -> new InstanceClassLoader(id, urls, commonClassNames::contains, contextClassLoader);
    }

    /**
     * @return true iff this class was loaded by an InstanceClassLoader, and as such is used by a dtest node
     */
    public static boolean wasLoadedByAnInstanceClassLoader(Class<?> clazz)
    {
        return clazz.getClassLoader().getClass().getName().equals(InstanceClassLoader.class.getName());
    }

}
