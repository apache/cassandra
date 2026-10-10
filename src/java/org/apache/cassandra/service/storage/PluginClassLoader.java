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

package org.apache.cassandra.service.storage;

import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Enumeration;
import java.util.List;

public class PluginClassLoader extends URLClassLoader
{
    private static final String[] PARENT_FIRST = {
    "java.", "javax.", "jdk.", "sun.",                       // the platform
    "org.apache.cassandra.service.storage.",                 // the SPI itself
    "org.apache.cassandra.io.util.ChannelProxy",             // types in the SPI signature
    "org.apache.cassandra.io.util.File",
    "org.apache.cassandra.notifications.",                   // INotificationConsumer and what it is handed
    "org.slf4j.",                                            // one logging binding, not two
    };

    static
    {
        registerAsParallelCapable();
    }

    public PluginClassLoader(URL[] urls, ClassLoader parent)
    {
        super(urls, parent);
    }

    private static boolean parentFirst(String name)
    {
        for (String prefix : PARENT_FIRST)
            if (name.startsWith(prefix))
                return true;
        return false;
    }

    @Override
    protected Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException
    {
        synchronized (getClassLoadingLock(name))
        {
            Class<?> c = findLoadedClass(name);
            if (c == null)
            {
                if (parentFirst(name))
                {
                    c = super.loadClass(name, false);
                }
                else
                {
                    try
                    {
                        c = findClass(name); // provider's own copy wins
                    }
                    catch (ClassNotFoundException e)
                    {
                        c = super.loadClass(name, false);
                    }
                }
            }
            if (resolve)
                resolveClass(c);

            return c;
        }
    }

    /**
     * Child-first for resources as well: ServiceLoader locates implementations through
     * META-INF/services/..., so the provider's own entry has to be seen before any the parent carries.
     */
    @Override
    public Enumeration<URL> getResources(String name) throws IOException
    {
        List<URL> urls = new ArrayList<>();
        Collections.list(findResources(name)).forEach(urls::add);
        Collections.list(getParent().getResources(name)).forEach(u -> {
            if (!urls.contains(u)) urls.add(u);
        });
        return Collections.enumeration(urls);
    }

    @Override
    public URL getResource(String name)
    {
        URL own = findResource(name);
        return own != null ? own : super.getResource(name);
    }
}
