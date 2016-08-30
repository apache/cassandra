package org.apache.cassandra.triggers;
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


import java.io.File;
import java.io.FilenameFilter;
import java.io.IOError;
import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.io.Files;

/**
 * Custom class loader will load the classes from the class path, CCL will load
 * the classes from the the URL first, if it cannot find the required class it
 * will let the parent class loader do the its job.
 *
 * Note: If the CCL is GC'ed then the associated classes will be unloaded.
 */
public class CustomClassLoader extends URLClassLoader
{
    private static final Logger logger = LoggerFactory.getLogger(CustomClassLoader.class);
    private final Map<String, Class<?>> cache = new ConcurrentHashMap<String, Class<?>>();
    private final ClassLoader parent;

    public CustomClassLoader(ClassLoader parent)
    {
        super(new URL[] {}, parent);
        assert parent != null;
        this.parent = getParent();
    }

    public CustomClassLoader(ClassLoader parent, File classPathDir)
    {
        super(new URL[] {}, parent);
        assert parent != null;
        this.parent = getParent();
        addClassPath(classPathDir);
    }

    public void addClassPath(File dir)
    {
        if (dir == null || !dir.exists())
            return;
        FilenameFilter filter = new FilenameFilter()
        {
            public boolean accept(File dir, String name)
            {
                return name.endsWith(".jar");
            }
        };
        for (File inputJar : dir.listFiles(filter))
        {
            File lib = new File(System.getProperty("java.io.tmpdir"), "lib");
            if (!lib.exists())
            {
                lib.mkdir();
                lib.deleteOnExit();
            }
            try
            {
                File out = File.createTempFile("cassandra-", ".jar", lib);
                out.deleteOnExit();
                logger.info("Loading new jar {}", inputJar.getAbsolutePath());
                Files.copy(inputJar, out);
                addURL(out.toURI().toURL());
            }
            catch (IOException ex)
            {
                throw new IOError(ex);
            }
        }
    }

    @Override
    public Class<?> loadClass(String name) throws ClassNotFoundException
    {
        Class<?> clazz = cache.get(name);
        if (clazz == null)
            return loadClassInternal(name);
        return clazz;
    }

    public synchronized Class<?> loadClassInternal(String name) throws ClassNotFoundException
    {
        try
        {
            return parent.loadClass(name);
        }
        catch (ClassNotFoundException ex)
        {
            logger.trace("Class not found using parent class loader,", ex);
            // Don't throw the exception here, try triggers directory.
        }
        Class<?> clazz = this.findClass(name);
        cache.put(name, clazz);
        return clazz;
    }
}
