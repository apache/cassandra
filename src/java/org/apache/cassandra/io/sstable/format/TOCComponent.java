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

package org.apache.cassandra.io.sstable.format;

import java.io.FileNotFoundException;
import java.io.IOError;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.util.Collection;
import java.util.List;
import java.util.Set;

import com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.io.FSWriteError;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.format.SSTableFormat.Components;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.FileOutputStreamPlus;

import static org.apache.cassandra.io.util.File.WriteMode.APPEND;

public class TOCComponent
{
    private static final Logger logger = LoggerFactory.getLogger(TOCComponent.class);

    /**
     * Reads the list of components from the TOC component.
     *
     * @return set of components found in the TOC
     */
    public static Set<Component> loadTOC(Descriptor descriptor) throws IOException
    {
        return loadTOC(descriptor, true);
    }

    /**
     * Reads the list of components from the TOC component.
     *
     * @param skipMissing skip adding the component to the returned set if the corresponding file is missing.
     * @return set of components found in the TOC
     */
    public static Set<Component> loadTOC(Descriptor descriptor, boolean skipMissing) throws IOException
    {
        File tocFile = descriptor.fileFor(Components.TOC);
        List<String> componentNames = Files.readAllLines(tocFile.toPath());
        Set<Component> components = Sets.newHashSetWithExpectedSize(componentNames.size());
        for (String componentName : componentNames)
        {
            Component component = Component.parse(componentName, descriptor.version.format);
            if (skipMissing && !descriptor.fileFor(component).exists())
                logger.error("Missing component: {}", descriptor.fileFor(component));
            else
                components.add(component);
        }
        return components;
    }

    /**
     * Appends new component names to the TOC component.
     */
    public static void appendTOC(Descriptor descriptor, Collection<Component> components)
    {
        File tocFile = descriptor.fileFor(Components.TOC);
        try (FileOutputStreamPlus out = tocFile.newOutputStream(APPEND);
             PrintWriter w = new PrintWriter(out))
        {
            for (Component component : components)
                w.println(component.name);
            w.flush();
            out.sync();
        }
        catch (IOException e)
        {
            throw new FSWriteError(e, tocFile);
        }
    }

    public static Set<Component> loadOrCreate(Descriptor descriptor)
    {
        try
        {
            try
            {
                return TOCComponent.loadTOC(descriptor);
            }
            catch (FileNotFoundException | NoSuchFileException e)
            {
                Set<Component> components = descriptor.discoverComponents();
                if (components.isEmpty())
                    return components; // sstable doesn't exist yet

                components.add(Components.TOC);
                TOCComponent.appendTOC(descriptor, components);
                return components;
            }
        }
        catch (IOException e)
        {
            throw new IOError(e);
        }
    }

    /**
     * Rewrite TOC components by deleting existing TOC file and append new components
     */
    public static void rewriteTOC(Descriptor descriptor, Collection<Component> components)
    {
        File tocFile = descriptor.fileFor(Components.TOC);
        if (!tocFile.tryDelete())
            logger.error("Failed to delete TOC component for " + descriptor);
        appendTOC(descriptor, components);
    }
}
