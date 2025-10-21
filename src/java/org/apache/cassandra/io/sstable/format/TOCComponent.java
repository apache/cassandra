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
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import com.google.common.collect.Collections2;
import com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.io.FSWriteError;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.format.SSTableFormat.Components;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.FileUtils;

import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.SYNC;
import static java.nio.file.StandardOpenOption.TRUNCATE_EXISTING;

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
     * Updates the TOC file by reading existing component entries, merging them with the given components,
     * sorting the combined list in lexicographic order for deterministic output.
     *
     * @param descriptor the SSTable descriptor for which to update the TOC
     * @param components new components to merge into the TOC (existing TOC entries are preserved)
     * @throws FSWriteError if an I/O error occurs when creating or overwriting the TOC file
     */
    public static void updateTOC(Descriptor descriptor, Collection<Component> components)
    {
        if (components.isEmpty())
            return;

        File tocFile = descriptor.fileFor(Components.TOC);

        Set<String> componentNames = new TreeSet<>(Collections2.transform(components, Component::name));

        if (tocFile.exists())
            componentNames.addAll(FileUtils.readLines(tocFile));

        try
        {
            FileUtils.write(tocFile, new ArrayList<>(componentNames), CREATE, TRUNCATE_EXISTING, SYNC);
        }
        catch (RuntimeException ex)
        {
            throw new RuntimeException("Exception occurred while writing to " + tocFile,
                                       ex.getCause() != null ? ex.getCause() : ex);
        }
    }

    /**
     * Loads existing TOC file or creates a new one.
     *
     * @param descriptor descriptor to load TOC for
     * @return set of loaded or discovered components
     * @throws IOError when loading of a component is erroneous
     * @throws FSWriteError when creating of a new TOC file is erroneous
     */
    public static Set<Component> loadOrCreate(Descriptor descriptor)
    {
        try
        {
            return TOCComponent.loadTOC(descriptor);
        }
        catch (FileNotFoundException | NoSuchFileException e)
        {
            // if TOC is missing, we might create a new one
        }
        catch (IOException ex)
        {
            throw new IOError(ex);
        }

        Set<Component> components = descriptor.discoverComponents();
        if (components.isEmpty())
            return components; // sstable doesn't exist yet

        components.add(Components.TOC);
        TOCComponent.updateTOC(descriptor, components);
        return components;
    }

    /**
     * Rewrites the TOC component by deleting and recreating it only with provided component names.
     */
    public static void rewriteTOC(Descriptor descriptor, Collection<Component> components)
    {
        File tocFile = descriptor.fileFor(Components.TOC);
        if (!tocFile.tryDelete())
            logger.error("Failed to delete TOC component for {}", descriptor);
        updateTOC(descriptor, components);
    }
}
