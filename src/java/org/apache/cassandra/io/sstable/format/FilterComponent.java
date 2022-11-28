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

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.FileInputStreamPlus;
import org.apache.cassandra.io.util.FileOutputStreamPlus;
import org.apache.cassandra.utils.BloomFilterSerializer;
import org.apache.cassandra.utils.FilterFactory;
import org.apache.cassandra.utils.IFilter;

public class FilterComponent
{
    private static final Logger logger = LoggerFactory.getLogger(FilterComponent.class);

    private FilterComponent()
    {
    }

    /**
     * Load bloom filter from Filter.db file.
     */
    public static IFilter load(Descriptor descriptor) throws IOException
    {
        File filterFile = descriptor.fileFor(Component.FILTER);

        if (!filterFile.exists())
            return null;

        if (filterFile.length() == 0)
            return FilterFactory.AlwaysPresent;

        try (FileInputStreamPlus stream = descriptor.fileFor(Component.FILTER).newInputStream())
        {
            return BloomFilterSerializer.forVersion(descriptor.version.hasOldBfFormat()).deserialize(stream);
        }
        catch (IOException ex)
        {
            throw new IOException("Failed to load Bloom filter for SSTable: " + descriptor.baseFilename(), ex);
        }
    }

    public static void save(IFilter filter, Descriptor descriptor) throws IOException
    {
        File filterFile = descriptor.fileFor(Component.FILTER);
        try (FileOutputStreamPlus stream = filterFile.newOutputStream(File.WriteMode.OVERWRITE))
        {
            filter.serialize(stream, descriptor.version.hasOldBfFormat());
            stream.flush();
            stream.sync(); // is it needed if we close the file right after that?
        }
        catch (IOException ex)
        {
            throw new IOException("Failed to save Bloom filter for SSTable: " + descriptor.baseFilename(), ex);
        }
    }

    public static void saveOrDeleteCorrupted(Descriptor descriptor, IFilter filter) throws IOException
    {
        try
        {
            save(filter, descriptor);
        }
        catch (IOException ex)
        {
            descriptor.fileFor(Component.FILTER).deleteIfExists();
            throw ex;
        }
    }
}