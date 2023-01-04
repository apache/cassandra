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
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.metadata.ValidationMetadata;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.FileInputStreamPlus;
import org.apache.cassandra.io.util.FileOutputStreamPlus;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.AlwaysPresentFilter;
import org.apache.cassandra.utils.BloomFilterSerializer;
import org.apache.cassandra.utils.FilterFactory;
import org.apache.cassandra.utils.IFilter;

public class FilterComponent
{
    private static final Logger logger = LoggerFactory.getLogger(FilterComponent.class);

    final static boolean rebuildFilterOnFPChanceChange = false;
    final static double filterFPChanceTolerance = 0d;

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

    /**
     * Optionally loads a Bloom filter. If the filter is not needed (FP chance is neglectable), it sets
     * {@link AlwaysPresentFilter} as a filter in the builder. If the filter is expected to be recreated for various
     * reasons, it leaves it {@code null} (unchanged). Otherwise, it attempts to load the filter, and if it succeeds,
     * it is set in the builder. If a filter fails to load, it is left {@code null} (unchanged) meaning that it should
     * be rebuilt.
     */
    public static IFilter maybeLoadBloomFilter(Descriptor descriptor, Set<Component> components, TableMetadata metadata, ValidationMetadata validationMetadata)
    {
        double currentFPChance = validationMetadata != null ? validationMetadata.bloomFilterFPChance : Double.NaN;
        double desiredFPChance = metadata.params.bloomFilterFpChance;

        IFilter filter = null;
        if (!shouldUseBloomFilter(desiredFPChance))
        {
            if (logger.isTraceEnabled())
                logger.trace("Bloom filter for {} will not be loaded because fpChance={} is neglectable", descriptor, desiredFPChance);

            return FilterFactory.AlwaysPresent;
        }
        else if (!components.contains(Component.FILTER) || Double.isNaN(currentFPChance))
        {
            if (logger.isTraceEnabled())
                logger.trace("Bloom filter for {} will not be loaded because filter component is missing or sstable lacks validation metadata", descriptor);

            return null;
        }
        else if (!isFPChanceDiffNeglectable(desiredFPChance, currentFPChance) && rebuildFilterOnFPChanceChange)
        {
            if (logger.isTraceEnabled())
                logger.trace("Bloom filter for {} will not be loaded because fpChance has changed from {} to {} and the filter should be recreated", descriptor, currentFPChance, desiredFPChance);

            return null;
        }

        try
        {
            filter = load(descriptor);
            if (filter == null || filter instanceof AlwaysPresentFilter)
                logger.info("Bloom filter for {} is missing or invalid", descriptor);
        }
        catch (IOException ex)
        {
            logger.info("Bloom filter for " + descriptor + " could not be deserialized", ex);
        }

        return filter;
    }

    static boolean shouldUseBloomFilter(double fpChance)
    {
        return !(Math.abs(1 - fpChance) <= filterFPChanceTolerance);
    }

    static boolean isFPChanceDiffNeglectable(double fpChance1, double fpChance2)
    {
        return Math.abs(fpChance1 - fpChance2) <= filterFPChanceTolerance;
    }
}