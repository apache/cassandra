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
import org.apache.cassandra.io.sstable.format.SSTableFormat.Components;
import org.apache.cassandra.io.sstable.metadata.ValidationMetadata;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.FileInputStreamPlus;
import org.apache.cassandra.io.util.FileOutputStreamPlus;
import org.apache.cassandra.schema.TableMetadata;
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
        File filterFile = descriptor.fileFor(Components.FILTER);

        if (!filterFile.exists())
            return null;

        if (filterFile.length() == 0)
            return FilterFactory.AlwaysPresent;

        try (FileInputStreamPlus stream = descriptor.fileFor(Components.FILTER).newInputStream())
        {
            return BloomFilterSerializer.forVersion(descriptor.version.hasOldBfFormat()).deserialize(stream);
        }
        catch (IOException ex)
        {
            throw new IOException("Failed to load Bloom filter for SSTable: " + descriptor.baseFile(), ex);
        }
    }

    public static void save(IFilter filter, Descriptor descriptor, boolean deleteOnFailure) throws IOException
    {
        File filterFile = descriptor.fileFor(Components.FILTER);
        try (FileOutputStreamPlus stream = filterFile.newOutputStream(File.WriteMode.OVERWRITE))
        {
            filter.serialize(stream, descriptor.version.hasOldBfFormat());
            stream.flush();
            stream.sync();
        }
        catch (IOException ex)
        {
            if (deleteOnFailure)
                descriptor.fileFor(Components.FILTER).deleteIfExists();
            throw new IOException("Failed to save Bloom filter for SSTable: " + descriptor.baseFile(), ex);
        }
    }

    /**
     * Optionally loads a Bloom filter.
     * If the filter is not needed (FP chance is neglectable), it returns {@link FilterFactory#AlwaysPresent}.
     * If the filter is expected to be recreated for various reasons the method returns {@code null}.
     * Otherwise, an attempt to load the filter is made and if it succeeds, the loaded filter is returned.
     * If loading fails, the method returns {@code null}.
     *
     * @return {@link FilterFactory#AlwaysPresent}, loaded filter or {@code null} (which means that the filter should be rebuilt)
     */
    public static IFilter maybeLoadBloomFilter(Descriptor descriptor, Set<Component> components, TableMetadata metadata, ValidationMetadata validationMetadata)
    {
        double currentFPChance = validationMetadata != null ? validationMetadata.bloomFilterFPChance : Double.NaN;
        double desiredFPChance = metadata.params.bloomFilterFpChance;

        IFilter filter = null;
        if (!shouldUseBloomFilter(desiredFPChance))
        {
            if (logger.isTraceEnabled())
                logger.trace("Bloom filter for {} will not be loaded because fpChance={} is negligible", descriptor, desiredFPChance);

            return FilterFactory.AlwaysPresent;
        }
        else if (!components.contains(Components.FILTER) || Double.isNaN(currentFPChance))
        {
            if (logger.isTraceEnabled())
                logger.trace("Bloom filter for {} will not be loaded because the filter component is missing or sstable lacks validation metadata", descriptor);

            return null;
        }
        else if (!isFPChanceDiffNegligible(desiredFPChance, currentFPChance) && rebuildFilterOnFPChanceChange)
        {
            if (logger.isTraceEnabled())
                logger.trace("Bloom filter for {} will not be loaded because fpChance has changed from {} to {} and the filter should be recreated", descriptor, currentFPChance, desiredFPChance);

            return null;
        }

        try
        {
            filter = load(descriptor);
            if (filter == null || !filter.isInformative())
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

    static boolean isFPChanceDiffNegligible(double fpChance1, double fpChance2)
    {
        return Math.abs(fpChance1 - fpChance2) <= filterFPChanceTolerance;
    }
}
