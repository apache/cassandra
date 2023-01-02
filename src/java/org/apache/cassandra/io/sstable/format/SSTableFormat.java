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

import java.util.List;
import java.util.Set;

import com.google.common.base.CharMatcher;

import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.GaugeProvider;
import org.apache.cassandra.io.sstable.format.big.BigFormat;
import org.apache.cassandra.schema.TableMetadataRef;

/**
 * Provides the accessors to data on disk.
 */
public interface SSTableFormat<R extends SSTableReader, W extends SSTableWriter>
{
    boolean enableSSTableDevelopmentTestMode = Boolean.getBoolean("cassandra.test.sstableformatdevelopment");

    Type getType();

    Version getLatestVersion();
    Version getVersion(String version);

    SSTableWriter.Factory getWriterFactory();
    SSTableReaderFactory<R, ?> getReaderFactory();

    Set<Component> supportedComponents();

    Set<Component> streamingComponents();

    Set<Component> primaryComponents();

    Set<Component> batchComponents();

    Set<Component> uploadComponents();

    Set<Component> mutableComponents();

    Set<Component> generatedOnLoadComponents();

    boolean isKeyCacheSupported();
    AbstractRowIndexEntry.KeyCacheValueSerializer<?, ?> getKeyCacheValueSerializer();

    R cast(SSTableReader sstr);

    W cast(SSTableWriter sstw);

    FormatSpecificMetricsProviders getFormatSpecificMetricsProviders();

    enum Type
    {
        //The original sstable format
        BIG("big", BigFormat.instance);

        public final SSTableFormat info;
        public final String name;

        public static Type current()
        {
            return BIG;
        }

        Type(String name, SSTableFormat info)
        {
            //Since format comes right after generation
            //we disallow formats with numeric names
            assert !CharMatcher.digit().matchesAllOf(name);

            this.name = name;
            this.info = info;
        }

        public static Type validate(String name)
        {
            for (Type valid : Type.values())
            {
                if (valid.name.equalsIgnoreCase(name))
                    return valid;
            }

            throw new IllegalArgumentException("No Type constant " + name);
        }
    }

    interface FormatSpecificMetricsProviders
    {
        List<GaugeProvider<?, ?>> getGaugeProviders();
    }

    interface SSTableReaderFactory<R extends SSTableReader, B extends SSTableReaderBuilder<R, B>>
    {
        SSTableReaderBuilder<R, B> builder(Descriptor descriptor);

        SSTableReaderLoadingBuilder<R, B> builder(Descriptor descriptor, TableMetadataRef tableMetadataRef, Set<Component> components);
    }
}