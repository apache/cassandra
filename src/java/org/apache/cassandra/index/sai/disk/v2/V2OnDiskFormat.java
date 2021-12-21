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

package org.apache.cassandra.index.sai.disk.v2;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.EnumSet;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.ClusteringComparator;
import org.apache.cassandra.index.sai.disk.PerSSTableWriter;
import org.apache.cassandra.index.sai.disk.PrimaryKeyMap;
import org.apache.cassandra.index.sai.disk.format.IndexComponent;
import org.apache.cassandra.index.sai.disk.format.IndexDescriptor;
import org.apache.cassandra.index.sai.disk.format.IndexFeatureSet;
import org.apache.cassandra.index.sai.disk.v1.V1OnDiskFormat;
import org.apache.cassandra.index.sai.utils.PrimaryKey;
import org.apache.cassandra.index.sai.utils.SAICodecUtils;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.lucene.store.IndexInput;

public class V2OnDiskFormat extends V1OnDiskFormat
{
    private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private static final Set<IndexComponent> PER_SSTABLE_COMPONENTS = EnumSet.of(IndexComponent.GROUP_COMPLETION_MARKER,
                                                                                 IndexComponent.GROUP_META,
                                                                                 IndexComponent.TOKEN_VALUES,
                                                                                 IndexComponent.PRIMARY_KEY_TRIE,
                                                                                 IndexComponent.PRIMARY_KEY_BLOCKS,
                                                                                 IndexComponent.PRIMARY_KEY_BLOCK_OFFSETS);

    public static final V2OnDiskFormat instance = new V2OnDiskFormat();

    private static final IndexFeatureSet v2IndexFeatureSet = new IndexFeatureSet()
    {
        @Override
        public boolean isRowAware()
        {
            return true;
        }
    };

    protected V2OnDiskFormat()
    {}

    @Override
    public IndexFeatureSet indexFeatureSet()
    {
        return v2IndexFeatureSet;
    }

    @Override
    public PrimaryKey.Factory primaryKeyFactory(ClusteringComparator comparator)
    {
        return new RowAwarePrimaryKeyFactory(comparator);
    }

    @Override
    public PrimaryKeyMap.Factory newPrimaryKeyMapFactory(IndexDescriptor indexDescriptor, SSTableReader sstable) throws IOException
    {
        return new RowAwarePrimaryKeyMap.RowAwarePrimaryKeyMapFactory(indexDescriptor, sstable);
    }

    @Override
    public PerSSTableWriter newPerSSTableWriter(IndexDescriptor indexDescriptor) throws IOException
    {
        return new SSTableComponentsWriter(indexDescriptor);
    }

    @Override
    public boolean validatePerSSTableComponents(IndexDescriptor indexDescriptor, boolean checksum)
    {
        for (IndexComponent indexComponent : perSSTableComponents())
        {
            if (isBuildCompletionMarker(indexComponent))
                continue;
            try (IndexInput input = indexDescriptor.openPerSSTableInput(indexComponent))
            {
                if (checksum)
                    SAICodecUtils.validateChecksum(input);
                else
                    SAICodecUtils.validate(input);
            }
            catch (Throwable e)
            {
                if (logger.isDebugEnabled())
                {
                    logger.debug(indexDescriptor.logMessage("{} failed for index component {} on SSTable {}"),
                                 (checksum ? "Checksum validation" : "Validation"),
                                 indexComponent,
                                 indexDescriptor.descriptor);
                }
                return false;
            }
        }
        return true;
    }

    @Override
    public Set<IndexComponent> perSSTableComponents()
    {
        return PER_SSTABLE_COMPONENTS;
    }

    @Override
    public int openFilesPerSSTable()
    {
        return 4;
    }
}
