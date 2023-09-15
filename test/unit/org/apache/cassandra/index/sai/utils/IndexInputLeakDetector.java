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
package org.apache.cassandra.index.sai.utils;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.carrotsearch.randomizedtesting.rules.TestRuleAdapter;
import org.apache.cassandra.index.sai.disk.format.IndexDescriptor;
import org.apache.cassandra.index.sai.disk.io.TrackingIndexFileUtils;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.util.SequentialWriterOption;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.lucene.store.IndexInput;

import static org.junit.Assert.assertTrue;

public class IndexInputLeakDetector extends TestRuleAdapter
{
    private final static Set<TrackingIndexFileUtils> trackedIndexFileUtils = Collections.synchronizedSet(new HashSet<>());

    public IndexDescriptor newIndexDescriptor(Descriptor descriptor, TableMetadata tableMetadata, SequentialWriterOption sequentialWriterOption)
    {
        TrackingIndexFileUtils trackingIndexFileUtils = new TrackingIndexFileUtils(sequentialWriterOption);
        trackedIndexFileUtils.add(trackingIndexFileUtils);
        return IndexDescriptor.create(descriptor, tableMetadata.partitioner, tableMetadata.comparator);
    }

    @Override
    protected void afterIfSuccessful()
    {
        for (TrackingIndexFileUtils fileUtils : trackedIndexFileUtils)
        {
            final Map<IndexInput, String> openInputs = fileUtils.getOpenInputs();
            assertTrue("Index components have open inputs: " + openInputs, openInputs.isEmpty());
        }
    }

    @Override
    protected void afterAlways(List<Throwable> errors)
    {
        trackedIndexFileUtils.clear();
    }
}
