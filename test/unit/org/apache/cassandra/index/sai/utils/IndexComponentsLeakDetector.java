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
import org.apache.cassandra.index.sai.disk.io.IndexComponents;
import org.apache.cassandra.index.sai.disk.io.TrackingIndexComponents;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.util.SequentialWriterOption;
import org.apache.cassandra.schema.CompressionParams;
import org.apache.lucene.store.IndexInput;

import static org.junit.Assert.assertTrue;

public class IndexComponentsLeakDetector extends TestRuleAdapter
{
    private final static Set<TrackingIndexComponents> trackedIndexComponents = Collections.synchronizedSet(new HashSet<>());

    public IndexComponents newIndexComponents(String column, Descriptor descriptor, SequentialWriterOption sequentialWriterOption,
                                              CompressionParams params)
    {
        final TrackingIndexComponents components = new TrackingIndexComponents(column, descriptor, sequentialWriterOption, params);
        trackedIndexComponents.add(components);
        return components;
    }

    @Override
    protected void afterIfSuccessful()
    {
        for (TrackingIndexComponents components : trackedIndexComponents)
        {
            final Map<IndexInput, String> openInputs = components.getOpenInputs();
            assertTrue("Index components have open inputs: " + openInputs, openInputs.isEmpty());
        }
    }

    @Override
    protected void afterAlways(List<Throwable> errors)
    {
        trackedIndexComponents.clear();
    }
}
