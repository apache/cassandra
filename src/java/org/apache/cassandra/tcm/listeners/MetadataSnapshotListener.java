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

package org.apache.cassandra.tcm.listeners;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.ClusterMetadataService;
import org.apache.cassandra.tcm.Sealed;
import org.apache.cassandra.tcm.Transformation;
import org.apache.cassandra.tcm.log.Entry;

public class MetadataSnapshotListener implements LogListener
{
    private static final Logger logger = LoggerFactory.getLogger(MetadataSnapshotListener.class);
    @Override
    public void notify(Entry entry, Transformation.Result result)
    {
        ClusterMetadata next = result.success().metadata;
        if (entry.transform.kind() == Transformation.Kind.SEAL_PERIOD)
        {
            assert next.lastInPeriod;
            try
            {
                ClusterMetadataService.instance().snapshotManager().storeSnapshot(next);
                Sealed.recordSealedPeriod(next.period, next.epoch);
            }
            catch (Throwable e)
            {
                logger.warn("Unable to serialize metadata snapshot triggered by SealPeriod transformation", e);
            }
        }
    }
}
