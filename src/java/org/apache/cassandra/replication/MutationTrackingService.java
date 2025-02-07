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

package org.apache.cassandra.replication;

import java.io.IOException;

import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.replication.simple.SimpleMutationSummary;
import org.apache.cassandra.replication.simple.SimpleMutationTracker;
import org.apache.cassandra.replication.simple.SimpleReconciliationPlan;
import org.apache.cassandra.service.reads.logged.ReadReconciliations;

public class MutationTrackingService
{
    private static final MutationTracker tracker = new SimpleMutationTracker();

    private static final ReadReconciliations reconciliations = new ReadReconciliations();

    private MutationTrackingService() {}

    public static MutationTracker instance()
    {
        return tracker;
    }

    public static ReadReconciliations reconciliations()
    {
        return reconciliations;
    }

    public static final IVersionedSerializer<MutationSummary> summarySerializer = new IVersionedSerializer<MutationSummary>()
    {
        @Override
        public void serialize(MutationSummary summary, DataOutputPlus out, int version) throws IOException
        {
            SimpleMutationSummary.serializer.serialize((SimpleMutationSummary) summary, out, version);

        }

        @Override
        public MutationSummary deserialize(DataInputPlus in, int version) throws IOException
        {
            return SimpleMutationSummary.serializer.deserialize(in, version);
        }

        @Override
        public long serializedSize(MutationSummary summary, int version)
        {
            return SimpleMutationSummary.serializer.serializedSize((SimpleMutationSummary) summary, version);
        }
    };

    public static final IVersionedSerializer<ReconciliationPlan> reconciliationPlanSerializer = new IVersionedSerializer<ReconciliationPlan>()
    {
        @Override
        public void serialize(ReconciliationPlan plan, DataOutputPlus out, int version) throws IOException
        {
            SimpleReconciliationPlan.serializer.serialize((SimpleReconciliationPlan) plan, out, version);
        }

        @Override
        public ReconciliationPlan deserialize(DataInputPlus in, int version) throws IOException
        {
            return SimpleReconciliationPlan.serializer.deserialize(in, version);
        }

        @Override
        public long serializedSize(ReconciliationPlan plan, int version)
        {
            return SimpleReconciliationPlan.serializer.serializedSize((SimpleReconciliationPlan) plan, version);
        }
    };
}
