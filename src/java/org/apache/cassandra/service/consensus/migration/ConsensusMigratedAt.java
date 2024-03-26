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

package org.apache.cassandra.service.consensus.migration;

import java.io.IOException;
import javax.annotation.Nullable;

import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.tcm.Epoch;
import org.apache.cassandra.utils.NullableSerializer;

public class ConsensusMigratedAt
{
    public static final IVersionedSerializer<ConsensusMigratedAt> serializer = NullableSerializer.wrap(new IVersionedSerializer<ConsensusMigratedAt>()
    {
        @Override
        public void serialize(ConsensusMigratedAt t, DataOutputPlus out, int version) throws IOException
        {
            Epoch.messageSerializer.serialize(t.migratedAtEpoch, out, version);
            out.writeByte(t.migratedAtTarget.value);
        }

        @Override
        public ConsensusMigratedAt deserialize(DataInputPlus in, int version) throws IOException
        {
            Epoch migratedAtEpoch = Epoch.messageSerializer.deserialize(in, version);
            ConsensusMigrationTarget target = ConsensusMigrationTarget.fromValue(in.readByte());
            return new ConsensusMigratedAt(migratedAtEpoch, target);
        }

        @Override
        public long serializedSize(ConsensusMigratedAt t, int version)
        {
            return TypeSizes.sizeof(ConsensusMigrationTarget.accord.value)
                   + Epoch.messageSerializer.serializedSize(t.migratedAtEpoch, version);
        }
    });

    // Fields are not nullable when used for messaging
    @Nullable
    public final Epoch migratedAtEpoch;

    @Nullable
    public final ConsensusMigrationTarget migratedAtTarget;

    public ConsensusMigratedAt(Epoch migratedAtEpoch, ConsensusMigrationTarget migratedAtTarget)
    {
        this.migratedAtEpoch = migratedAtEpoch;
        this.migratedAtTarget = migratedAtTarget;
    }
}
