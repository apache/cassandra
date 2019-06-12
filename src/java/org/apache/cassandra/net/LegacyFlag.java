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
package org.apache.cassandra.net;

import java.io.IOException;

import com.google.common.base.Preconditions;

import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;

/**
 * Before 4.0 introduced flags field to {@link Message}, we used to encode flags in params field,
 * using a dummy value (single byte set to 0). From now on, {@link MessageFlag} should be extended
 * instead.
 *
 * Once 3.0/3.11 compatibility is phased out, this class should be removed.
 */
@Deprecated
final class LegacyFlag
{
    static final LegacyFlag instance = new LegacyFlag();

    private LegacyFlag()
    {
    }

    static IVersionedSerializer<LegacyFlag> serializer = new IVersionedSerializer<LegacyFlag>()
    {
        public void serialize(LegacyFlag param, DataOutputPlus out, int version) throws IOException
        {
            Preconditions.checkArgument(param == instance);
            out.write(0);
        }

        public LegacyFlag deserialize(DataInputPlus in, int version) throws IOException
        {
            byte b = in.readByte();
            assert b == 0;
            return instance;
        }

        public long serializedSize(LegacyFlag param, int version)
        {
            Preconditions.checkArgument(param == instance);
            return 1;
        }
    };
}
