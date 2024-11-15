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

package org.apache.cassandra.service.accord.serializers;

import accord.api.Result;
import accord.primitives.ProgressToken;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;

public class ResultSerializers
{
    // TODO (expected): this is meant to encode e.g. whether the transaction's condition met or not
    public static final Result APPLIED = new Result()
    {
        @Override
        public ProgressToken asProgressToken()
        {
            return ProgressToken.APPLIED;
        }
    };

    public static final IVersionedSerializer<Result> result = new IVersionedSerializer<>()
    {
        public void serialize(Result t, DataOutputPlus out, int version) { }
        public Result deserialize(DataInputPlus in, int version)
        {
            return APPLIED;
        }

        public long serializedSize(Result t, int version)
        {
            return 0;
        }
    };
}
