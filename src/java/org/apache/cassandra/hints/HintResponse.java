/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.cassandra.hints;

import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.net.MessageOut;
import org.apache.cassandra.net.MessagingService;

/**
 * An empty successful response to a HintMessage.
 */
public final class HintResponse
{
    public static final IVersionedSerializer<HintResponse> serializer = new Serializer();

    static final HintResponse instance = new HintResponse();
    static final MessageOut<HintResponse> message =
        new MessageOut<>(MessagingService.Verb.REQUEST_RESPONSE, instance, serializer);

    private HintResponse()
    {
    }

    private static final class Serializer implements IVersionedSerializer<HintResponse>
    {
        public long serializedSize(HintResponse response, int version)
        {
            return 0;
        }

        public void serialize(HintResponse response, DataOutputPlus out, int version)
        {
        }

        public HintResponse deserialize(DataInputPlus in, int version)
        {
            return instance;
        }
    }
}
