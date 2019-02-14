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

package org.apache.cassandra.distributed.api;

import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.MessagingService;

import java.util.function.BiConsumer;

public interface IMessageFilters
{
    public interface Filter
    {
        Filter restore();
        Filter drop();
    }

    public interface Builder
    {
        Builder from(int ... nums);
        Builder to(int ... nums);
        Filter ready();
        Filter drop();
    }

    Builder verbs(MessagingService.Verb... verbs);
    Builder allVerbs();
    void reset();

    // internal
    BiConsumer<InetAddressAndPort, IMessage> filter(BiConsumer<InetAddressAndPort, IMessage> applyIfNotFiltered);
}
