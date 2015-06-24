/*
 *
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
 *
 */
package org.apache.cassandra.stress.generate.values;


import java.util.UUID;

import org.apache.cassandra.db.marshal.TimeUUIDType;
import org.apache.cassandra.utils.UUIDGen;

public class TimeUUIDs extends Generator<UUID>
{
    final Dates dateGen;
    final long clockSeqAndNode;

    public TimeUUIDs(String name, GeneratorConfig config)
    {
        super(TimeUUIDType.instance, config, name, UUID.class);
        dateGen = new Dates(name, config);
        clockSeqAndNode = config.salt;
    }

    public void setSeed(long seed)
    {
        dateGen.setSeed(seed);
    }

    @Override
    public UUID generate()
    {
        return UUIDGen.getTimeUUID(dateGen.generate().getTime(), 0L, clockSeqAndNode);
    }
}
