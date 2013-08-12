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
package org.apache.cassandra.streaming.management;

import java.util.*;
import javax.management.openmbean.*;

import com.google.common.base.Function;
import com.google.common.base.Throwables;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import org.apache.cassandra.streaming.SessionInfo;
import org.apache.cassandra.streaming.StreamState;

/**
 */
public class StreamStateCompositeData
{
    private static final String[] ITEM_NAMES = new String[]{"planId", "description", "sessions"};
    private static final String[] ITEM_DESCS = new String[]{"Plan ID of this stream",
                                                            "Stream plan description",
                                                            "Active stream sessions"};
    private static final OpenType<?>[] ITEM_TYPES;

    public static final CompositeType COMPOSITE_TYPE;
    static  {
        try
        {
            ITEM_TYPES = new OpenType[]{SimpleType.STRING,
                                         SimpleType.STRING,
                                         ArrayType.getArrayType(SessionInfoCompositeData.COMPOSITE_TYPE)};
            COMPOSITE_TYPE = new CompositeType(StreamState.class.getName(),
                                            "StreamState",
                                            ITEM_NAMES,
                                            ITEM_DESCS,
                                            ITEM_TYPES);
        }
        catch (OpenDataException e)
        {
            throw Throwables.propagate(e);
        }
    }

    public static CompositeData toCompositeData(final StreamState streamState)
    {
        Map<String, Object> valueMap = new HashMap<>();
        valueMap.put(ITEM_NAMES[0], streamState.planId.toString());
        valueMap.put(ITEM_NAMES[1], streamState.description);

        CompositeData[] sessions = new CompositeData[streamState.sessions.size()];
        Lists.newArrayList(Iterables.transform(streamState.sessions, new Function<SessionInfo, CompositeData>()
        {
            public CompositeData apply(SessionInfo input)
            {
                return SessionInfoCompositeData.toCompositeData(streamState.planId, input);
            }
        })).toArray(sessions);
        valueMap.put(ITEM_NAMES[2], sessions);
        try
        {
            return new CompositeDataSupport(COMPOSITE_TYPE, valueMap);
        }
        catch (OpenDataException e)
        {
            throw Throwables.propagate(e);
        }
    }

    public static StreamState fromCompositeData(CompositeData cd)
    {
        assert cd.getCompositeType().equals(COMPOSITE_TYPE);
        Object[] values = cd.getAll(ITEM_NAMES);
        UUID planId = UUID.fromString((String) values[0]);
        String description = (String) values[1];
        Set<SessionInfo> sessions = Sets.newHashSet(Iterables.transform(Arrays.asList((CompositeData[]) values[2]),
                                                                        new Function<CompositeData, SessionInfo>()
                                                                        {
                                                                            public SessionInfo apply(CompositeData input)
                                                                            {
                                                                                return SessionInfoCompositeData.fromCompositeData(input);
                                                                            }
                                                                        }));
        return new StreamState(planId, description, sessions);
    }
}
