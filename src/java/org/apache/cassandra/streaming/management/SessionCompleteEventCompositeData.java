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

import java.util.HashMap;
import java.util.Map;
import javax.management.openmbean.*;

import com.google.common.base.Throwables;

import org.apache.cassandra.streaming.StreamEvent;

public class SessionCompleteEventCompositeData
{
    private static final String[] ITEM_NAMES = new String[]{"planId",
                                                            "peer",
                                                            "success"};
    private static final String[] ITEM_DESCS = new String[]{"Plan ID",
                                                            "Session peer",
                                                            "Indicates whether session was successful"};
    private static final OpenType<?>[] ITEM_TYPES = new OpenType[]{SimpleType.STRING,
                                                                   SimpleType.STRING,
                                                                   SimpleType.BOOLEAN};

    public static final CompositeType COMPOSITE_TYPE;
    static  {
        try
        {
            COMPOSITE_TYPE = new CompositeType(StreamEvent.SessionCompleteEvent.class.getName(),
                                               "SessionCompleteEvent",
                                               ITEM_NAMES,
                                               ITEM_DESCS,
                                               ITEM_TYPES);
        }
        catch (OpenDataException e)
        {
            throw Throwables.propagate(e);
        }
    }

    public static CompositeData toCompositeData(StreamEvent.SessionCompleteEvent event)
    {
        Map<String, Object> valueMap = new HashMap<>();
        valueMap.put(ITEM_NAMES[0], event.planId.toString());
        valueMap.put(ITEM_NAMES[1], event.peer.getHostAddress());
        valueMap.put(ITEM_NAMES[2], event.success);
        try
        {
            return new CompositeDataSupport(COMPOSITE_TYPE, valueMap);
        }
        catch (OpenDataException e)
        {
            throw Throwables.propagate(e);
        }
    }
}
