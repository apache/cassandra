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

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import javax.management.openmbean.*;

import com.google.common.base.Throwables;

import org.apache.cassandra.streaming.ProgressInfo;

public class ProgressInfoCompositeData
{
    private static final String[] ITEM_NAMES = new String[]{"planId",
                                                            "peer",
                                                            "sessionIndex",
                                                            "fileName",
                                                            "direction",
                                                            "currentBytes",
                                                            "totalBytes"};
    private static final String[] ITEM_DESCS = new String[]{"String representation of Plan ID",
                                                            "Session peer",
                                                            "Index of session",
                                                            "Name of the file",
                                                            "Direction('IN' or 'OUT')",
                                                            "Current bytes transferred",
                                                            "Total bytes to transfer"};
    private static final OpenType<?>[] ITEM_TYPES = new OpenType[]{SimpleType.STRING,
                                                                   SimpleType.STRING,
                                                                   SimpleType.INTEGER,
                                                                   SimpleType.STRING,
                                                                   SimpleType.STRING,
                                                                   SimpleType.LONG,
                                                                   SimpleType.LONG};

    public static final CompositeType COMPOSITE_TYPE;
    static
    {
        try
        {
            COMPOSITE_TYPE = new CompositeType(ProgressInfo.class.getName(),
                                               "ProgressInfo",
                                               ITEM_NAMES,
                                               ITEM_DESCS,
                                               ITEM_TYPES);
        }
        catch (OpenDataException e)
        {
            throw Throwables.propagate(e);
        }
    }

    public static CompositeData toCompositeData(UUID planId, ProgressInfo progressInfo)
    {
        Map<String, Object> valueMap = new HashMap<>();
        valueMap.put(ITEM_NAMES[0], planId.toString());
        valueMap.put(ITEM_NAMES[1], progressInfo.peer.getHostAddress());
        valueMap.put(ITEM_NAMES[2], progressInfo.sessionIndex);
        valueMap.put(ITEM_NAMES[3], progressInfo.fileName);
        valueMap.put(ITEM_NAMES[4], progressInfo.direction.name());
        valueMap.put(ITEM_NAMES[5], progressInfo.currentBytes);
        valueMap.put(ITEM_NAMES[6], progressInfo.totalBytes);
        try
        {
            return new CompositeDataSupport(COMPOSITE_TYPE, valueMap);
        }
        catch (OpenDataException e)
        {
            throw Throwables.propagate(e);
        }
    }

    public static ProgressInfo fromCompositeData(CompositeData cd)
    {
        Object[] values = cd.getAll(ITEM_NAMES);
        try
        {
            return new ProgressInfo(InetAddress.getByName((String) values[1]),
                                    (int) values[2],
                                    (String) values[3],
                                    ProgressInfo.Direction.valueOf((String)values[4]),
                                    (long) values[5],
                                    (long) values[6]);
        }
        catch (UnknownHostException e)
        {
            throw Throwables.propagate(e);
        }
    }
}
