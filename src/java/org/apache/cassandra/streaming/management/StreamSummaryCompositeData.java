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

import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.streaming.StreamSummary;

/**
 */
public class StreamSummaryCompositeData
{
    private static final String[] ITEM_NAMES = new String[]{"tableId",
                                                            "files",
                                                            "totalSize"};
    private static final String[] ITEM_DESCS = new String[]{"ColumnFamilu ID",
                                                            "Number of files",
                                                            "Total bytes of the files"};
    private static final OpenType<?>[] ITEM_TYPES = new OpenType[]{SimpleType.STRING,
                                                                   SimpleType.INTEGER,
                                                                   SimpleType.LONG};

    public static final CompositeType COMPOSITE_TYPE;
    static
    {
        try
        {
            COMPOSITE_TYPE = new CompositeType(StreamSummary.class.getName(),
                                               "StreamSummary",
                                               ITEM_NAMES,
                                               ITEM_DESCS,
                                               ITEM_TYPES);
        }
        catch (OpenDataException e)
        {
            throw new RuntimeException(e);
        }
    }

    public static CompositeData toCompositeData(StreamSummary streamSummary)
    {
        Map<String, Object> valueMap = new HashMap<>();
        valueMap.put(ITEM_NAMES[0], streamSummary.tableId.toString());
        valueMap.put(ITEM_NAMES[1], streamSummary.files);
        valueMap.put(ITEM_NAMES[2], streamSummary.totalSize);
        try
        {
            return new CompositeDataSupport(COMPOSITE_TYPE, valueMap);
        }
        catch (OpenDataException e)
        {
            throw new RuntimeException(e);
        }
    }

    public static StreamSummary fromCompositeData(CompositeData cd)
    {
        Object[] values = cd.getAll(ITEM_NAMES);
        return new StreamSummary(TableId.fromString((String) values[0]),
                                 (int) values[1],
                                 (long) values[2]);
    }
}
