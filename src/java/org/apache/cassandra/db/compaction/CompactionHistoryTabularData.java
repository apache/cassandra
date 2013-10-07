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
package org.apache.cassandra.db.compaction;

import javax.management.openmbean.*;
import java.util.Map;
import java.util.UUID;

import com.google.common.base.Throwables;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.utils.FBUtilities;

public class CompactionHistoryTabularData
{
    private static final String[] ITEM_NAMES = new String[]{ "id", "keyspace_name", "columnfamily_name", "compacted_at",
                                                             "bytes_in", "bytes_out", "rows_merged" };

    private static final String[] ITEM_DESCS = new String[]{ "time uuid", "keyspace name",
                                                             "column family name", "compaction finished at",
                                                             "total bytes in", "total bytes out", "total rows merged" };

    private static final String TYPE_NAME = "CompactionHistory";

    private static final String ROW_DESC = "CompactionHistory";

    private static final OpenType<?>[] ITEM_TYPES;

    private static final CompositeType COMPOSITE_TYPE;

    private static final TabularType TABULAR_TYPE;

    static {
        try
        {
            ITEM_TYPES = new OpenType[]{ SimpleType.STRING, SimpleType.STRING, SimpleType.STRING, SimpleType.LONG,
                                         SimpleType.LONG, SimpleType.LONG, SimpleType.STRING };

            COMPOSITE_TYPE = new CompositeType(TYPE_NAME, ROW_DESC, ITEM_NAMES, ITEM_DESCS, ITEM_TYPES);

            TABULAR_TYPE = new TabularType(TYPE_NAME, ROW_DESC, COMPOSITE_TYPE, ITEM_NAMES);
        }
        catch (OpenDataException e)
        {
            throw Throwables.propagate(e);
        }
    }

    public static TabularData from(UntypedResultSet resultSet) throws OpenDataException
    {
        TabularDataSupport result = new TabularDataSupport(TABULAR_TYPE);
        for (UntypedResultSet.Row row : resultSet)
        {
            UUID id = row.getUUID(ITEM_NAMES[0]);
            String ksName = row.getString(ITEM_NAMES[1]);
            String cfName = row.getString(ITEM_NAMES[2]);
            long compactedAt = row.getLong(ITEM_NAMES[3]);
            long bytesIn = row.getLong(ITEM_NAMES[4]);
            long bytesOut = row.getLong(ITEM_NAMES[5]);
            Map<Integer, Long> rowMerged = row.getMap(ITEM_NAMES[6], Int32Type.instance, LongType.instance);

            result.put(new CompositeDataSupport(COMPOSITE_TYPE, ITEM_NAMES,
                       new Object[]{ id.toString(), ksName, cfName, compactedAt, bytesIn, bytesOut,
                                     "{" + FBUtilities.toString(rowMerged) + "}" }));
        }
        return result;
    }
}
