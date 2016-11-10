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
package org.apache.cassandra.db;

import java.util.Map;
import javax.management.openmbean.*;

import com.google.common.base.Throwables;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.utils.Pair;




public class SnapshotDetailsTabularData
{

    private static final String[] ITEM_NAMES = new String[]{"Snapshot name",
            "Keyspace name",
            "Column family name",
            "True size",
            "Size on disk"};

    private static final String[] ITEM_DESCS = new String[]{"snapshot_name",
            "keyspace_name",
            "columnfamily_name",
            "TrueDiskSpaceUsed",
            "TotalDiskSpaceUsed"};

    private static final String TYPE_NAME = "SnapshotDetails";

    private static final String ROW_DESC = "SnapshotDetails";

    private static final OpenType<?>[] ITEM_TYPES;

    private static final CompositeType COMPOSITE_TYPE;

    public static final TabularType TABULAR_TYPE;

    static
    {
        try
        {
            ITEM_TYPES = new OpenType[]{ SimpleType.STRING, SimpleType.STRING, SimpleType.STRING, SimpleType.STRING, SimpleType.STRING };

            COMPOSITE_TYPE = new CompositeType(TYPE_NAME, ROW_DESC, ITEM_NAMES, ITEM_DESCS, ITEM_TYPES);

            TABULAR_TYPE = new TabularType(TYPE_NAME, ROW_DESC, COMPOSITE_TYPE, ITEM_NAMES);
        }
        catch (OpenDataException e)
        {
            throw Throwables.propagate(e);
        }
    }


    public static void from(final String snapshot, final String ks, final String cf, Map.Entry<String, Pair<Long,Long>> snapshotDetail, TabularDataSupport result)
    {
        try
        {
            final String totalSize = FileUtils.stringifyFileSize(snapshotDetail.getValue().left);
            final String liveSize =  FileUtils.stringifyFileSize(snapshotDetail.getValue().right);
            result.put(new CompositeDataSupport(COMPOSITE_TYPE, ITEM_NAMES,
                    new Object[]{ snapshot, ks, cf, liveSize, totalSize }));
        }
        catch (OpenDataException e)
        {
            throw new RuntimeException(e);
        }
    }
}
