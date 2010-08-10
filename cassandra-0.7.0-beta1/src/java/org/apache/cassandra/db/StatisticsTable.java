/**
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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.filter.QueryPath;
import org.apache.cassandra.db.filter.QueryFilter;
import org.apache.cassandra.io.sstable.SSTable;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.EstimatedHistogram;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.service.StorageService;

import static com.google.common.base.Charsets.UTF_8;

public class StatisticsTable
{
    private static Logger logger = LoggerFactory.getLogger(StatisticsTable.class);
    public static final String STATISTICS_CF = "Statistics";
    public static final byte[] ROWSIZE_SC = "RowSize".getBytes(UTF_8);
    public static final byte[] COLUMNCOUNT_SC = "ColumnCount".getBytes(UTF_8);

    private static DecoratedKey decorate(byte[] key)
    {
        return StorageService.getPartitioner().decorateKey(key);
    }

    public static void persistSSTableStatistics(Descriptor desc, EstimatedHistogram rowsizes, EstimatedHistogram columncounts) throws IOException
    {
        String filename = getRelativePath(desc.filenameFor(SSTable.COMPONENT_DATA));
        if (isTemporary(filename) || desc.ksname.equals(Table.SYSTEM_TABLE))
            return;
        long[] rowbuckets = rowsizes.getBucketOffsets();
        long[] rowvalues = rowsizes.get(false);
        long[] columnbuckets = columncounts.getBucketOffsets();
        long[] columnvalues = columncounts.get(false);
        RowMutation rm = new RowMutation(Table.SYSTEM_TABLE, filename.getBytes(UTF_8));
        for (int i=0; i<rowbuckets.length; i++)
        {
            QueryPath path = new QueryPath(STATISTICS_CF, ROWSIZE_SC, FBUtilities.toByteArray(rowbuckets[i]));
            rm.add(path, FBUtilities.toByteArray(rowvalues[i]), new TimestampClock(System.currentTimeMillis()));
        }
        for (int i=0; i<columnbuckets.length; i++)
        {
            QueryPath path = new QueryPath(STATISTICS_CF, COLUMNCOUNT_SC, FBUtilities.toByteArray(columnbuckets[i]));
            rm.add(path, FBUtilities.toByteArray(columnvalues[i]), new TimestampClock(System.currentTimeMillis()));
        }
        rm.apply();
        if (logger.isDebugEnabled())
            logger.debug("Recorded SSTable statistics for " + filename);
    }

    public static void deleteSSTableStatistics(String filepath) throws IOException
    {
        String filename = getRelativePath(filepath);
        RowMutation rm = new RowMutation(Table.SYSTEM_TABLE, filename.getBytes(UTF_8));
        QueryPath path = new QueryPath(STATISTICS_CF);
        rm.delete(path, new TimestampClock(System.currentTimeMillis()));
        rm.apply();
        if (logger.isDebugEnabled())
            logger.debug("Deleted SSTable statistics for " + filename);
    }

    private static long[] getSSTableStatistics(String filepath, byte[] superCol) throws IOException
    {
        long[] rv;
        String filename = getRelativePath(filepath);
        QueryPath path = new QueryPath(STATISTICS_CF);
        QueryFilter filter = QueryFilter.getNamesFilter(decorate(filename.getBytes(UTF_8)), path, superCol);
        ColumnFamily cf = Table.open(Table.SYSTEM_TABLE).getColumnFamilyStore(STATISTICS_CF).getColumnFamily(filter);
        if (cf == null)
            return new long[0];
        IColumn scolumn = cf.getColumn(superCol);
        rv = new long[scolumn.getSubColumns().size()];
        int i = 0;
        for (IColumn col : scolumn.getSubColumns()) {
            rv[i] = ByteBuffer.wrap(col.value()).getLong();
            i++;
        }
        return rv;
    }

    public static long [] getSSTableRowSizeStatistics(String filename) throws IOException
    {
        return getSSTableStatistics(filename, ROWSIZE_SC);
    }

    public static long [] getSSTableColumnCountStatistics(String filename) throws IOException
    {
        return getSSTableStatistics(filename, COLUMNCOUNT_SC);
    }

    private static String getRelativePath(String filename)
    {
        for (String prefix : DatabaseDescriptor.getAllDataFileLocations())
        {
            if (filename.startsWith(prefix))
               return filename.substring(prefix.length());
        }
        return filename;
    }

    private static boolean isTemporary(String filename)
    {
        return filename.contains("-" + SSTable.TEMPFILE_MARKER);
    }
}
