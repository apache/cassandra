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

package org.apache.cassandra.cql.common;

import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.List;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.cql.execution.RuntimeErrorMsg;
import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.IColumn;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.Row;
import org.apache.cassandra.service.StorageProxy;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.LogUtil;
import org.apache.log4j.Logger;

/**
 * A Row Source Defintion (RSD) for doing a super column range query on a Super Column Family.
 */
public class SuperColumnRangeQueryRSD extends RowSourceDef
{
    private final static Logger logger_ = Logger.getLogger(SuperColumnRangeQueryRSD.class);
    private CFMetaData cfMetaData_;
    private OperandDef rowKey_;
    private OperandDef superColumnKey_;
    private int        offset_;
    private int        limit_;

    /**
     * Set up a range query on super column map in a super column family.
     * The super column map is identified by the rowKey.
     * 
     * Note: "limit" of -1 is the equivalent of no limit.
     *       "offset" specifies the number of rows to skip.
     *        An offset of 0 implies from the first row.
     */
    public SuperColumnRangeQueryRSD(CFMetaData cfMetaData, OperandDef rowKey, int offset, int limit)
    {
        cfMetaData_     = cfMetaData;
        rowKey_         = rowKey;
        offset_         = offset;
        limit_          = limit;
    }

    public List<Map<String,String>> getRows()
    {
        Row row = null;
        try
        {
            String key = (String)(rowKey_.get());
            ReadCommand readCommand = new ReadCommand(cfMetaData_.tableName, key, cfMetaData_.cfName, offset_, limit_);
            row = StorageProxy.readProtocol(readCommand, StorageService.ConsistencyLevel.WEAK);
        }
        catch (Exception e)
        {
            logger_.error(LogUtil.throwableToString(e));
            throw new RuntimeException(RuntimeErrorMsg.GENERIC_ERROR.getMsg());
        }

        List<Map<String, String>> rows = new LinkedList<Map<String, String>>();
        if (row != null)
        {
            Map<String, ColumnFamily> cfMap = row.getColumnFamilyMap();
            if (cfMap != null && cfMap.size() > 0)
            {
                ColumnFamily cfamily = cfMap.get(cfMetaData_.cfName);
                if (cfamily != null)
                {
                    Collection<IColumn> columns = cfamily.getAllColumns();
                    if (columns != null && columns.size() > 0)
                    {
                        for (IColumn column : columns)
                        {
                            Collection<IColumn> subColumns = column.getSubColumns();
                            for( IColumn subColumn : subColumns )
                            {
                               Map<String, String> result = new HashMap<String, String>();
                               result.put(cfMetaData_.n_superColumnKey, column.name());                               
                               result.put(cfMetaData_.n_columnKey, subColumn.name());
                               result.put(cfMetaData_.n_columnValue, new String(subColumn.value()));
                               result.put(cfMetaData_.n_columnTimestamp, Long.toString(subColumn.timestamp()));
                               rows.add(result);
                            }
                        }
                    }
                }
            }
        }
        return rows;
    }

    public String explainPlan()
    {
        return String.format("%s Column Family: Super Column Range Query: \n" +
                "  Table Name:       %s\n" +
                "  Column Family:    %s\n" +
                "  RowKey:           %s\n" +
                "  Offset:           %d\n" +
                "  Limit:            %d\n" +
                "  Order By:         %s",
                cfMetaData_.columnType,
                cfMetaData_.tableName,
                cfMetaData_.cfName,
                rowKey_.explain(),
                offset_, limit_,
                cfMetaData_.indexProperty_);
    }
}