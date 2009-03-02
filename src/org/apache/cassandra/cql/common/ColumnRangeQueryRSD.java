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
import org.apache.cassandra.db.Row;
import org.apache.cassandra.service.StorageProxy;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.LogUtil;
import org.apache.log4j.Logger;
import org.apache.cassandra.db.*;

/**
 * A Row Source Defintion (RSD) for doing a range query on a column map
 * (in Standard or Super Column Family).
 */
public class ColumnRangeQueryRSD extends RowSourceDef
{
    private final static Logger logger_ = Logger.getLogger(ColumnRangeQueryRSD.class);
    private CFMetaData cfMetaData_;
    private OperandDef rowKey_;
    private OperandDef superColumnKey_;
    private int        offset_;
    private int        limit_;

    /**
     * Set up a range query on column map in a simple column family.
     * The column map in a simple column family is identified by the rowKey.
     * 
     * Note: "limit" of -1 is the equivalent of no limit.
     *       "offset" specifies the number of rows to skip. An offset of 0 implies from the first row.
     */
    public ColumnRangeQueryRSD(CFMetaData cfMetaData, OperandDef rowKey, int offset, int limit)
    {
        cfMetaData_     = cfMetaData;
        rowKey_         = rowKey;
        superColumnKey_ = null;
        offset_         = offset;
        limit_          = limit;
    }

    /**
     * Setup a range query on a column map in a super column family.
     * The column map in a super column family is identified by the rowKey & superColumnKey.
     *  
     * Note: "limit" of -1 is the equivalent of no limit.
     *       "offset" specifies the number of rows to skip. An offset of 0 implies the first row.  
     */
    public ColumnRangeQueryRSD(CFMetaData cfMetaData, ConstantOperand rowKey, ConstantOperand superColumnKey,
                               int offset, int limit)
    {
        cfMetaData_     = cfMetaData;
        rowKey_         = rowKey;
        superColumnKey_ = superColumnKey;
        offset_         = offset;
        limit_          = limit;
    }

    public List<Map<String,String>> getRows()
    {
        String columnFamily_column;
        String superColumnKey = null;      

        if (superColumnKey_ != null)
        {
            superColumnKey = (String)(superColumnKey_.get());
            columnFamily_column = cfMetaData_.cfName + ":" + superColumnKey;
        }
        else
        {
            columnFamily_column = cfMetaData_.cfName;
        }

        Row row = null;
        try
        {
            String key = (String)(rowKey_.get());
            row = StorageProxy.readProtocol(cfMetaData_.tableName, key, columnFamily_column,
                                            offset_, limit_, StorageService.ConsistencyLevel.WEAK);
        }
        catch (Exception e)
        {
            logger_.error(LogUtil.throwableToString(e));
            throw new RuntimeException(RuntimeErrorMsg.GENERIC_ERROR.getMsg());
        }

        List<Map<String, String>> rows = new LinkedList<Map<String, String>>();
        if (row != null)
        {
            Map<String, ColumnFamily> cfMap = row.getColumnFamilies();
            if (cfMap != null && cfMap.size() > 0)
            {
                ColumnFamily cfamily = cfMap.get(cfMetaData_.cfName);
                if (cfamily != null)
                {
                    Collection<IColumn> columns = null;
                    if (superColumnKey_ != null)
                    {
                        // this is the super column case 
                        IColumn column = cfamily.getColumn(superColumnKey);
                        if (column != null)
                            columns = column.getSubColumns();
                    }
                    else
                    {
                        columns = cfamily.getAllColumns();
                    }

                    if (columns != null && columns.size() > 0)
                    {
                        for (IColumn column : columns)
                        {
                            Map<String, String> result = new HashMap<String, String>();
                            
                            result.put(cfMetaData_.n_columnKey, column.name());
                            result.put(cfMetaData_.n_columnValue, new String(column.value()));
                            result.put(cfMetaData_.n_columnTimestamp, Long.toString(column.timestamp()));
                            
                            rows.add(result);
                        }
                    }
                }
            }
        }
        return rows;
    }

    public String explainPlan()
    {
        return String.format("%s Column Family: Column Range Query: \n" +
                "  Table Name:       %s\n" +
                "  Column Family:    %s\n" +
                "  RowKey:           %s\n" +
                "%s"                   +
                "  Offset:           %d\n" +
                "  Limit:            %d\n" +
                "  Order By:         %s",
                cfMetaData_.columnType,
                cfMetaData_.tableName,
                cfMetaData_.cfName,
                rowKey_.explain(),
                (superColumnKey_ == null) ? "" : "  SuperColumnKey:   " + superColumnKey_.explain() + "\n",
                offset_, limit_,
                cfMetaData_.indexProperty_);
    }
}