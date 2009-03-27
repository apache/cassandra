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
 * A Row Source Defintion (RSD) for looking up a unique column within a column family.
 */
public class UniqueKeyQueryRSD extends RowSourceDef
{
    private final static Logger logger_ = Logger.getLogger(UniqueKeyQueryRSD.class);    
    private CFMetaData cfMetaData_;
    private OperandDef rowKey_;
    private OperandDef superColumnKey_;
    private OperandDef columnKey_;

    // super column family
    public UniqueKeyQueryRSD(CFMetaData cfMetaData, OperandDef rowKey, OperandDef superColumnKey, OperandDef columnKey)
    {
        cfMetaData_     = cfMetaData;
        rowKey_         = rowKey;
        superColumnKey_ = superColumnKey;
        columnKey_      = columnKey;
    }

    // simple column family
    public UniqueKeyQueryRSD(CFMetaData cfMetaData, OperandDef rowKey, OperandDef columnKey)
    {
        cfMetaData_ = cfMetaData;
        rowKey_     = rowKey;
        columnKey_  = columnKey;
        superColumnKey_ = null;
    }

    // specific column lookup
    public List<Map<String,String>> getRows() throws RuntimeException
    {
        String columnKey = (String)(columnKey_.get());
        String columnFamily_column;
        String superColumnKey = null;

        if (superColumnKey_ != null)
        {
            superColumnKey = (String)(superColumnKey_.get());
            columnFamily_column = cfMetaData_.cfName + ":" + superColumnKey + ":" + columnKey;
        }
        else
        {
            columnFamily_column = cfMetaData_.cfName + ":" + columnKey;
        }

        Row row = null;
        try
        {
            String key = (String)(rowKey_.get());
            row = StorageProxy.readProtocol(cfMetaData_.tableName, key, columnFamily_column, -1,
                                            Integer.MAX_VALUE, StorageService.ConsistencyLevel.WEAK);
        }
        catch (Exception e)
        {
            logger_.error(LogUtil.throwableToString(e));
            throw new RuntimeException(RuntimeErrorMsg.GENERIC_ERROR.getMsg());
        }

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
                        if (columns.size() > 1)
                        {
                            // We are looking up by a rowKey & columnKey. There should
                            // be at most one column that matches. If we find more than
                            // one, then it is an internal error.
                            throw new RuntimeException(RuntimeErrorMsg.INTERNAL_ERROR.getMsg("Too many columns found for: " + columnKey));
                        }
                        for (IColumn column : columns)
                        {
                            List<Map<String, String>> rows = new LinkedList<Map<String, String>>();

                            Map<String, String> result = new HashMap<String, String>();
                            result.put(cfMetaData_.n_columnKey, column.name());
                            result.put(cfMetaData_.n_columnValue, new String(column.value()));
                            result.put(cfMetaData_.n_columnTimestamp, Long.toString(column.timestamp()));
                            
                            rows.add(result);
                                
                            // at this point, due to the prior checks, we are guaranteed that
                            // there is only one item in "columns".
                            return rows;
                        }
                        return null;
                    }
                }
            }
        }
        throw new RuntimeException(RuntimeErrorMsg.NO_DATA_FOUND.getMsg());
    }

    public String explainPlan()
    {
        return String.format("%s Column Family: Unique Key Query: \n" +
                "   Table Name:     %s\n" +
                "   Column Famly:   %s\n" +
                "   RowKey:         %s\n" +
                "%s" +
                "   ColumnKey:      %s",
                cfMetaData_.columnType,
                cfMetaData_.tableName,
                cfMetaData_.cfName,
                rowKey_.explain(),
                (superColumnKey_ == null) ? "" : "   SuperColumnKey: " + superColumnKey_.explain() + "\n",                
                columnKey_.explain());
    }
}