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

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.cql.execution.RuntimeErrorMsg;
import org.apache.cassandra.db.RowMutation;
import org.apache.cassandra.service.StorageProxy;
import org.apache.cassandra.utils.LogUtil;
import org.apache.log4j.Logger;


/**
 * Execution plan for setting a specific column in a Simple/Super column family.
 *   SET table.standard_cf[<rowKey>][<columnKey>] = <value>;
 *   SET table.super_cf[<rowKey>][<superColumnKey>][<columnKey>] = <value>; 
 */
public class SetUniqueKey extends DMLPlan
{
    private final static Logger logger_ = Logger.getLogger(SetUniqueKey.class);    
    private CFMetaData cfMetaData_;
    private OperandDef rowKey_;
    private OperandDef superColumnKey_;
    private OperandDef columnKey_;
    private OperandDef value_;

    /**
     *  Construct an execution plan for setting a column in a simple column family
     * 
     *   SET table.standard_cf[<rowKey>][<columnKey>] = <value>;
     */
    public SetUniqueKey(CFMetaData cfMetaData, OperandDef rowKey, OperandDef columnKey, OperandDef value)
    {
        cfMetaData_     = cfMetaData;
        rowKey_         = rowKey;
        columnKey_      = columnKey;
        superColumnKey_ = null;
        value_          = value;
    }
    
    /**
     * Construct execution plan for setting a column in a super column family.
     * 
     *  SET table.super_cf[<rowKey>][<superColumnKey>][<columnKey>] = <value>;
     */
    public SetUniqueKey(CFMetaData cfMetaData, OperandDef rowKey, OperandDef superColumnKey, OperandDef columnKey, OperandDef value)
    {
        cfMetaData_     = cfMetaData;
        rowKey_         = rowKey;
        superColumnKey_ = superColumnKey;
        columnKey_      = columnKey;
        value_          = value;
    }

    public CqlResult execute()
    {
        String columnKey = (String)(columnKey_.get());
        String columnFamily_column;

        if (superColumnKey_ != null)
        {
            String superColumnKey = (String)(superColumnKey_.get());
            columnFamily_column = cfMetaData_.cfName + ":" + superColumnKey + ":" + columnKey;
        }
        else
        {
            columnFamily_column = cfMetaData_.cfName + ":" + columnKey;
        }

        try
        {
            RowMutation rm = new RowMutation(cfMetaData_.tableName, (String)(rowKey_.get()));
            rm.add(columnFamily_column, ((String)value_.get()).getBytes(), System.currentTimeMillis());
            StorageProxy.insert(rm);
        }
        catch (Exception e)
        {
            logger_.error(LogUtil.throwableToString(e));
            throw new RuntimeException(RuntimeErrorMsg.GENERIC_ERROR.getMsg());            
        }
        return null;
    }

    public String explainPlan()
    {
        return
            String.format("%s Column Family: Unique Key SET: \n" +
                "   Table Name:     %s\n" +
                "   Column Famly:   %s\n" +
                "   RowKey:         %s\n" +
                "%s" +
                "   ColumnKey:      %s\n" +
                "   Value:          %s\n",
                cfMetaData_.columnType,
                cfMetaData_.tableName,
                cfMetaData_.cfName,
                rowKey_.explain(),
                (superColumnKey_ == null) ? "" : "   SuperColumnKey: " + superColumnKey_.explain() + "\n",                
                columnKey_.explain(),
                value_.explain());
    }
}