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
import org.apache.cassandra.cql.execution.*;
import org.apache.cassandra.db.*;
import org.apache.cassandra.service.*;

/**
 * Execution plan for batch setting a set of columns in a Simple/Super column family.
 *   SET table.standard_cf[<rowKey>] = <columnMapExpr>;                
 *   SET table.super_cf[<rowKey>][<superColumn>] = <columnMapExpr>;
 */
public class SetColumnMap extends DMLPlan
{
    private final static Logger logger_ = Logger.getLogger(SetUniqueKey.class);    
    private CFMetaData    cfMetaData_;
    private OperandDef    rowKey_;
    private OperandDef    superColumnKey_;
    private ColumnMapExpr columnMapExpr_;

    /**
     *  construct an execution plan node to set the column map for a Standard Column Family.
     *
     *    SET table.standard_cf[<rowKey>] = <columnMapExpr>;                
     */
    public SetColumnMap(CFMetaData cfMetaData, OperandDef rowKey, ColumnMapExpr columnMapExpr)
    {
        cfMetaData_     = cfMetaData;
        rowKey_         = rowKey;
        superColumnKey_ = null;        
        columnMapExpr_  = columnMapExpr;
    }

    /**
     * Construct an execution plan node to set the column map for a Super Column Family
     * 
     *   SET table.super_cf[<rowKey>][<superColumn>] = <columnMapExpr>;
     */
    public SetColumnMap(CFMetaData cfMetaData, OperandDef rowKey, OperandDef superColumnKey, ColumnMapExpr columnMapExpr)
    {
        cfMetaData_     = cfMetaData;
        rowKey_         = rowKey;
        superColumnKey_ = superColumnKey;
        columnMapExpr_  = columnMapExpr;
    }

    public CqlResult execute()
    {
        String columnFamily_column;
        
        if (superColumnKey_ != null)
        {
            String superColumnKey = (String)(superColumnKey_.get());
            columnFamily_column = cfMetaData_.cfName + ":" + superColumnKey + ":";
        }
        else
        {
            columnFamily_column = cfMetaData_.cfName + ":";
        }

        try
        {
            RowMutation rm = new RowMutation(cfMetaData_.tableName, (String)(rowKey_.get()));
            long time = System.currentTimeMillis();

            for (Pair<OperandDef, OperandDef> entry : columnMapExpr_)
            {
                OperandDef columnKey = entry.getFirst();
                OperandDef value     = entry.getSecond();

                rm.add(columnFamily_column + (String)(columnKey.get()), ((String)value.get()).getBytes(), time);
            }
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
        StringBuffer sb = new StringBuffer();
        
        String prefix =
            String.format("%s Column Family: Batch SET a set of columns: \n" +
            "   Table Name:     %s\n" +
            "   Column Famly:   %s\n" +
            "   RowKey:         %s\n" +
            "%s",
            cfMetaData_.columnType,
            cfMetaData_.tableName,
            cfMetaData_.cfName,
            rowKey_.explain(),
            (superColumnKey_ == null) ? "" : "   SuperColumnKey: " + superColumnKey_.explain() + "\n");                

        for (Pair<OperandDef, OperandDef> entry : columnMapExpr_)
        {
            OperandDef columnKey = entry.getFirst();
            OperandDef value     = entry.getSecond();
            sb.append(String.format("   ColumnKey:        %s\n" +
                                    "   Value:            %s\n",
                                    columnKey.explain(), value.explain()));
        }
        
        return prefix + sb.toString();
    }
}