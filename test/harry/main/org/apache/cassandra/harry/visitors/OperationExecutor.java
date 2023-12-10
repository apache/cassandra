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

package org.apache.cassandra.harry.visitors;

import org.apache.cassandra.harry.core.Run;
import org.apache.cassandra.harry.operations.CompiledStatement;

public interface OperationExecutor
{
    interface RowVisitorFactory
    {
        OperationExecutor make(Run run);
    }

    default CompiledStatement perform(VisitExecutor.Operation operation)
    {
        switch (operation.kind())
        {
            // TODO: switch to EnumMap
            // TODO: pluggable capabilities; OperationKind can/should bear its own logic
            case INSERT:
                return insert((VisitExecutor.WriteOp) operation);
            case UPDATE:
                return update((VisitExecutor.WriteOp) operation);
            case DELETE_ROW:
                return deleteRow((VisitExecutor.DeleteRowOp) operation);
            case INSERT_WITH_STATICS:
                return insertWithStatics((VisitExecutor.WriteStaticOp) operation);
            case UPDATE_WITH_STATICS:
                return updateWithStatics((VisitExecutor.WriteStaticOp) operation);
            case DELETE_PARTITION:
                return deletePartition((VisitExecutor.DeleteOp) operation);
            case DELETE_COLUMN:
                return deleteColumn((VisitExecutor.DeleteColumnsOp) operation);
            case DELETE_COLUMN_WITH_STATICS:
                return deleteColumnWithStatics((VisitExecutor.DeleteColumnsOp) operation);
            case DELETE_RANGE:
                return deleteRange((VisitExecutor.DeleteOp) operation);
            case DELETE_SLICE:
                return deleteSlice((VisitExecutor.DeleteOp) operation);
            default:
                throw new IllegalStateException();
        }
    }

    CompiledStatement insert(VisitExecutor.WriteOp operation);
    CompiledStatement update(VisitExecutor.WriteOp operation);

    CompiledStatement insertWithStatics(VisitExecutor.WriteStaticOp operation);
    CompiledStatement updateWithStatics(VisitExecutor.WriteStaticOp operation);

    CompiledStatement deleteColumn(VisitExecutor.DeleteColumnsOp operation);
    CompiledStatement deleteColumnWithStatics(VisitExecutor.DeleteColumnsOp operation);

    CompiledStatement deleteRow(VisitExecutor.DeleteRowOp operation);

    CompiledStatement deletePartition(VisitExecutor.DeleteOp deleteOp);
    CompiledStatement deleteRange(VisitExecutor.DeleteOp deleteOp);
    CompiledStatement deleteSlice(VisitExecutor.DeleteOp deleteOp);


}