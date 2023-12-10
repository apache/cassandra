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

import org.apache.cassandra.harry.core.MetricReporter;
import org.apache.cassandra.harry.core.Run;
import com.google.common.annotations.VisibleForTesting;
import org.apache.cassandra.harry.ddl.SchemaSpec;
import org.apache.cassandra.harry.model.OpSelectors;
import org.apache.cassandra.harry.operations.CompiledStatement;
import org.apache.cassandra.harry.operations.DeleteHelper;
import org.apache.cassandra.harry.operations.WriteHelper;
import org.apache.cassandra.harry.util.BitSet;

public class MutatingRowVisitor implements OperationExecutor
{
    protected final SchemaSpec schema;
    protected final OpSelectors.Clock clock;
    protected final MetricReporter metricReporter;

    public MutatingRowVisitor(Run run)
    {
        this(run.schemaSpec,
             run.clock,
             run.metricReporter);
    }

    @VisibleForTesting
    public MutatingRowVisitor(SchemaSpec schema,
                              OpSelectors.Clock clock,
                              MetricReporter metricReporter)
    {
        this.metricReporter = metricReporter;
        this.schema = schema;
        this.clock = clock;
    }

    public CompiledStatement insert(VisitExecutor.WriteOp op)
    {
        metricReporter.insert();
        return WriteHelper.inflateInsert(schema, op.pd(), op.cd(), op.vds(), null, clock.rts(op.lts()));
    }

    public CompiledStatement insertWithStatics(VisitExecutor.WriteStaticOp op)
    {
        metricReporter.insert();
        return WriteHelper.inflateInsert(schema, op.pd(), op.cd(), op.vds(), op.sds(), clock.rts(op.lts()));
    }

    public CompiledStatement update(VisitExecutor.WriteOp op)
    {
        metricReporter.insert();
        return WriteHelper.inflateUpdate(schema, op.pd(), op.cd(), op.vds(), null, clock.rts(op.lts()));
    }

    public CompiledStatement updateWithStatics(VisitExecutor.WriteStaticOp op)
    {
        metricReporter.insert();
        return WriteHelper.inflateUpdate(schema, op.pd(), op.cd(), op.vds(), op.sds(), clock.rts(op.lts()));
    }

    public CompiledStatement deleteColumn(VisitExecutor.DeleteColumnsOp op)
    {
        metricReporter.columnDelete();
        BitSet mask = schema.regularColumnsMask();
        return DeleteHelper.deleteColumn(schema, op.pd(), op.cd(), op.columns(), mask, clock.rts(op.lts()));
    }

    public CompiledStatement deleteColumnWithStatics(VisitExecutor.DeleteColumnsOp op)
    {
        metricReporter.columnDelete();
        BitSet mask = schema.regularAndStaticColumnsMask();
        return DeleteHelper.deleteColumn(schema, op.pd(), op.cd(), op.columns(), mask, clock.rts(op.lts()));
    }

    public CompiledStatement deleteRow(VisitExecutor.DeleteRowOp op)
    {
        metricReporter.rowDelete();
        return DeleteHelper.deleteRow(schema, op.pd(), op.cd(), clock.rts(op.lts()));
    }

    public CompiledStatement deletePartition(VisitExecutor.DeleteOp op)
    {
        metricReporter.partitionDelete();
        return DeleteHelper.delete(schema, op.pd(), clock.rts(op.lts()));
    }

    public CompiledStatement deleteRange(VisitExecutor.DeleteOp op)
    {
        metricReporter.rangeDelete();
        return op.relations().toDeleteStatement(clock.rts(op.lts()));
    }

    public CompiledStatement deleteSlice(VisitExecutor.DeleteOp op)
    {
        metricReporter.rangeDelete();
        return op.relations().toDeleteStatement(clock.rts(op.lts()));
    }
}