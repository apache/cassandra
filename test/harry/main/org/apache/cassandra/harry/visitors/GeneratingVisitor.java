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
import org.apache.cassandra.harry.ddl.SchemaSpec;
import org.apache.cassandra.harry.model.OpSelectors;
import org.apache.cassandra.harry.operations.Query;
import org.apache.cassandra.harry.operations.QueryGenerator;
import org.apache.cassandra.harry.util.BitSet;

public class GeneratingVisitor extends LtsVisitor
{
    private final OpSelectors.PdSelector pdSelector;
    private final OpSelectors.DescriptorSelector descriptorSelector;
    private final QueryGenerator rangeSelector;
    private final SchemaSpec schema;

    public GeneratingVisitor(Run run,
                             VisitExecutor delegate)
    {
        super(delegate, run.clock::nextLts);

        this.pdSelector = run.pdSelector;
        this.descriptorSelector = run.descriptorSelector;
        this.schema = run.schemaSpec;
        this.rangeSelector = run.rangeSelector;
    }

    @Override
    public void visit(long lts)
    {
        generate(lts, pdSelector.pd(lts, schema));
    }

    private void generate(long lts, long pd)
    {
        beforeLts(lts, pd);
        int opsPerLts = descriptorSelector.operationsPerLts(lts);
        for (long opId = 0; opId < opsPerLts; opId++)
        {
            OpSelectors.OperationKind kind = descriptorSelector.operationType(pd, lts, opId);
            BaseOperation operation;
            switch (kind)
            {
                case INSERT:
                case UPDATE:
                    operation = writeOp(lts, pd, opId, kind);
                    break;
                case INSERT_WITH_STATICS:
                case UPDATE_WITH_STATICS:
                    operation = writeRegularAndStatic(lts, pd, opId, kind);
                    break;
                case DELETE_ROW:
                {
                    long cd = descriptorSelector.cd(pd, lts, opId, schema);
                    operation = new GeneratedDeleteRowOp(lts, pd, cd, opId, kind);
                    break;
                }
                case DELETE_COLUMN:
                case DELETE_COLUMN_WITH_STATICS:
                {
                    long cd = descriptorSelector.cd(pd, lts, opId, schema);
                    BitSet columns = descriptorSelector.columnMask(pd, lts, opId, kind);
                    operation = new GeneratedDeleteColumnsOp(lts, pd, cd, opId, kind, columns);
                    break;
                }
                case DELETE_PARTITION:
                case DELETE_RANGE:
                case DELETE_SLICE:
                    operation = new GeneratedDeleteOp(lts, pd, opId, kind, rangeSelector);
                    break;
                default:
                    throw new IllegalStateException("All cases are covered but not " + kind);
            }
            operation(operation);
        }
        afterLts(lts, pd);
    }

    public BaseOperation writeOp(long lts, long pd, long opId, OpSelectors.OperationKind kind)
    {
        long cd = descriptorSelector.cd(pd, lts, opId, schema);

        return new GeneratedWriteOp(lts, pd, cd, opId, kind)
        {
            @Override
            public long[] vds()
            {
                return descriptorSelector.vds(pd, cd(), lts, opId, kind(), schema);
            }
        };
    }

    public BaseOperation writeRegularAndStatic(long lts, long pd, long opId, OpSelectors.OperationKind kind)
    {
        long cd = descriptorSelector.cd(pd, lts, opId, schema);

        return new GeneratedWriteWithStaticOp(lts, pd, cd, opId, kind)
        {
            public long[] sds()
            {
                return descriptorSelector.sds(pd, cd(), lts, opId, kind(), schema);
            }

            @Override
            public long[] vds()
            {
                return descriptorSelector.vds(pd, cd(), lts, opId, kind(), schema);
            }
        };
    }

    public abstract static class GeneratedWriteOp extends BaseOperation implements ReplayingVisitor.WriteOp
    {
        protected final long cd;
        public GeneratedWriteOp(long lts, long pd, long cd, long opId, OpSelectors.OperationKind kind)
        {
            super(lts, pd, opId, kind);
            this.cd = cd;
        }

        @Override
        public long cd()
        {
            return cd;
        }
    }

    public abstract static class GeneratedWriteWithStaticOp extends GeneratedWriteOp implements ReplayingVisitor.WriteStaticOp
    {
        public GeneratedWriteWithStaticOp(long lts, long pd, long cd, long opId, OpSelectors.OperationKind kind)
        {
            super(lts, pd, cd, opId, kind);
        }
    }

    public static class GeneratedDeleteRowOp extends BaseOperation implements ReplayingVisitor.DeleteRowOp
    {
        private final long cd;
        public GeneratedDeleteRowOp(long lts, long pd, long cd, long opId, OpSelectors.OperationKind kind)
        {
            super(lts, pd, opId, kind);
            this.cd = cd;
        }

        @Override
        public long cd()
        {
            return cd;
        }
    }

    public static class GeneratedDeleteOp extends BaseOperation implements ReplayingVisitor.DeleteOp
    {
        private final Query relations;

        public GeneratedDeleteOp(long lts, long pd, long opId, OpSelectors.OperationKind kind, QueryGenerator queryGenerator)
        {
            this(lts, pd, opId, kind, queryGenerator.inflate( lts, opId, queryKind(kind)));
        }

        public GeneratedDeleteOp(long lts, long pd, long opId, OpSelectors.OperationKind kind, Query relations)
        {
            super(lts, pd, opId, kind);
            this.relations = relations;
        }

        @Override
        public Query relations()
        {
            return relations;
        }

        protected static Query.QueryKind queryKind(OpSelectors.OperationKind kind)
        {
            switch (kind)
            {
                case DELETE_PARTITION:
                    return Query.QueryKind.SINGLE_PARTITION;
                case DELETE_ROW:
                    return Query.QueryKind.SINGLE_CLUSTERING;
                case DELETE_RANGE:
                    return Query.QueryKind.CLUSTERING_RANGE;
                case DELETE_SLICE:
                    return Query.QueryKind.CLUSTERING_SLICE;
                default:
                    throw new IllegalStateException(String.format("Can not transform %s into delete", kind));
            }
        }
    }

    public static class GeneratedDeleteColumnsOp extends BaseOperation implements ReplayingVisitor.DeleteColumnsOp
    {
        private final long cd;
        private final BitSet columnMask;

        public GeneratedDeleteColumnsOp(long lts, long pd, long cd, long opId, OpSelectors.OperationKind kind, BitSet columnMask)
        {
            super(lts, pd, opId, kind);
            this.cd = cd;
            this.columnMask = columnMask;
        }

        public long cd()
        {
            return cd;
        }

        @Override
        public BitSet columns()
        {
            return columnMask;
        }
    }
}
