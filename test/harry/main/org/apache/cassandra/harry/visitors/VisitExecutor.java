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

import org.apache.cassandra.harry.model.OpSelectors;
import org.apache.cassandra.harry.operations.Query;
import org.apache.cassandra.harry.util.BitSet;

public abstract class VisitExecutor
{
    protected abstract void beforeLts(long lts, long pd);

    protected abstract void afterLts(long lts, long pd);

    protected abstract void operation(Operation operation);

    public abstract void shutdown() throws InterruptedException;

    public interface WriteOp extends Operation
    {
        long cd();
        long[] vds();
    }

    public interface WriteStaticOp extends WriteOp
    {
        long[] sds();
    }

    public interface DeleteRowOp extends Operation
    {
        long cd();
    }

    public interface DeleteOp extends Operation
    {
        Query relations();
    }

    public interface DeleteColumnsOp extends Operation
    {
        long cd();
        BitSet columns();
    }

    public interface Operation
    {
        long pd();
        long lts();
        long opId();
        OpSelectors.OperationKind kind();
    }

    public static abstract class BaseOperation implements Operation
    {
        public final long lts;
        public final long opId;
        public final long pd;
        public final OpSelectors.OperationKind kind;

        public BaseOperation(long lts, long pd, long opId, OpSelectors.OperationKind kind)
        {
            assert opId >= 0;
            this.pd = pd;
            this.lts = lts;
            this.opId = opId;
            this.kind = kind;
        }

        @Override
        public final long pd()
        {
            return pd;

        }

        @Override
        public final long lts()
        {
            return lts;
        }

        @Override
        public final long opId()
        {
            return opId;
        }

        @Override
        public final OpSelectors.OperationKind kind()
        {
            return kind;

        }

        public String toString()
        {
            return "Operation{" +
                   "  lts=" + lts +
                   "  pd=" + pd +
                   "  opId=" + opId +
                   ", kind=" + kind +
                   '}';
        }
    }
}
