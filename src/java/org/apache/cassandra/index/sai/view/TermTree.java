/*
 * All changes to the original code are Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */

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

package org.apache.cassandra.index.sai.view;

import java.nio.ByteBuffer;
import java.util.Set;

import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.index.sai.SSTableIndex;
import org.apache.cassandra.index.sai.plan.Expression;
import org.apache.cassandra.index.sai.utils.TypeUtil;

public interface TermTree
{
    Set<SSTableIndex> search(Expression e);

    abstract class Builder
    {
        protected final AbstractType<?> comparator;
        protected ByteBuffer min, max;

        protected Builder(AbstractType<?> comparator)
        {
            this.comparator = comparator;
        }

        public final void add(SSTableIndex index)
        {
            addIndex(index);

            min = min == null || TypeUtil.compare(min, index.minTerm(), comparator) > 0 ? index.minTerm() : min;
            max = max == null || TypeUtil.compare(max, index.maxTerm(), comparator) < 0 ? index.maxTerm() : max;
        }

        protected abstract void addIndex(SSTableIndex index);

        public abstract TermTree build();
    }
}
