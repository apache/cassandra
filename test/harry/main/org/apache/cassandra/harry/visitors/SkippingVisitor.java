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

import java.util.Set;
import java.util.function.LongSupplier;

public class SkippingVisitor extends LtsVisitor
{
    private final Set<Long> ltsToSkip;
    private final Set<Long> pdsToSkip;
    private final LtsToPd ltsToPd;
    // Use DelegatingVisitor class instead of VisitExecutor available via protected field
    private LtsVisitor delegateShadow;

    public SkippingVisitor(LtsVisitor delegate,
                           LongSupplier ltsSupplier,
                           LtsToPd ltsToPd,
                           Set<Long> ltsToSkip,
                           Set<Long> pdsToSkip)
    {
        super(delegate, ltsSupplier);
        this.ltsToSkip = ltsToSkip;
        this.pdsToSkip = pdsToSkip;
        this.ltsToPd = ltsToPd;
    }

    public void visit(long lts)
    {
        if (ltsToSkip.contains(lts) || pdsToSkip.contains(ltsToPd.convert(lts)))
            return;

        delegateShadow.visit(lts);
    }

    public static interface LtsToPd
    {
        public long convert(long lts);
    }
}
