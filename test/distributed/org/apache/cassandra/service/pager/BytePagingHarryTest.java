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

package org.apache.cassandra.service.pager;

import org.apache.cassandra.harry.execution.InJvmDTestVisitExecutor;
import org.apache.cassandra.harry.gen.EntropySource;
import org.apache.cassandra.harry.op.Visit;

/**
 * CASSANDRA-11745: validate byte-paged reads against Harry's model on divergent and synchronized replicas.
 */
public class BytePagingHarryTest extends PagingHarryTest
{
    private static final int[] PAGE_SIZES_IN_BYTES = { 1, 64, 128, 256, 512, 1024, 4096 };

    @Override
    protected InJvmDTestVisitExecutor.PageSizeSelector pageSizeSelector(EntropySource rng)
    {
        return new InJvmDTestVisitExecutor.PageSizeSelector()
        {
            @Override
            public int pages(Visit visit)
            {
                int bytes = PAGE_SIZES_IN_BYTES[rng.nextInt(PAGE_SIZES_IN_BYTES.length)];
                logger.info("Byte-paged validation at lts {} with page size {} bytes", visit.lts, bytes);
                return bytes;
            }

            @Override
            public boolean isInBytes()
            {
                return true;
            }
        };
    }
}
