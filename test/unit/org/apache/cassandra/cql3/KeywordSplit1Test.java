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

package org.apache.cassandra.cql3;

import java.util.Collection;

import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/**
 * This base class tests all keywords which took a long time. Hence it was split into multiple
 * KeywordTestSplitN to prevent CI timing out. If timeouts reappear split it further
 */
@RunWith(Parameterized.class)
public class KeywordSplit1Test extends KeywordTestBase
{
    static int SPLIT = 1;
    static int TOTAL_SPLITS = 2;

    @Parameterized.Parameters(name = "keyword {0} isReserved {1}")
    public static Collection<Object[]> keywords() {
        return KeywordTestBase.getKeywordsForSplit(SPLIT, TOTAL_SPLITS);
    }

    public KeywordSplit1Test(String keyword, boolean isReserved)
    {
        super(keyword, isReserved);
    }
}
