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

package org.apache.cassandra.harry;

import java.util.List;

import org.apache.cassandra.harry.gen.Bijections;
import org.apache.cassandra.harry.gen.EntropySource;

public class ValueGeneratorHelper
{
    public static long[] randomDescriptors(EntropySource rng, List<Bijections.IndexedBijection<Object>> valueGens)
    {
        long[] vds = new long[valueGens.size()];
        for (int i = 0; i < valueGens.size(); i++)
        {
            if (rng.nextBoolean())
                vds[i] = MagicConstants.UNSET_DESCR;
            else
                vds[i] = valueGens.get(i).descriptorAt(rng.nextInt(valueGens.size()));
        }

        return vds;
    }
}
