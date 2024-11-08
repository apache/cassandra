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

package org.apache.cassandra.harry.execution;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;

import org.apache.cassandra.harry.gen.Bijections;
import org.apache.cassandra.harry.MagicConstants;
import org.apache.cassandra.harry.gen.ValueGenerators;
import org.apache.cassandra.harry.util.StringUtils;

public class ResultSetRow
{
    public final long pd;
    public final long cd;
    public final long[] vds;
    public final long[] lts;

    public final long[] sds;
    public final long[] slts;

    public ResultSetRow(long pd,
                        long cd,
                        long[] sds,
                        long[] slts,
                        long[] vds,
                        long[] lts)
    {
        this.pd = pd;
        this.cd = cd;
        this.vds = vds;
        this.lts = lts;
        this.sds = sds;
        this.slts = slts;
    }

    public boolean hasStaticColumns()
    {
        return slts.length > 0;
    }

    @Override
    public ResultSetRow clone()
    {
        return new ResultSetRow(pd, cd,
                                Arrays.copyOf(sds, sds.length), Arrays.copyOf(slts, slts.length),
                                Arrays.copyOf(vds, vds.length), Arrays.copyOf(lts, lts.length));
    }

    @Override
    public int hashCode()
    {
        int result = Objects.hash(pd, cd);
        result = 31 * result + Arrays.hashCode(vds);
        result = 31 * result + Arrays.hashCode(lts);
        result = 31 * result + Arrays.hashCode(sds);
        result = 31 * result + Arrays.hashCode(slts);
        return result;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ResultSetRow that = (ResultSetRow) o;
        return pd == that.pd &&
               cd == that.cd &&
               Arrays.equals(vds, that.vds) &&
               Arrays.equals(lts, that.lts) &&
               Arrays.equals(sds, that.sds) &&
               Arrays.equals(slts, that.slts);
    }

    public String toString()
    {
        return "resultSetRow("
               + pd +
               "L, " + cd +
               (sds == null ? "" : "L, statics(" + StringUtils.toString(sds) + ")") +
               (slts == null ? "" : ", lts(" + StringUtils.toString(slts) + ")") +
               ", values(" + StringUtils.toString(vds) + ")" +
               ", lts(" + StringUtils.toString(lts) + ")" +
               ")";
    }

    private static String toString(long[] descriptors, List<Bijections.IndexedBijection<Object>> gens)
    {
        int[] idxs = new int[gens.size()];
        for (int i = 0; i < descriptors.length; i++)
            idxs[i] = descrToIdx(gens.get(i), descriptors[i]);
        return StringUtils.toString(idxs);
    }

    private static int descrToIdx(Bijections.IndexedBijection<?> gen, long descr)
    {
        if (descr == MagicConstants.UNSET_DESCR)
            return MagicConstants.UNSET_IDX;

        if (descr == MagicConstants.NIL_DESCR)
            return MagicConstants.NIL_IDX;

        return gen.idxFor(descr);
    }

    public String toString(ValueGenerators valueGenerators)
    {
        return "resultSetRow("
               + valueGenerators.pkGen.idxFor(pd)
               + ", " + StringUtils.toString(descrToIdx(valueGenerators.ckGen, cd)) +
               (sds == null ? "" : ", statics(" + toString(sds, valueGenerators.staticColumnGens) + ")") +
               (slts == null ? "" : ", slts(" + StringUtils.toString(slts) + ")") +
               ", values(" + toString(vds, valueGenerators.regularColumnGens) + ")" +
               ", lts(" + StringUtils.toString(lts) + ")" +
               ")";
    }
}
