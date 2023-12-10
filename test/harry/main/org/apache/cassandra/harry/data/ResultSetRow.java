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

package org.apache.cassandra.harry.data;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

import org.apache.cassandra.harry.ddl.SchemaSpec;
import org.apache.cassandra.harry.util.StringUtils;

public class ResultSetRow
{
    public final long pd;
    public final long cd;
    public final long[] vds;
    public final long[] lts;

    public final long[] sds;
    public final long[] slts;
    public final List<Long> visited_lts;

    private ResultSetRow(long pd,
                         long cd,
                         long[] sds,
                         long[] slts,
                         long[] vds,
                         long[] lts)
    {
        this(pd, cd, sds, slts, vds, lts, Collections.emptyList());
    }

    public ResultSetRow(long pd,
                        long cd,
                        long[] sds,
                        long[] slts,
                        long[] vds,
                        long[] lts,
                        List<Long> visited_lts)
    {
        // There should be as many timestamps as value descriptors, both for regular and static columns
        assert slts.length == sds.length : String.format("Descriptors: %s, timestamps: %s", Arrays.toString(sds), Arrays.toString(slts));
        assert lts.length == vds.length : String.format("Descriptors: %s, timestamps: %s", Arrays.toString(vds), Arrays.toString(lts));
        this.pd = pd;
        this.cd = cd;
        this.vds = vds;
        this.lts = lts;
        this.sds = sds;
        this.slts = slts;
        this.visited_lts = visited_lts;
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
                                Arrays.copyOf(vds, vds.length), Arrays.copyOf(lts, lts.length),
                                visited_lts);
    }

    @Override
    public int hashCode()
    {
        int result = Objects.hash(pd, cd, visited_lts);
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
               Arrays.equals(slts, that.slts) &&
               Objects.equals(visited_lts, that.visited_lts);
    }

    @Override
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

    public String toString(SchemaSpec schema)
    {
        return "resultSetRow("
               + pd +
               "L, " + cd +
               (sds == null ? "" : "L, staticValues(" + StringUtils.toString(sds) + ")") +
               (slts == null ? "" : ", slts(" + StringUtils.toString(slts) + ")") +
               ", values(" + StringUtils.toString(vds) + ")" +
               ", lts(" + StringUtils.toString(lts) + ")" +
               ", clustering=" + Arrays.toString(schema.inflateClusteringKey(cd)) +
               ", values=" + Arrays.toString(schema.inflateRegularColumns(vds)) +
               (sds == null ? "" : ", statics=" + Arrays.toString(schema.inflateStaticColumns(sds))) +
               ")";
    }
}
