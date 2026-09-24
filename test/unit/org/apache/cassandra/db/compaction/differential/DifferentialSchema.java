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

package org.apache.cassandra.db.compaction.differential;

/**
 * One named table shape and the write workload that populates it.
 *
 * <p>A schema is path-agnostic: it names a CQL table definition and drives writes through a
 * {@link DifferentialWorkload}, referencing no compaction, flush or read type. The same fixture can thus
 * feed a compaction differential here and, later, read-path and flush-path differential engines on higher
 * branches.
 *
 * <p>Implementations must produce at least two overlapping flushes in {@link #write}, so any engine built
 * on the fixture has at least two inputs whose partitions merge.
 */
public interface DifferentialSchema
{
    /** Short, kebab-ish name used to label parameterized cases, e.g. {@code "clustering-free"}. */
    String name();

    /** The CQL {@code CREATE TABLE %s (...)} definition, with the {@code %s} placeholder CQLTester expects. */
    String tableDefinition();

    /**
     * Applies the write workload, cutting it into at least two overlapping flushes that merge.
     *
     * @param scale multiplies the shape's base partition/row counts; {@code scale == 1} is the fast-matrix
     *              size, larger values feed a burn test. Fixed padding sizes (e.g. multi-block value width)
     *              do not scale — only counts do.
     */
    void write(DifferentialWorkload workload, int scale);

    /**
     * Whether the shape intends at least one partition to span more than one column-index block, so an
     * engine can guard that the block-navigation path is actually exercised. Defaults to false.
     */
    default boolean spansMultipleIndexBlocks()
    {
        return false;
    }
}
