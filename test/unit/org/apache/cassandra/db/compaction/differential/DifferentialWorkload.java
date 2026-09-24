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
 * The minimal surface a schema fixture needs to populate a table with data and cut it into sstables.
 *
 * <p>This interface is deliberately path-agnostic: it references no compaction, flush or read type, so
 * the fixture corpus that depends on it ({@link DifferentialSchema}, {@link DifferentialSchemas}) can be
 * hoisted into shared test infrastructure and reused by read-path and flush-path differential engines on
 * higher branches. An adapter supplies the actual execution and flush, delegating to CQLTester.
 */
public interface DifferentialWorkload
{
    /** Runs one CQL statement, with CQLTester's {@code %s} table placeholder and bind arguments. */
    void execute(String cql, Object... args);

    /** Flushes the memtable to a new sstable, ending the current round of writes. */
    void flush();
}
