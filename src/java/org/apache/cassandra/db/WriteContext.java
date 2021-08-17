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

package org.apache.cassandra.db;

/**
 * Issued by the keyspace write handler and used in the write path (as expected), as well as the read path
 * and some async index building code. In the read and index paths, the write context is intended to be used
 * as a marker for ordering operations. Reads can also end up performing writes in some cases, particularly
 * when correcting secondary indexes.
 */
public interface WriteContext extends AutoCloseable
{
    @Override
    void close();
}
