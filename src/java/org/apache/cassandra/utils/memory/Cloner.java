/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.cassandra.utils.memory;

import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.rows.Cell;

/**
 * Allow cloning of partition elements
 *
 */
public interface Cloner
{
    /**
     * Clones the specified key.
     *
     * @param key the key to clone
     * @return the cloned key
     */
    DecoratedKey clone(DecoratedKey key);

    /**
     * Clones the specified clustering.
     *
     * @param clustering the clustering to clone
     * @return the cloned clustering
     */
    Clustering<?> clone(Clustering<?> clustering);

    /**
     * Clones the specified cell.
     *
     * @param cell the cell to clone
     * @return the cloned cell
     */
    Cell<?> clone(Cell<?> cell);
}