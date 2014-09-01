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
package org.apache.cassandra.db.rows;

import java.nio.ByteBuffer;
import java.security.MessageDigest;

import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.db.Aliasable;
import org.apache.cassandra.db.LivenessInfo;

/**
 * A cell holds a single "simple" value for a given column, as well as "liveness"
 * informations regarding that value.
 * <p>
 * The is 2 kind of columns: simple ones and complex ones.
 * Simple columns have only a single associated cell, while complex ones,
 * the one corresponding to non-frozen collections and UDTs, are comprised
 * of multiple cells. For complex columns, the different cells are distinguished
 * by their cell path.
 * <p>
 * We can also distinguish different kind of cells based on the property of their
 * {@link #livenessInfo}:
 *  1) "Normal" cells: their liveness info has no ttl and no deletion time.
 *  2) Expiring cells: their liveness info has both a ttl and a deletion time (the latter
 *    deciding when the cell is actually expired).
 *  3) Tombstones/deleted cells: their liveness info has a deletion time but no ttl. Those
 *     cells don't really have a value but their {@link #value} method return an empty
 *     buffer by convention.
 */
public interface Cell extends Aliasable<Cell>
{
    /**
     * The column this cell belongs to.
     *
     * @return the column this cell belongs to.
     */
    public ColumnDefinition column();

    /**
     * Whether the cell is a counter cell or not.
     *
     * @return whether the cell is a counter cell or not.
     */
    public boolean isCounterCell();

    /**
     * The cell value.
     *
     * @return the cell value.
     */
    public ByteBuffer value();

    /**
     * The liveness info of the cell, that is its timestamp and whether it is
     * expiring, deleted or none of the above.
     *
     * @return the cell {@link LivenessInfo}.
     */
    public LivenessInfo livenessInfo();

    /**
     * Whether the cell is a tombstone or not.
     *
     * @return whether the cell is a tombstone or not.
     */
    public boolean isTombstone();

    /**
     * Whether the cell is an expiring one or not.
     * <p>
     * Note that this only correspond to whether the cell liveness info
     * have a TTL or not, but doesn't tells whether the cell is already expired
     * or not. You should use {@link #isLive} for that latter information.
     *
     * @return whether the cell is an expiring one or not.
     */
    public boolean isExpiring();

    /**
     * Whether the cell is live or not given the current time.
     *
     * @param nowInSec the current time in seconds. This is used to
     * decide if an expiring cell is expired or live.
     * @return whether the cell is live or not at {@code nowInSec}.
     */
    public boolean isLive(int nowInSec);

    /**
     * For cells belonging to complex types (non-frozen collection and UDT), the
     * path to the cell.
     *
     * @return the cell path for cells of complex column, and {@code null} for other cells.
     */
    public CellPath path();

    /**
     * Write the cell to the provided writer.
     *
     * @param writer the row writer to write the cell to.
     */
    public void writeTo(Row.Writer writer);

    /**
     * Adds the cell to the provided digest.
     *
     * @param digest the {@code MessageDigest} to add the cell to.
     */
    public void digest(MessageDigest digest);

    /**
     * Validate the cell value.
     *
     * @throws MarshalException if the cell value is not a valid value for
     * the column type this is a cell of.
     */
    public void validate();

    /**
     * The size of the data hold by this cell.
     *
     * This is mainly used to verify if batches goes over a given size.
     *
     * @return the size used by the data of this cell.
     */
    public int dataSize();
}
