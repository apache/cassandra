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

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.index.Index;
import org.apache.cassandra.utils.concurrent.OpOrder;

public class ReadExecutionController implements AutoCloseable
{
    // For every reads
    private final OpOrder.Group baseOp;
    private final CFMetaData baseMetadata; // kept to sanity check that we have take the op order on the right table

    // For index reads
    private final ReadExecutionController indexController;
    private final OpOrder.Group writeOp;

    private ReadExecutionController(OpOrder.Group baseOp, CFMetaData baseMetadata, ReadExecutionController indexController, OpOrder.Group writeOp)
    {
        // We can have baseOp == null, but only when empty() is called, in which case the controller will never really be used
        // (which validForReadOn should ensure). But if it's not null, we should have the proper metadata too.
        assert (baseOp == null) == (baseMetadata == null);
        this.baseOp = baseOp;
        this.baseMetadata = baseMetadata;
        this.indexController = indexController;
        this.writeOp = writeOp;
    }

    public ReadExecutionController indexReadController()
    {
        return indexController;
    }

    public OpOrder.Group writeOpOrderGroup()
    {
        return writeOp;
    }

    public boolean validForReadOn(ColumnFamilyStore cfs)
    {
        return baseOp != null && cfs.metadata.cfId.equals(baseMetadata.cfId);
    }

    public static ReadExecutionController empty()
    {
        return new ReadExecutionController(null, null, null, null);
    }

    /**
     * Creates an execution controller for the provided command.
     * <p>
     * Note: no code should use this method outside of {@link ReadCommand#executionController} (for
     * consistency sake) and you should use that latter method if you need an execution controller.
     *
     * @param command the command for which to create a controller.
     * @return the created execution controller, which must always be closed.
     */
    @SuppressWarnings("resource") // ops closed during controller close
    static ReadExecutionController forCommand(ReadCommand command)
    {
        ColumnFamilyStore baseCfs = Keyspace.openAndGetStore(command.metadata());
        ColumnFamilyStore indexCfs = maybeGetIndexCfs(baseCfs, command);

        if (indexCfs == null)
        {
            return new ReadExecutionController(baseCfs.readOrdering.start(), baseCfs.metadata, null, null);
        }
        else
        {
            OpOrder.Group baseOp = null, writeOp = null;
            ReadExecutionController indexController = null;
            // OpOrder.start() shouldn't fail, but better safe than sorry.
            try
            {
                baseOp = baseCfs.readOrdering.start();
                indexController = new ReadExecutionController(indexCfs.readOrdering.start(), indexCfs.metadata, null, null);
                // TODO: this should perhaps not open and maintain a writeOp for the full duration, but instead only *try* to delete stale entries, without blocking if there's no room
                // as it stands, we open a writeOp and keep it open for the duration to ensure that should this CF get flushed to make room we don't block the reclamation of any room being made
                writeOp = Keyspace.writeOrder.start();
                return new ReadExecutionController(baseOp, baseCfs.metadata, indexController, writeOp);
            }
            catch (RuntimeException e)
            {
                // Note that must have writeOp == null since ReadOrderGroup ctor can't fail
                assert writeOp == null;
                try
                {
                    if (baseOp != null)
                        baseOp.close();
                }
                finally
                {
                    if (indexController != null)
                        indexController.close();
                }
                throw e;
            }
        }
    }

    private static ColumnFamilyStore maybeGetIndexCfs(ColumnFamilyStore baseCfs, ReadCommand command)
    {
        Index index = command.getIndex(baseCfs);
        return index == null ? null : index.getBackingTable().orElse(null);
    }

    public CFMetaData metaData()
    {
        return baseMetadata;
    }

    public void close()
    {
        try
        {
            if (baseOp != null)
                baseOp.close();
        }
        finally
        {
            if (indexController != null)
            {
                try
                {
                    indexController.close();
                }
                finally
                {
                    writeOp.close();
                }
            }
        }
    }
}
