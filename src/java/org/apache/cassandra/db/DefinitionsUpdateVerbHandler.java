/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.db;

import java.io.IOError;
import java.io.IOException;
import java.util.Collection;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.concurrent.Stage;
import org.apache.cassandra.concurrent.StageManager;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.config.ConfigurationException;
import org.apache.cassandra.db.migration.Migration;
import org.apache.cassandra.net.IVerbHandler;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.service.MigrationManager;
import org.apache.cassandra.utils.UUIDGen;
import org.apache.cassandra.utils.WrappedRunnable;

public class DefinitionsUpdateVerbHandler implements IVerbHandler
{
    private static final Logger logger = LoggerFactory.getLogger(DefinitionsUpdateVerbHandler.class);

    /** someone sent me their data definitions */
    public void doVerb(final Message message, String id)
    {
        try
        {
            // these are the serialized row mutations that I must apply.
            // check versions at every step along the way to make sure migrations are not applied out of order.
            Collection<Column> cols = MigrationManager.makeColumns(message);
            for (Column col : cols)
            {
                final UUID version = UUIDGen.getUUID(col.name());
                if (version.timestamp() > Schema.instance.getVersion().timestamp())
                {
                    final Migration m = Migration.deserialize(col.value(), message.getVersion());
                    assert m.getVersion().equals(version);
                    StageManager.getStage(Stage.MIGRATION).submit(new WrappedRunnable()
                    {
                        protected void runMayThrow() throws Exception
                        {
                            // check to make sure the current version is before this one.
                            if (Schema.instance.getVersion().timestamp() == version.timestamp())
                                logger.debug("Not appling (equal) " + version.toString());
                            else if (Schema.instance.getVersion().timestamp() > version.timestamp())
                                logger.debug("Not applying (before)" + version.toString());
                            else
                            {
                                logger.debug("Applying {} from {}", m.getClass().getSimpleName(), message.getFrom());
                                try
                                {
                                    m.apply();
                                    // update gossip, but don't contact nodes directly
                                    m.passiveAnnounce();
                                }
                                catch (ConfigurationException ex)
                                {
                                    // Trying to apply the same migration twice. This happens as a result of gossip.
                                    logger.debug("Migration not applied " + ex.getMessage());
                                }
                            }
                        }
                    });
                }
            }
        }
        catch (IOException ex)
        {
            throw new IOError(ex);
        }
    }
}
