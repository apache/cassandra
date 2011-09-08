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

package org.apache.cassandra.service;

import java.io.*;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.Iterables;
import com.google.common.collect.MapMaker;
import org.apache.cassandra.config.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.concurrent.Stage;
import org.apache.cassandra.concurrent.StageManager;
import org.apache.cassandra.config.ConfigurationException;
import org.apache.cassandra.db.Column;
import org.apache.cassandra.db.IColumn;
import org.apache.cassandra.db.marshal.TimeUUIDType;
import org.apache.cassandra.db.migration.Migration;
import org.apache.cassandra.gms.*;
import org.apache.cassandra.io.util.FastByteArrayInputStream;
import org.apache.cassandra.io.util.FastByteArrayOutputStream;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;

public class MigrationManager implements IEndpointStateChangeSubscriber
{
    private static final Logger logger = LoggerFactory.getLogger(MigrationManager.class);

    // avoids re-pushing migrations that we're waiting on target to apply already
    private static Map<InetAddress,UUID> lastPushed = new MapMaker().expiration(1, TimeUnit.MINUTES).makeMap();

    public void onJoin(InetAddress endpoint, EndpointState epState) { 
        VersionedValue value = epState.getApplicationState(ApplicationState.SCHEMA);
        if (value != null)
        {
            UUID theirVersion = UUID.fromString(value.value);
            rectify(theirVersion, endpoint);
        }
    }

    public void onChange(InetAddress endpoint, ApplicationState state, VersionedValue value)
    {
        if (state != ApplicationState.SCHEMA)
            return;
        UUID theirVersion = UUID.fromString(value.value);
        rectify(theirVersion, endpoint);
    }

    /** gets called after a this node joins a cluster */
    public void onAlive(InetAddress endpoint, EndpointState state)
    { 
        VersionedValue value = state.getApplicationState(ApplicationState.SCHEMA);
        if (value != null)
        {
            UUID theirVersion = UUID.fromString(value.value);
            rectify(theirVersion, endpoint);
        }
    }

    public void onDead(InetAddress endpoint, EndpointState state) { }

    public void onRestart(InetAddress endpoint, EndpointState state) { }

    public void onRemove(InetAddress endpoint) { }
    
    /** 
     * will either push or pull an updating depending on who is behind.
     * fat clients should never push their schemas (since they have no local storage).
     */
    public static void rectify(UUID theirVersion, InetAddress endpoint)
    {
        UUID myVersion = Schema.instance.getVersion();
        if (theirVersion.timestamp() < myVersion.timestamp()
            && !StorageService.instance.isClientMode())
        {
            if (lastPushed.get(endpoint) == null || theirVersion.timestamp() >= lastPushed.get(endpoint).timestamp())
            {
                logger.debug("Schema on {} is old. Sending updates since {}", endpoint, theirVersion);
                Collection<IColumn> migrations = Migration.getLocalMigrations(theirVersion, myVersion);
                pushMigrations(endpoint, migrations);
                lastPushed.put(endpoint, TimeUUIDType.instance.compose(Iterables.getLast(migrations).name()));
            }
            else
            {
                logger.debug("Waiting for {} to process migrations up to {} before sending more",
                             endpoint, lastPushed.get(endpoint));
            }
        }
    }

    private static void pushMigrations(InetAddress endpoint, Collection<IColumn> migrations)
    {
        try
        {
            Message msg = makeMigrationMessage(migrations, Gossiper.instance.getVersion(endpoint));
            MessagingService.instance().sendOneWay(msg, endpoint);
        }
        catch (IOException ex)
        {
            throw new IOError(ex);
        }
    }

    /** actively announce a new version to active hosts via rpc */
    public static void announce(IColumn column)
    {

        Collection<IColumn> migrations = Collections.singleton(column);
        for (InetAddress endpoint : Gossiper.instance.getLiveMembers())
            pushMigrations(endpoint, migrations);
    }

    /** announce my version passively over gossip **/
    public static void passiveAnnounce(UUID version)
    {
        // this is for notifying nodes as they arrive in the cluster.
        Gossiper.instance.addLocalApplicationState(ApplicationState.SCHEMA, StorageService.instance.valueFactory.migration(version));
        logger.debug("Gossiping my schema version " + version);
    }

    /**
     * gets called during startup if we notice a mismatch between the current migration version and the one saved. This
     * can only happen as a result of the commit log recovering schema updates, which overwrites lastVersionId.
     * 
     * This method silently eats IOExceptions thrown by Migration.apply() as a result of applying a migration out of
     * order.
     */
    public static void applyMigrations(final UUID from, final UUID to) throws IOException
    {
        List<Future<?>> updates = new ArrayList<Future<?>>();
        Collection<IColumn> migrations = Migration.getLocalMigrations(from, to);
        for (IColumn col : migrations)
        {
            // assuming MessagingService.version_ is a bit of a risk, but you're playing with fire if you purposefully
            // take down a node to upgrade it during the middle of a schema update.
            final Migration migration = Migration.deserialize(col.value(), MessagingService.version_);
            Future<?> update = StageManager.getStage(Stage.MIGRATION).submit(new Runnable()
            {
                public void run()
                {
                    try
                    {
                        migration.apply();
                    }
                    catch (ConfigurationException ex)
                    {
                        // this happens if we try to apply something that's already been applied. ignore and proceed.
                        logger.debug("Migration not applied " + ex.getMessage());
                    }
                    catch (IOException ex)
                    {
                        throw new RuntimeException(ex);
                    }
                }
            });
            updates.add(update);
        }
        
        // wait on all the updates before proceeding.
        for (Future<?> f : updates)
        {
            try
            {
                f.get();
            }
            catch (InterruptedException e)
            {
                throw new IOException(e);
            }
            catch (ExecutionException e)
            {
                throw new IOException(e);
            }
        }
        passiveAnnounce(to); // we don't need to send rpcs, but we need to update gossip
    }

    // other half of transformation is in DefinitionsUpdateResponseVerbHandler.
    private static Message makeMigrationMessage(Collection<IColumn> migrations, int version) throws IOException
    {
    	FastByteArrayOutputStream bout = new FastByteArrayOutputStream();
        DataOutputStream dout = new DataOutputStream(bout);
        dout.writeInt(migrations.size());
        // riddle me this: how do we know that these binary values (which contained serialized row mutations) are compatible
        // with the destination?  Further, since these migrations may be old, how do we know if they are compatible with
        // the current version?  The bottom line is that we don't.  For this reason, running migrations from a new node
        // to an old node will be a crap shoot.  Pushing migrations from an old node to a new node should work, so long
        // as the oldest migrations are only one version old.  We need a way of flattening schemas so that this isn't a
        // problem during upgrades.
        for (IColumn col : migrations)
        {
            assert col instanceof Column;
            ByteBufferUtil.writeWithLength(col.name(), dout);
            ByteBufferUtil.writeWithLength(col.value(), dout);
        }
        dout.close();
        byte[] body = bout.toByteArray();
        return new Message(FBUtilities.getBroadcastAddress(), StorageService.Verb.DEFINITIONS_UPDATE, body, version);
    }
    
    // other half of this transformation is in MigrationManager.
    public static Collection<Column> makeColumns(Message msg) throws IOException
    {
        Collection<Column> cols = new ArrayList<Column>();
        DataInputStream in = new DataInputStream(new FastByteArrayInputStream(msg.getMessageBody()));
        int count = in.readInt();
        for (int i = 0; i < count; i++)
        {
            byte[] name = new byte[in.readInt()];
            in.readFully(name);
            byte[] value = new byte[in.readInt()];
            in.readFully(value);
            cols.add(new Column(ByteBuffer.wrap(name), ByteBuffer.wrap(value)));
        }
        in.close();
        return cols;
    }
}
