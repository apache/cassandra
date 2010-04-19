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

import org.apache.cassandra.concurrent.StageManager;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.Column;
import org.apache.cassandra.db.IColumn;
import org.apache.cassandra.db.migration.Migration;
import org.apache.cassandra.gms.ApplicationState;
import org.apache.cassandra.gms.EndpointState;
import org.apache.cassandra.gms.IEndpointStateChangeSubscriber;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.utils.FBUtilities;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOError;
import java.io.IOException;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Set;
import java.util.UUID;

public class MigrationManager implements IEndpointStateChangeSubscriber
{
    public static final String MIGRATION_STATE = "MIGRATION";
    private static final Logger logger = LoggerFactory.getLogger(MigrationManager.class);
    
    /** I'm not going to act here. */
    public void onJoin(InetAddress endpoint, EndpointState epState) { }

    public void onChange(InetAddress endpoint, String stateName, ApplicationState state)
    {
        if (!MIGRATION_STATE.equals(stateName))
            return;
        UUID theirVersion = UUID.fromString(state.getValue());
        rectify(theirVersion, endpoint);
    }

    /** gets called after a this node joins a cluster */
    public void onAlive(InetAddress endpoint, EndpointState state)
    { 
        ApplicationState appState = state.getApplicationState(MIGRATION_STATE);
        if (appState != null)
        {
            UUID theirVersion = UUID.fromString(appState.getValue());
            rectify(theirVersion, endpoint);
        }
    }

    public void onDead(InetAddress endpoint, EndpointState state) { }
    
    /** will either push or pull an updating depending on who is behind. */
    public static void rectify(UUID theirVersion, InetAddress endpoint)
    {
        UUID myVersion = DatabaseDescriptor.getDefsVersion();
        if (theirVersion.timestamp() == myVersion.timestamp())
            return;
        else if (theirVersion.timestamp() > myVersion.timestamp())
        {
            logger.debug("My data definitions are old. Asking for updates since {}", myVersion.toString());
            announce(myVersion, Collections.singleton(endpoint));
        }
        else
        {
            logger.debug("Their data definitions are old. Sending updates since {}", theirVersion.toString());
            pushMigrations(theirVersion, myVersion, endpoint);
        }
    }

    /** announce my version to a set of hosts.  They may culminate with them sending me migrations. */
    public static void announce(UUID version, Set<InetAddress> hosts)
    {
        Message msg = makeVersionMessage(version);
        for (InetAddress host : hosts)
            MessagingService.instance.sendOneWay(msg, host);
    }
    
    /** pushes migrations from this host to another host */
    public static void pushMigrations(UUID from, UUID to, InetAddress host)
    {
        // I want all the rows from theirVersion through myVersion.
        Collection<IColumn> migrations = Migration.getLocalMigrations(from, to);
        try
        {
            Message msg = makeMigrationMessage(migrations);
            MessagingService.instance.sendOneWay(msg, host);
        }
        catch (IOException ex)
        {
            throw new IOError(ex);
        }
    }
    
    private static Message makeVersionMessage(UUID version)
    {
        byte[] body = version.toString().getBytes();
        return new Message(FBUtilities.getLocalAddress(), StageManager.READ_STAGE, StorageService.Verb.DEFINITIONS_ANNOUNCE, body);
    }
    
    // other half of transformation is in DefinitionsUpdateResponseVerbHandler.
    private static Message makeMigrationMessage(Collection<IColumn> migrations) throws IOException
    {
        ByteArrayOutputStream bout = new ByteArrayOutputStream();
        DataOutputStream dout = new DataOutputStream(bout);
        dout.writeInt(migrations.size());
        for (IColumn col : migrations)
        {
            assert col instanceof Column;
            dout.writeInt(col.name().length);
            dout.write(col.name());
            dout.writeInt(col.value().length);
            dout.write(col.value());
        }
        dout.close();
        byte[] body = bout.toByteArray();
        return new Message(FBUtilities.getLocalAddress(), StageManager.MUTATION_STAGE, StorageService.Verb.DEFINITIONS_UPDATE_RESPONSE, body);
    }
    
    // other half of this transformation is in MigrationManager.
    public static Collection<Column> makeColumns(Message msg) throws IOException
    {
        Collection<Column> cols = new ArrayList<Column>();
        DataInputStream in = new DataInputStream(new ByteArrayInputStream(msg.getMessageBody()));
        int count = in.readInt();
        for (int i = 0; i < count; i++)
        {
            byte[] name = new byte[in.readInt()];
            in.readFully(name);
            byte[] value = new byte[in.readInt()];
            in.readFully(value);
            cols.add(new Column(name, value));
        }
        in.close();
        return cols;
    }
}
