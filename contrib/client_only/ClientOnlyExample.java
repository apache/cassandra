/**
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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import org.apache.cassandra.db.*;
import org.apache.cassandra.db.filter.QueryPath;
import org.apache.cassandra.service.StorageProxy;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.thrift.ColumnPath;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.utils.ByteBufferUtil;

public class ClientOnlyExample
{

    private static void testWriting() throws Exception
    {
        StorageService.instance.initClient();
        // sleep for a bit so that gossip can do its thing.
        try
        {
            Thread.sleep(10000L);
        }
        catch (Exception ex)
        {
            throw new AssertionError(ex);
        }

        // do some writing.
        for (int i = 0; i < 100; i++)
        {
            RowMutation change = new RowMutation("Keyspace1", ByteBuffer.wrap(("key" + i).getBytes()));
            ColumnPath cp = new ColumnPath("Standard1").setColumn(("colb").getBytes());
            change.add(new QueryPath(cp), ByteBuffer.wrap(("value" + i).getBytes()), 0);

            // don't call change.apply().  The reason is that is makes a static call into Table, which will perform
            // local storage initialization, which creates local directories.
            // change.apply();

            StorageProxy.mutate(Arrays.asList(change), ConsistencyLevel.ONE);
            System.out.println("wrote key" + i);
        }
        System.out.println("Done writing.");
        StorageService.instance.stopClient();
    }

    private static void testReading() throws Exception
    {
        StorageService.instance.initClient();
        // sleep for a bit so that gossip can do its thing.
        try
        {
            Thread.sleep(10000L);
        }
        catch (Exception ex)
        {
            throw new AssertionError(ex);
        }

        // do some queries.
        Collection<ByteBuffer> cols = new ArrayList<ByteBuffer>()
        {{
            add(ByteBuffer.wrap("colb".getBytes()));
        }};
        for (int i = 0; i < 100; i++)
        {
            List<ReadCommand> commands = new ArrayList<ReadCommand>();
            SliceByNamesReadCommand readCommand = new SliceByNamesReadCommand("Keyspace1", ByteBuffer.wrap(("key" + i).getBytes()),
                                                                              new QueryPath("Standard1", null, null), cols);
            readCommand.setDigestQuery(false);
            commands.add(readCommand);
            List<Row> rows = StorageProxy.readProtocol(commands, ConsistencyLevel.ONE);
            assert rows.size() == 1;
            Row row = rows.get(0);
            ColumnFamily cf = row.cf;
            if (cf != null)
            {
                for (IColumn col : cf.getSortedColumns())
                {
                    System.out.println(ByteBufferUtil.string(col.name(), Charsets.UTF_8) + ", " + ByteBufferUtil.string(col.value(), Charsets.UTF_8));
                }
            }
            else
                System.err.println("This output indicates that nothing was read.");
        }

        // no need to do this:
        // StorageService.instance().decommission();
        // do this instead:
        StorageService.instance.stopClient();
    }

    /**
     * First, bring one or more nodes up. Then run ClientOnlyExample with these VM arguments:
     *
     -Xmx1G
     -Dstorage-config=/Users/gary.dusbabek/cass-configs/trunk/conf3-client

     Pass "write" or "read" into the program to exercise the various methods.

     Caveats:

     1.  Much of cassandra is not reentrant.  That is, you can't spin a client up, down, then back up in the same jvm.
     2.  Because of the above, you still need to force-quit the process. StorageService.stopClient() doesn't (can't)
         spin everything down.
     */
    public static void main(String args[]) throws Exception
    {
        if (args.length == 0)
            System.out.println("run with \"read\" or \"write\".");
        else if ("read".equalsIgnoreCase(args[0]))
        {
            testReading();
        }
        else if ("write".equalsIgnoreCase(args[0]))
        {
            testWriting();
        }
    }
}
