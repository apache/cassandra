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

import org.apache.cassandra.db.*;
import org.apache.cassandra.db.filter.QueryPath;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.service.*;
import org.apache.cassandra.thrift.ColumnPath;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.UnavailableException;

import static org.apache.cassandra.thrift.ThriftGlue.createColumnPath;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeoutException;

public class ClientOnlyExample
{

    private static void testWriting() throws IOException
    {
        StorageService.instance.initClient();
        // sleep for a bit so that gossip can do its thing.
        try
        {
            Thread.sleep(10000L);
        }
        catch (Exception ex)
        {
        }

        // do some writing.
        final AbstractType comp = ColumnFamily.getComparatorFor("Keyspace1", "Standard1", null);
        for (int i = 0; i < 100; i++)
        {
            RowMutation change = new RowMutation("Keyspace1", "key" + i);
            ColumnPath cp = createColumnPath("Standard1", null, ("colb").getBytes());
            change.add(new QueryPath(cp), ("value" + i).getBytes(), 0);

            // don't call change.apply().  The reason is that is makes a static call into Table, which will perform
            // local storage initialization, which creates local directories.
            // change.apply();

            StorageProxy.mutate(Arrays.asList(change));
            try
            {
                Thread.sleep(50L);
            }
            catch (Exception ex)
            {
            }
            System.out.println("wrote key" + i);
        }
        System.out.println("Done writing.");
        StorageService.instance.stopClient();
    }

    private static void testReading() throws IOException
    {
        StorageService.instance.initClient();
        // sleep for a bit so that gossip can do its thing.
        try
        {
            Thread.sleep(10000L);
        }
        catch (Exception ex)
        {
        }

        // do some queries.
        Collection<byte[]> cols = new ArrayList<byte[]>()
        {{
            add("colb".getBytes());
        }};
        for (int i = 0; i < 100; i++)
        {
            List<ReadCommand> commands = new ArrayList<ReadCommand>();
            SliceByNamesReadCommand readCommand = new SliceByNamesReadCommand("Keyspace1", "key" + i, new QueryPath("Standard1", null, null), cols);
            readCommand.setDigestQuery(false);
            commands.add(readCommand);
            try
            {
                List<Row> rows = StorageProxy.readProtocol(commands, ConsistencyLevel.ONE);
                assert rows.size() == 1;
                Row row = rows.get(0);
                ColumnFamily cf = row.cf;
                if (cf != null)
                {
                    for (IColumn col : cf.getSortedColumns())
                    {
                        System.out.println(new String(col.name()) + ", " + new String(col.value()));
                    }
                }
                else
                    System.err.println("This output indicates that nothing was read.");
            }
            catch (UnavailableException e)
            {
                throw new RuntimeException(e);
            }
            catch (TimeoutException e)
            {
                throw new RuntimeException(e);
            }

        }

        // no need to do this:
        // StorageService.instance().decommission();
        // do this instead:
        StorageService.instance.stopClient();
    }

    /**
     * First, bring one or more nodes up. Then run ClientOnlyExample with these VM arguments:
     *
     -Xms128M
     -Xmx1G
     -Dstorage-config=/Users/gary.dusbabek/cass-configs/trunk/conf3-client

     Pass "write" or "read" into the program to exercise the various methods.

     Caveats:

     1.  Much of cassandra is not reentrant.  That is, you can't spin a client up, down, then back up in the same jvm.
     2.  Because of the above, you still need to force-quit the process. StorageService.stopClient() doesn't (can't)
         spin everything down.
     */
    public static void main(String args[])
    {
        if (args.length == 0)
            System.out.println("run with \"read\" or \"write\".");
        else if ("read".equalsIgnoreCase(args[0]))
        {
            try
            {
                testReading();
            }
            catch (IOException e)
            {
                throw new RuntimeException(e);
            }
        }
        else if ("write".equalsIgnoreCase(args[0]))
        {
            try
            {
                testWriting();
            }
            catch (IOException e)
            {
                throw new RuntimeException(e);
            }
        }
    }
}
