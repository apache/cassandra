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

package org.apache.cassandra.tools;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.ByteBuffer;
import java.util.*;

import org.apache.commons.cli.*;

import org.apache.cassandra.config.ConfigurationException;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.ExpiringColumn;
import org.apache.cassandra.db.IColumn;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.io.sstable.*;

import static org.apache.cassandra.utils.ByteBufferUtil.bytesToHex;
import static org.apache.cassandra.utils.ByteBufferUtil.hexToBytes;

/**
 * Export SSTables to JSON format.
 */
public class SSTableExport
{
    private static int INPUT_FILE_BUFFER_SIZE = 8 * 1024 * 1024;

    private static final String KEY_OPTION = "k";
    private static final String EXCLUDEKEY_OPTION = "x";
    private static final String ENUMERATEKEYS_OPTION = "e";
    private static Options options;
    private static CommandLine cmd;
    
    static
    {
        options = new Options();

        Option optKey = new Option(KEY_OPTION, true, "Row key");
        // Number of times -k <key> can be passed on the command line.
        optKey.setArgs(500);
        options.addOption(optKey);

        Option excludeKey = new Option(EXCLUDEKEY_OPTION, true, "Excluded row key");
        // Number of times -x <key> can be passed on the command line.
        excludeKey.setArgs(500);
        options.addOption(excludeKey);

        Option optEnumerate = new Option(ENUMERATEKEYS_OPTION, false, "enumerate keys only");
        options.addOption(optEnumerate);
    }
    
    private static String quote(String val)
    {
        return String.format("\"%s\"", val);
    }
    
    private static String asKey(String val)
    {
        return String.format("%s: ", quote(val));
    }
    
    private static void serializeColumns(PrintStream outs, Collection<IColumn> cols, AbstractType comp)
    {
        outs.print("[");

        Iterator<IColumn> iter = cols.iterator();
        while (iter.hasNext())
        {
            outs.print("[");
            IColumn column = iter.next();
            outs.print(quote(bytesToHex(column.name())));
            outs.print(", ");
            outs.print(quote(bytesToHex(column.value())));
            outs.print(", ");
            outs.print(column.timestamp());
            outs.print(", ");
            outs.print(column.isMarkedForDelete());
            if (column instanceof ExpiringColumn)
            {
                outs.print(", ");
                outs.print(((ExpiringColumn) column).getTimeToLive());
                outs.print(", ");
                outs.print(column.getLocalDeletionTime());
            }
            outs.print("]");
            if (iter.hasNext())
                outs.print(", ");
        }
        
        outs.print("]");
    }
    
    private static void serializeRow(PrintStream outs, SSTableIdentityIterator row) throws IOException
    {
        ColumnFamily cf = row.getColumnFamilyWithColumns();
        AbstractType comparator = cf.getComparator();
        outs.print(asKey(bytesToHex(row.getKey().key)));

        if (cf.isSuper())
        {
            outs.print("{ ");

            Iterator<IColumn> iter = cf.getSortedColumns().iterator();
            while (iter.hasNext())
            {
                IColumn column = iter.next();
                outs.print(asKey(bytesToHex(column.name())));
                outs.print("{");
                outs.print(asKey("deletedAt"));
                outs.print(column.getMarkedForDeleteAt());
                outs.print(", ");
                outs.print(asKey("subColumns"));
                serializeColumns(outs, column.getSubColumns(), comparator);
                outs.print("}");
                if (iter.hasNext())
                    outs.print(", ");
            }
            
            outs.print("}");
        }
        else
        {
            serializeColumns(outs, cf.getSortedColumns(), comparator);
        }
    }

    /**
     * Enumerate row keys from an SSTableReader and write the result to a PrintStream.
     * 
     * @param ssTableFile the file to export the rows from
     * @param outs PrintStream to write the output to
     * @throws IOException on failure to read/write input/output
     */
    public static void enumeratekeys(String ssTableFile, PrintStream outs)
    throws IOException
    {
        Descriptor desc = Descriptor.fromFilename(ssTableFile);
        KeyIterator iter = new KeyIterator(desc);
        DecoratedKey lastKey = null;
        while (iter.hasNext())
        {
            DecoratedKey key = iter.next();

            // validate order of the keys in the sstable
            if (lastKey != null && lastKey.compareTo(key) > 0 )
                throw new IOException("Key out of order! " + lastKey + " > " + key);
            lastKey = key;

            outs.println(bytesToHex(key.key));
        }
        iter.close();
        outs.flush();
    }

    /**
     * Export specific rows from an SSTable and write the resulting JSON to a PrintStream.
     * 
     * @param ssTableFile the SSTable to export the rows from
     * @param outs PrintStream to write the output to
     * @param keys the keys corresponding to the rows to export
     * @throws IOException on failure to read/write input/output
     */
    public static void export(String ssTableFile, PrintStream outs, String[] keys, String[] excludes)
    throws IOException
    {
        SSTableReader reader = SSTableReader.open(Descriptor.fromFilename(ssTableFile));
        SSTableScanner scanner = reader.getDirectScanner(INPUT_FILE_BUFFER_SIZE);
        IPartitioner<?> partitioner = DatabaseDescriptor.getPartitioner();    
        Set<String> excludeSet = new HashSet<String>();
        int i = 0;

        if (excludes != null)
            excludeSet = new HashSet<String>(Arrays.asList(excludes));
        
        outs.println("{");

        // last key to compare order 
        DecoratedKey lastKey = null;
        
        for (String key : keys)
        {
            if (excludeSet.contains(key))
                continue;
            DecoratedKey<?> dk = partitioner.decorateKey(hexToBytes(key));

            // validate order of the keys in the sstable
            if (lastKey != null && lastKey.compareTo(dk) > 0 )
                throw new IOException("Key out of order! " + lastKey + " > " + dk);
            lastKey = dk;

            scanner.seekTo(dk);
            
            i++;
            
            if (scanner.hasNext())
            {
                SSTableIdentityIterator row = (SSTableIdentityIterator) scanner.next();
                try
                {
                    serializeRow(outs, row);
                    if (i != 1)
                        outs.println(",");
                }
                catch (IOException ioexc)
                {
                    System.err.println("WARNING: Corrupt row " + key + " (skipping).");
                    continue;
                }
                catch (OutOfMemoryError oom)
                {
                    System.err.println("ERROR: Out of memory deserializing row " + key);
                    continue;
                }
            }
        }
        
        outs.println("\n}");
        outs.flush();
    }

    // This is necessary to accommodate the test suite since you cannot open a Reader more
    // than once from within the same process.
    static void export(SSTableReader reader, PrintStream outs, String[] excludes) throws IOException
    {
        SSTableScanner scanner = reader.getDirectScanner(INPUT_FILE_BUFFER_SIZE);
        Set<String> excludeSet = new HashSet<String>();

        if (excludes != null)
            excludeSet = new HashSet<String>(Arrays.asList(excludes));

        outs.println("{");

        SSTableIdentityIterator row;

        boolean elementWritten = false;
        while (scanner.hasNext())
        {
            row = (SSTableIdentityIterator) scanner.next();

            if (excludeSet.contains(bytesToHex(row.getKey().key)))
                continue;
            else if (elementWritten)
                outs.println(",");

            try
            {
                serializeRow(outs, row);

                // used to decide should we put ',' after previous row or not
                if (!elementWritten)
                    elementWritten = true;
            }
            catch (IOException ioexcep)
            {
                System.err.println("WARNING: Corrupt row " + bytesToHex(row.getKey().key) + " (skipping).");
                continue;
            }
            catch (OutOfMemoryError oom)
            {
                System.err.println("ERROR: Out of memory deserializing row " + bytesToHex(row.getKey().key));
                continue;
            }
        }
        
        outs.printf("%n}%n");
        outs.flush();
    }
    
    /**
     * Export an SSTable and write the resulting JSON to a PrintStream.
     * 
     * @param ssTableFile the SSTable to export
     * @param outs PrintStream to write the output to
     * @throws IOException on failure to read/write input/output
     */
    public static void export(String ssTableFile, PrintStream outs, String[] excludes) throws IOException
    {
        SSTableReader reader = SSTableReader.open(Descriptor.fromFilename(ssTableFile));
        export(reader, outs, excludes);
    }

    /**
     * Export an SSTable and write the resulting JSON to standard out.
     * 
     * @param ssTableFile SSTable to export
     * @throws IOException on failure to read/write SSTable/standard out
     */
    public static void export(String ssTableFile, String[] excludes) throws IOException
    {
        export(ssTableFile, System.out, excludes);
    }

    /**
     * Given arguments specifying an SSTable, and optionally an output file,
     * export the contents of the SSTable to JSON.
     *  
     * @param args command lines arguments
     * @throws IOException on failure to open/read/write files or output streams
     */
    public static void main(String[] args) throws IOException, ConfigurationException
    {
        String usage = String.format("Usage: %s <sstable> [-k key [-k key [...]] -x key [-x key [...]]]%n", SSTableExport.class.getName());
        
        CommandLineParser parser = new PosixParser();
        try
        {
            cmd = parser.parse(options, args);
        }
        catch (ParseException e1)
        {
            System.err.println(e1.getMessage());
            System.err.println(usage);
            System.exit(1);
        }


        if (cmd.getArgs().length != 1)
        {
            System.err.println("You must supply exactly one sstable");
            System.err.println(usage);
            System.exit(1);
        }
        

        String[] keys = cmd.getOptionValues(KEY_OPTION);
        String[] excludes = cmd.getOptionValues(EXCLUDEKEY_OPTION);
        String ssTableFileName = new File(cmd.getArgs()[0]).getAbsolutePath();

        DatabaseDescriptor.loadSchemas();
        if (DatabaseDescriptor.getNonSystemTables().size() < 1)
        {
            String msg = "no non-system tables are defined";
            System.err.println(msg);
            throw new ConfigurationException(msg);
        }

        if (cmd.hasOption(ENUMERATEKEYS_OPTION))
        {
            enumeratekeys(ssTableFileName, System.out);
        }
        else
        {
            if ((keys != null) && (keys.length > 0))
                export(ssTableFileName, System.out, keys, excludes);
            else
                export(ssTableFileName, excludes);
        }

        System.exit(0);
    }
}
