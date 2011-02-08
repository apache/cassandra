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

import org.apache.cassandra.db.*;
import org.apache.cassandra.db.columniterator.IColumnIterator;
import org.apache.cassandra.db.filter.QueryFilter;
import org.apache.cassandra.db.filter.QueryPath;
import org.apache.cassandra.io.util.BufferedRandomAccessFile;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.commons.cli.*;

import org.apache.cassandra.config.ConfigurationException;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.io.sstable.*;

import static org.apache.cassandra.utils.ByteBufferUtil.bytesToHex;
import static org.apache.cassandra.utils.ByteBufferUtil.hexToBytes;

/**
 * Export SSTables to JSON format.
 */
public class SSTableExport
{
    // size of the columns page
    private static final int PAGE_SIZE = 1000;

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

    /**
     * Wraps given string into quotes
     * @param val string to quote
     * @return quoted string
     */
    private static String quote(String val)
    {
        return String.format("\"%s\"", val);
    }

    /**
     * JSON Hash Key serializer
     * @param val value to set as a key
     * @return JSON Hash key
     */
    private static String asKey(String val)
    {
        return String.format("%s: ", quote(val));
    }

    /**
     * Serialize columns using given column iterator
     * @param columns column iterator
     * @param out output stream
     */
    private static void serializeColumns(Iterator<IColumn> columns, PrintStream out)
    {
        while (columns.hasNext())
        {
            serializeColumn(columns.next(), out);

            if (columns.hasNext())
                out.print(", ");
        }
    }

    /**
     * Serialize a collection of the columns
     * @param columns collection of the columns to serialize
     * @param out output stream
     */
    private static void serializeColumns(Collection<IColumn> columns, PrintStream out)
    {
        serializeColumns(columns.iterator(), out);
    }

    /**
     * Serialize a given column to the JSON format
     * @param column column presentation
     * @param out output stream
     */
    private static void serializeColumn(IColumn column, PrintStream out)
    {
        out.print("[");
        out.print(quote(bytesToHex(column.name())));
        out.print(", ");
        out.print(quote(bytesToHex(column.value())));
        out.print(", ");
        out.print(column.timestamp());
        out.print(", ");
        out.print(column.isMarkedForDelete());

        if (column instanceof ExpiringColumn)
        {
            out.print(", ");
            out.print(((ExpiringColumn) column).getTimeToLive());
            out.print(", ");
            out.print(column.getLocalDeletionTime());
        }

        out.print("]");
    }

    /**
     * Get portion of the columns and serialize in loop while not more columns left in the row
     * @param reader SSTableReader for given SSTable
     * @param row SSTableIdentityIterator row representation with Column Family
     * @param key Decorated Key for the required row
     * @param out output stream
     */
    private static void serializeRow(SSTableReader reader, SSTableIdentityIterator row, DecoratedKey key, PrintStream out)
    {
        ColumnFamily columnFamily = row.getColumnFamily();
        boolean isSuperCF = columnFamily.isSuper();
        ByteBuffer startColumn = ByteBufferUtil.EMPTY_BYTE_BUFFER; // initial column name, "blank" for first

        out.print(asKey(bytesToHex(key.key)));

        out.print(isSuperCF ? "{" : "[");

        while (true)
        {
            QueryFilter filter = QueryFilter.getSliceFilter(key,
                                                            new QueryPath(columnFamily.metadata().tableName),
                                                            startColumn,
                                                            ByteBufferUtil.EMPTY_BYTE_BUFFER,
                                                            false,
                                                            PAGE_SIZE);

            IColumnIterator columns = filter.getSSTableColumnIterator(reader);

            int columnCount = 0;
            while (columns.hasNext())
            {
                // setting new start column to the last of the current columns
                startColumn = columns.next().name();
                columnCount++;
            }

            try
            {
                columns = filter.getSSTableColumnIterator(reader); // iterator reset
                serializeRow(columns, isSuperCF, out);
            }
            catch (IOException e)
            {
                System.err.println("WARNING: Corrupt row " + key + " (skipping).");
            }

            if (columnCount < PAGE_SIZE)
                break;
        }

        out.print(isSuperCF ? "}" : "]");
    }

    /**
     * Serialize a row with already given column iterator
     *
     * @param columns columns of the row
     * @param isSuper true if wrapping Column Family is Super
     * @param out output stream
     *
     * @throws IOException on any I/O error.
     */
    private static void serializeRow(IColumnIterator columns, boolean isSuper, PrintStream out) throws IOException
    {
        if (isSuper)
        {
            while (columns.hasNext())
            {
                IColumn column = columns.next();

                out.print(asKey(bytesToHex(column.name())));
                out.print("{");
                out.print(asKey("deletedAt"));
                out.print(column.getMarkedForDeleteAt());
                out.print(", ");
                out.print(asKey("subColumns"));
                out.print("[");
                serializeColumns(column.getSubColumns(), out);
                out.print("]");
                out.print("}");

                if (columns.hasNext())
                    out.print(", ");
            }
        }
        else
        {
            serializeColumns(columns, out);
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
     * @param ssTableFile the SSTableScanner to export the rows from
     * @param outs PrintStream to write the output to
     * @param toExport the keys corresponding to the rows to export
     * @param excludes keys to exclude from export
     * @throws IOException on failure to read/write input/output
     */
    public static void export(String ssTableFile, PrintStream outs, Collection<String> toExport, String[] excludes) throws IOException
    {
        SSTableReader reader = SSTableReader.open(Descriptor.fromFilename(ssTableFile));
        SSTableScanner scanner = reader.getDirectScanner(BufferedRandomAccessFile.DEFAULT_BUFFER_SIZE);

        IPartitioner<?> partitioner = StorageService.getPartitioner();

        for (String toExclude : excludes)
        {
            toExport.remove(toExclude); // excluding key from export
        }

        outs.println("{");

        int i = 0;

        // last key to compare order
        DecoratedKey lastKey = null;

        for (String key : toExport)
        {
            DecoratedKey decoratedKey = partitioner.decorateKey(hexToBytes(key));

            if (lastKey != null && lastKey.compareTo(decoratedKey) > 0)
                throw new IOException("Key out of order! " + lastKey + " > " + decoratedKey);

            lastKey = decoratedKey;

            scanner.seekTo(decoratedKey);

            if (!scanner.hasNext())
                continue;

            serializeRow(reader, (SSTableIdentityIterator) scanner.next(), decoratedKey, outs);

            if (i != 0)
                outs.println(",");

            i++;
        }

        outs.println("\n}");
        outs.flush();

        scanner.close();
    }

    // This is necessary to accommodate the test suite since you cannot open a Reader more
    // than once from within the same process.
    static void export(SSTableReader reader, PrintStream outs, String[] excludes) throws IOException
    {
        Set<String> excludeSet = new HashSet<String>();

        if (excludes != null)
            excludeSet = new HashSet<String>(Arrays.asList(excludes));


        SSTableIdentityIterator row;
        SSTableScanner scanner = reader.getDirectScanner(BufferedRandomAccessFile.DEFAULT_BUFFER_SIZE);

        outs.println("{");

        int i = 0;

        // collecting keys to export
        while (scanner.hasNext())
        {
            row = (SSTableIdentityIterator) scanner.next();

            String currentKey = bytesToHex(row.getKey().key);

            if (excludeSet.contains(currentKey))
                continue;
            else if (i != 0)
                outs.println(",");

            serializeRow(reader, row, row.getKey(), outs);

            i++;
        }

        outs.println("\n}");
        outs.flush();

        scanner.close();
    }
    
    /**
     * Export an SSTable and write the resulting JSON to a PrintStream.
     * 
     * @param ssTableFile the SSTable to export
     * @param outs PrintStream to write the output to
     * @param excludes keys to exclude from export
     *
     * @throws IOException on failure to read/write input/output
     */
    public static void export(String ssTableFile, PrintStream outs, String[] excludes) throws IOException
    {
        export(SSTableReader.open(Descriptor.fromFilename(ssTableFile)), outs, excludes);
    }

    /**
     * Export an SSTable and write the resulting JSON to standard out.
     * 
     * @param ssTableFile SSTable to export
     * @param excludes keys to exclude from export
     *
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
     *
     * @throws IOException on failure to open/read/write files or output streams
     * @throws ConfigurationException on configuration failure (wrong params given)
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
                export(ssTableFileName, System.out, Arrays.asList(keys), excludes);
            else
                export(ssTableFileName, excludes);
        }

        System.exit(0);
    }
}
