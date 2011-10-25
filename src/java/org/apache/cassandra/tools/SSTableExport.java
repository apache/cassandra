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

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.index.keys.KeysIndex;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.service.StorageService;

import org.apache.commons.cli.*;

import org.apache.cassandra.config.ConfigurationException;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.io.sstable.*;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.map.ObjectMapper;

import static org.apache.cassandra.utils.ByteBufferUtil.bytesToHex;
import static org.apache.cassandra.utils.ByteBufferUtil.hexToBytes;

/**
 * Export SSTables to JSON format.
 */
public class SSTableExport
{
    private static ObjectMapper jsonMapper = new ObjectMapper();

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

        // disabling auto close of the stream
        jsonMapper.configure(JsonGenerator.Feature.AUTO_CLOSE_TARGET, false);
    }

    /**
     * JSON Hash Key serializer
     *
     * @param out The output steam to write data
     * @param value value to set as a key
     */
    private static void writeKey(PrintStream out, String value)
    {
        writeJSON(out, value);
        out.print(": ");
    }

    /**
     * Serialize columns using given column iterator
     *
     * @param columns column iterator
     * @param out output stream
     * @param comparator columns comparator
     * @param cfMetaData Column Family metadata (to get validator)
     */
    private static void serializeColumns(Iterator<IColumn> columns, PrintStream out, AbstractType comparator, CFMetaData cfMetaData)
    {
        while (columns.hasNext())
        {
            writeJSON(out, serializeColumn(columns.next(), comparator, cfMetaData));

            if (columns.hasNext())
                out.print(", ");
        }
    }

    /**
     * Serialize a given column to the JSON format
     *
     * @param column column presentation
     * @param comparator columns comparator
     * @param cfMetaData Column Family metadata (to get validator)
     *
     * @return column as serialized list
     */
    private static List<Object> serializeColumn(IColumn column, AbstractType comparator, CFMetaData cfMetaData)
    {
        ArrayList<Object> serializedColumn = new ArrayList<Object>();

        ByteBuffer name = ByteBufferUtil.clone(column.name());
        ByteBuffer value = ByteBufferUtil.clone(column.value());

        serializedColumn.add(comparator.getString(name));
        if (column instanceof DeletedColumn)
        {
            serializedColumn.add(ByteBufferUtil.bytesToHex(value));
        }
        else
        {
            AbstractType validator = cfMetaData.getValueValidator(name);
            serializedColumn.add(validator.getString(value));
        }
        serializedColumn.add(column.timestamp());

        if (column instanceof DeletedColumn)
        {
            serializedColumn.add("d");
        }
        else if (column instanceof ExpiringColumn)
        {
            serializedColumn.add("e");
            serializedColumn.add(((ExpiringColumn) column).getTimeToLive());
            serializedColumn.add(column.getLocalDeletionTime());
        }
        else if (column instanceof CounterColumn)
        {
            serializedColumn.add("c");
            serializedColumn.add(((CounterColumn) column).timestampOfLastDelete());
        }

        return serializedColumn;
    }

    /**
     * Get portion of the columns and serialize in loop while not more columns left in the row
     * @param row SSTableIdentityIterator row representation with Column Family
     * @param key Decorated Key for the required row
     * @param out output stream
     */
    private static void serializeRow(SSTableIdentityIterator row, DecoratedKey key, PrintStream out)
    {
        ColumnFamily columnFamily = row.getColumnFamily();
        boolean isSuperCF = columnFamily.isSuper();
        CFMetaData cfMetaData = columnFamily.metadata();
        AbstractType comparator = columnFamily.getComparator();

        writeKey(out, bytesToHex(key.key));
        out.print(isSuperCF ? "{" : "[");

        if (isSuperCF)
        {
            while (row.hasNext())
            {
                IColumn column = row.next();

                writeKey(out, comparator.getString(column.name()));
                out.print("{");
                writeKey(out, "deletedAt");
                out.print(column.getMarkedForDeleteAt());
                out.print(", ");
                writeKey(out, "subColumns");
                out.print("[");
                serializeColumns(column.getSubColumns().iterator(), out, columnFamily.getSubComparator(), cfMetaData);
                out.print("]");
                out.print("}");

                if (row.hasNext())
                    out.print(", ");
            }
        }
        else
        {
            serializeColumns(row, out, comparator, cfMetaData);
        }

        out.print(isSuperCF ? "}" : "]");

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
        SSTableScanner scanner = reader.getDirectScanner();

        IPartitioner<?> partitioner = StorageService.getPartitioner();

        if (excludes != null)
            toExport.removeAll(Arrays.asList(excludes));

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

            SSTableIdentityIterator row = (SSTableIdentityIterator) scanner.next();
            if (!row.getKey().equals(decoratedKey))
                continue;

            serializeRow(row, decoratedKey, outs);

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
        SSTableScanner scanner = reader.getDirectScanner();

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

            serializeRow(row, row.getKey(), outs);

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
        Descriptor descriptor = Descriptor.fromFilename(ssTableFile);
        CFMetaData metadata;
        if (descriptor.cfname.contains("."))
        {
            // look up index metadata from parent
            int i = descriptor.cfname.indexOf(".");
            String parentName = descriptor.cfname.substring(0, i);
            CFMetaData parent = Schema.instance.getCFMetaData(descriptor.ksname, parentName);
            ColumnDefinition def = parent.getColumnDefinitionForIndex(descriptor.cfname.substring(i + 1));
            metadata = CFMetaData.newIndexMetadata(parent, def, KeysIndex.indexComparator());
        }
        else
        {
            metadata = Schema.instance.getCFMetaData(descriptor.ksname, descriptor.cfname);
        }

        export(SSTableReader.open(descriptor, metadata), outs, excludes);
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
        if (Schema.instance.getNonSystemTables().size() < 1)
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

    private static void writeJSON(PrintStream out, Object value)
    {
        try
        {
            jsonMapper.writeValue(out, value);
        }
        catch (Exception e)
        {
            throw new RuntimeException(e.getMessage(), e);
        }
    }
}
