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
package org.apache.cassandra.tools;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.util.*;

import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.commons.cli.*;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.composites.CellNameType;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.io.sstable.*;
import org.apache.cassandra.io.util.RandomAccessReader;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.map.ObjectMapper;

/**
 * Export SSTables to JSON format.
 */
public class SSTableExport
{
    private static final ObjectMapper jsonMapper = new ObjectMapper();

    private static final String KEY_OPTION = "k";
    private static final String EXCLUDEKEY_OPTION = "x";
    private static final String ENUMERATEKEYS_OPTION = "e";

    private static final Options options = new Options();
    private static CommandLine cmd;

    static
    {
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
     * Checks if PrintStream error and throw exception
     *
     * @param out The PrintStream to be check
     */
    private static void checkStream(PrintStream out) throws IOException
    {
        if (out.checkError())
            throw new IOException("Error writing output stream");
    }

    /**
     * JSON Hash Key serializer
     *
     * @param out   The output steam to write data
     * @param value value to set as a key
     */
    private static void writeKey(PrintStream out, String value)
    {
        writeJSON(out, value);
        out.print(": ");
    }

    private static List<Object> serializeAtom(OnDiskAtom atom, CFMetaData cfMetaData)
    {
        if (atom instanceof Cell)
        {
            return serializeColumn((Cell) atom, cfMetaData);
        }
        else
        {
            assert atom instanceof RangeTombstone;
            RangeTombstone rt = (RangeTombstone) atom;
            ArrayList<Object> serializedColumn = new ArrayList<Object>();
            serializedColumn.add(cfMetaData.comparator.getString(rt.min));
            serializedColumn.add(cfMetaData.comparator.getString(rt.max));
            serializedColumn.add(rt.data.markedForDeleteAt);
            serializedColumn.add("t");
            serializedColumn.add(rt.data.localDeletionTime);
            return serializedColumn;
        }
    }

    /**
     * Serialize a given cell to a List of Objects that jsonMapper knows how to turn into strings.  Type is
     *
     * human_readable_name, value, timestamp, [flag, [options]]
     *
     * Value is normally the human readable value as rendered by the validator, but for deleted cells we
     * give the local deletion time instead.
     *
     * Flag may be exactly one of {d,e,c} for deleted, expiring, or counter:
     *  - No options for deleted cells
     *  - If expiring, options will include the TTL and local deletion time.
     *  - If counter, options will include timestamp of last delete
     *
     * @param cell     cell presentation
     * @param cfMetaData Column Family metadata (to get validator)
     * @return cell as serialized list
     */
    private static List<Object> serializeColumn(Cell cell, CFMetaData cfMetaData)
    {
        CellNameType comparator = cfMetaData.comparator;
        ArrayList<Object> serializedColumn = new ArrayList<Object>();

        serializedColumn.add(comparator.getString(cell.name()));

        if (cell instanceof DeletedCell)
        {
            serializedColumn.add(cell.getLocalDeletionTime());
        }
        else
        {
            AbstractType<?> validator = cfMetaData.getValueValidator(cell.name());
            serializedColumn.add(validator.getString(cell.value()));
        }

        serializedColumn.add(cell.timestamp());

        if (cell instanceof DeletedCell)
        {
            serializedColumn.add("d");
        }
        else if (cell instanceof ExpiringCell)
        {
            serializedColumn.add("e");
            serializedColumn.add(((ExpiringCell) cell).getTimeToLive());
            serializedColumn.add(cell.getLocalDeletionTime());
        }
        else if (cell instanceof CounterCell)
        {
            serializedColumn.add("c");
            serializedColumn.add(((CounterCell) cell).timestampOfLastDelete());
        }

        return serializedColumn;
    }

    /**
     * Get portion of the columns and serialize in loop while not more columns left in the row
     *
     * @param row SSTableIdentityIterator row representation with Column Family
     * @param key Decorated Key for the required row
     * @param out output stream
     */
    private static void serializeRow(SSTableIdentityIterator row, DecoratedKey key, PrintStream out)
    {
        serializeRow(row.getColumnFamily().deletionInfo(), row, row.getColumnFamily().metadata(), key, out);
    }

    private static void serializeRow(DeletionInfo deletionInfo, Iterator<OnDiskAtom> atoms, CFMetaData metadata, DecoratedKey key, PrintStream out)
    {
        out.print("{");
        writeKey(out, "key");
        writeJSON(out, metadata.getKeyValidator().getString(key.getKey()));
        out.print(",\n");

        if (!deletionInfo.isLive())
        {
            out.print(" ");
            writeKey(out, "metadata");
            out.print("{");
            writeKey(out, "deletionInfo");
            writeJSON(out, deletionInfo.getTopLevelDeletion());
            out.print("}");
            out.print(",\n");
        }

        out.print(" ");
        writeKey(out, "cells");
        out.print("[");
        while (atoms.hasNext())
        {
            writeJSON(out, serializeAtom(atoms.next(), metadata));

            if (atoms.hasNext())
                out.print(",\n           ");
        }
        out.print("]");

        out.print("}");
    }

    /**
     * Enumerate row keys from an SSTableReader and write the result to a PrintStream.
     *
     * @param desc the descriptor of the file to export the rows from
     * @param outs PrintStream to write the output to
     * @param metadata Metadata to print keys in a proper format
     * @throws IOException on failure to read/write input/output
     */
    public static void enumeratekeys(Descriptor desc, PrintStream outs, CFMetaData metadata)
    throws IOException
    {
        KeyIterator iter = new KeyIterator(desc);
        try
        {
            DecoratedKey lastKey = null;
            while (iter.hasNext())
            {
                DecoratedKey key = iter.next();

                // validate order of the keys in the sstable
                if (lastKey != null && lastKey.compareTo(key) > 0)
                    throw new IOException("Key out of order! " + lastKey + " > " + key);
                lastKey = key;

                outs.println(metadata.getKeyValidator().getString(key.getKey()));
                checkStream(outs); // flushes
            }
        }
        finally
        {
            iter.close();
        }
    }

    /**
     * Export specific rows from an SSTable and write the resulting JSON to a PrintStream.
     *
     * @param desc     the descriptor of the sstable to read from
     * @param outs     PrintStream to write the output to
     * @param toExport the keys corresponding to the rows to export
     * @param excludes keys to exclude from export
     * @param metadata Metadata to print keys in a proper format
     * @throws IOException on failure to read/write input/output
     */
    public static void export(Descriptor desc, PrintStream outs, Collection<String> toExport, String[] excludes, CFMetaData metadata) throws IOException
    {
        SSTableReader sstable = SSTableReader.open(desc);
        RandomAccessReader dfile = sstable.openDataReader();
        try
        {
            IPartitioner partitioner = sstable.partitioner;

            if (excludes != null)
                toExport.removeAll(Arrays.asList(excludes));

            outs.println("[");

            int i = 0;

            // last key to compare order
            DecoratedKey lastKey = null;

            for (String key : toExport)
            {
                DecoratedKey decoratedKey = partitioner.decorateKey(metadata.getKeyValidator().fromString(key));

                if (lastKey != null && lastKey.compareTo(decoratedKey) > 0)
                    throw new IOException("Key out of order! " + lastKey + " > " + decoratedKey);

                lastKey = decoratedKey;

                RowIndexEntry entry = sstable.getPosition(decoratedKey, SSTableReader.Operator.EQ);
                if (entry == null)
                    continue;

                dfile.seek(entry.position);
                ByteBufferUtil.readWithShortLength(dfile); // row key
                DeletionInfo deletionInfo = new DeletionInfo(DeletionTime.serializer.deserialize(dfile));

                Iterator<OnDiskAtom> atomIterator = sstable.metadata.getOnDiskIterator(dfile, sstable.descriptor.version);
                checkStream(outs);

                if (i != 0)
                    outs.println(",");
                i++;
                serializeRow(deletionInfo, atomIterator, sstable.metadata, decoratedKey, outs);
            }

            outs.println("\n]");
            outs.flush();
        }
        finally
        {
            dfile.close();
        }
    }

    // This is necessary to accommodate the test suite since you cannot open a Reader more
    // than once from within the same process.
    static void export(SSTableReader reader, PrintStream outs, String[] excludes) throws IOException
    {
        Set<String> excludeSet = new HashSet<String>();

        if (excludes != null)
            excludeSet = new HashSet<>(Arrays.asList(excludes));

        SSTableIdentityIterator row;
        ISSTableScanner scanner = reader.getScanner();
        try
        {
            outs.println("[");

            int i = 0;

            // collecting keys to export
            while (scanner.hasNext())
            {
                row = (SSTableIdentityIterator) scanner.next();

                String currentKey = row.getColumnFamily().metadata().getKeyValidator().getString(row.getKey().getKey());

                if (excludeSet.contains(currentKey))
                    continue;
                else if (i != 0)
                    outs.println(",");

                serializeRow(row, row.getKey(), outs);
                checkStream(outs);

                i++;
            }

            outs.println("\n]");
            outs.flush();
        }
        finally
        {
            scanner.close();
        }
    }

    /**
     * Export an SSTable and write the resulting JSON to a PrintStream.
     *
     * @param desc     the descriptor of the sstable to read from
     * @param outs     PrintStream to write the output to
     * @param excludes keys to exclude from export
     * @throws IOException on failure to read/write input/output
     */
    public static void export(Descriptor desc, PrintStream outs, String[] excludes) throws IOException
    {
        export(SSTableReader.open(desc), outs, excludes);
    }

    /**
     * Export an SSTable and write the resulting JSON to standard out.
     *
     * @param desc     the descriptor of the sstable to read from
     * @param excludes keys to exclude from export
     * @throws IOException on failure to read/write SSTable/standard out
     */
    public static void export(Descriptor desc, String[] excludes) throws IOException
    {
        export(desc, System.out, excludes);
    }

    /**
     * Given arguments specifying an SSTable, and optionally an output file,
     * export the contents of the SSTable to JSON.
     *
     * @param args command lines arguments
     * @throws ConfigurationException on configuration failure (wrong params given)
     */
    public static void main(String[] args) throws ConfigurationException
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

        Schema.instance.loadFromDisk(false);
        Descriptor descriptor = Descriptor.fromFilename(ssTableFileName);

        // Start by validating keyspace name
        if (Schema.instance.getKSMetaData(descriptor.ksname) == null)
        {
            System.err.println(String.format("Filename %s references to nonexistent keyspace: %s!",
                                             ssTableFileName, descriptor.ksname));
            System.exit(1);
        }
        Keyspace keyspace = Keyspace.open(descriptor.ksname);

        // Make it works for indexes too - find parent cf if necessary
        String baseName = descriptor.cfname;
        if (descriptor.cfname.contains("."))
        {
            String[] parts = descriptor.cfname.split("\\.", 2);
            baseName = parts[0];
        }

        // IllegalArgumentException will be thrown here if ks/cf pair does not exist
        ColumnFamilyStore cfStore = null;
        try
        {
            cfStore = keyspace.getColumnFamilyStore(baseName);
        }
        catch (IllegalArgumentException e)
        {
            System.err.println(String.format("The provided table is not part of this cassandra keyspace: keyspace = %s, table = %s",
                                             descriptor.ksname, descriptor.cfname));
            System.exit(1);
        }

        try
        {
            if (cmd.hasOption(ENUMERATEKEYS_OPTION))
            {
                enumeratekeys(descriptor, System.out, cfStore.metadata);
            }
            else
            {
                if ((keys != null) && (keys.length > 0))
                    export(descriptor, System.out, Arrays.asList(keys), excludes, cfStore.metadata);
                else
                    export(descriptor, excludes);
            }
        }
        catch (IOException e)
        {
            // throwing exception outside main with broken pipe causes windows cmd to hang
            e.printStackTrace(System.err);
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
