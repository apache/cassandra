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
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;

import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.format.SSTableWriter;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.composites.*;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.serializers.MarshalException;
import org.apache.cassandra.service.ActiveRepairService;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.JVMStabilityInspector;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.JsonToken;
import org.codehaus.jackson.map.MappingJsonFactory;
import org.codehaus.jackson.type.TypeReference;

/**
 * Create SSTables from JSON input
 */
public class SSTableImport
{
    private static final String KEYSPACE_OPTION = "K";
    private static final String COLUMN_FAMILY_OPTION = "c";
    private static final String KEY_COUNT_OPTION = "n";
    private static final String IS_SORTED_OPTION = "s";

    private static final Options options = new Options();
    private static CommandLine cmd;

    private Integer keyCountToImport;
    private final boolean isSorted;

    private static final JsonFactory factory = new MappingJsonFactory().configure(
            JsonParser.Feature.INTERN_FIELD_NAMES, false);

    static
    {
        Option optKeyspace = new Option(KEYSPACE_OPTION, true, "Keyspace name.");
        optKeyspace.setRequired(true);
        options.addOption(optKeyspace);

        Option optColfamily = new Option(COLUMN_FAMILY_OPTION, true, "Table name.");
        optColfamily.setRequired(true);
        options.addOption(optColfamily);

        options.addOption(new Option(KEY_COUNT_OPTION, true, "Number of keys to import (Optional)."));
        options.addOption(new Option(IS_SORTED_OPTION, false, "Assume JSON file as already sorted (e.g. created by sstable2json tool) (Optional)."));
    }

    private static class JsonColumn<T>
    {
        private ByteBuffer name;
        private ByteBuffer value;
        private long timestamp;

        private String kind;
        // Expiring columns
        private int ttl;
        private int localExpirationTime;

        // Counter columns
        private long timestampOfLastDelete;

        public JsonColumn(T json, CFMetaData meta)
        {
            if (json instanceof List)
            {
                CellNameType comparator = meta.comparator;
                List fields = (List<?>) json;

                assert fields.size() >= 3 : "Cell definition should have at least 3";

                name  = stringAsType((String) fields.get(0), comparator.asAbstractType());
                timestamp = (Long) fields.get(2);
                kind = "";

                if (fields.size() > 3)
                {
                    kind = (String) fields.get(3);
                    if (isExpiring())
                    {
                        ttl = (Integer) fields.get(4);
                        localExpirationTime = (Integer) fields.get(5);
                    }
                    else if (isCounter())
                    {
                        timestampOfLastDelete = (long) ((Integer) fields.get(4));
                    }
                    else if (isRangeTombstone())
                    {
                        localExpirationTime = (Integer) fields.get(4);
                    }
                }

                if (isDeleted())
                {
                    value = ByteBufferUtil.bytes((Integer) fields.get(1));
                }
                else if (isRangeTombstone())
                {
                    value = stringAsType((String) fields.get(1), comparator.asAbstractType());
                }
                else
                {
                    assert meta.isCQL3Table() || name.hasRemaining() : "Cell name should not be empty";
                    value = stringAsType((String) fields.get(1), 
                            meta.getValueValidator(name.hasRemaining() 
                                    ? comparator.cellFromByteBuffer(name)
                                    : meta.comparator.rowMarker(Composites.EMPTY)));
                }
            }
        }

        public boolean isDeleted()
        {
            return kind.equals("d");
        }

        public boolean isExpiring()
        {
            return kind.equals("e");
        }

        public boolean isCounter()
        {
            return kind.equals("c");
        }

        public boolean isRangeTombstone()
        {
            return kind.equals("t");
        }

        public ByteBuffer getName()
        {
            return name.duplicate();
        }

        public ByteBuffer getValue()
        {
            return value.duplicate();
        }
    }

    public SSTableImport()
    {
        this(null, false);
    }

    public SSTableImport(boolean isSorted)
    {
        this(null, isSorted);
    }

    public SSTableImport(Integer keyCountToImport, boolean isSorted)
    {
        this.keyCountToImport = keyCountToImport;
        this.isSorted = isSorted;
    }

    /**
     * Add columns to a column family.
     *
     * @param row the columns associated with a row
     * @param cfamily the column family to add columns to
     */
    private void addColumnsToCF(List<?> row, ColumnFamily cfamily)
    {
        CFMetaData cfm = cfamily.metadata();
        assert cfm != null;

        for (Object c : row)
        {
            JsonColumn col = new JsonColumn<List>((List) c, cfm);
            if (col.isRangeTombstone())
            {
                Composite start = cfm.comparator.fromByteBuffer(col.getName());
                Composite end = cfm.comparator.fromByteBuffer(col.getValue());
                cfamily.addAtom(new RangeTombstone(start, end, col.timestamp, col.localExpirationTime));
                continue;
            }
            
            assert cfm.isCQL3Table() || col.getName().hasRemaining() : "Cell name should not be empty";
            CellName cname = col.getName().hasRemaining() ? cfm.comparator.cellFromByteBuffer(col.getName()) 
                    : cfm.comparator.rowMarker(Composites.EMPTY);

            if (col.isExpiring())
            {
                cfamily.addColumn(new BufferExpiringCell(cname, col.getValue(), col.timestamp, col.ttl, col.localExpirationTime));
            }
            else if (col.isCounter())
            {
                cfamily.addColumn(new BufferCounterCell(cname, col.getValue(), col.timestamp, col.timestampOfLastDelete));
            }
            else if (col.isDeleted())
            {
                cfamily.addTombstone(cname, col.getValue(), col.timestamp);
            }
            else if (col.isRangeTombstone())
            {
                CellName end = cfm.comparator.cellFromByteBuffer(col.getValue());
                cfamily.addAtom(new RangeTombstone(cname, end, col.timestamp, col.localExpirationTime));
            }
            // cql3 row marker, see CASSANDRA-5852
            else if (cname.isEmpty())
            {
                cfamily.addColumn(cfm.comparator.rowMarker(Composites.EMPTY), col.getValue(), col.timestamp);
            }
            else
            {
                cfamily.addColumn(cname, col.getValue(), col.timestamp);
            }
        }
    }

    private void parseMeta(Map<?, ?> map, ColumnFamily cf, ByteBuffer superColumnName)
    {

        // deletionInfo is the only metadata we store for now
        if (map.containsKey("deletionInfo"))
        {
            Map<?, ?> unparsedDeletionInfo = (Map<?, ?>) map.get("deletionInfo");
            Number number = (Number) unparsedDeletionInfo.get("markedForDeleteAt");
            long markedForDeleteAt = number instanceof Long ? (Long) number : number.longValue();
            int localDeletionTime = (Integer) unparsedDeletionInfo.get("localDeletionTime");
            if (superColumnName == null)
                cf.setDeletionInfo(new DeletionInfo(markedForDeleteAt, localDeletionTime));
            else
                cf.addAtom(new RangeTombstone(SuperColumns.startOf(superColumnName), SuperColumns.endOf(superColumnName), markedForDeleteAt, localDeletionTime));
        }
    }

    /**
     * Convert a JSON formatted file to an SSTable.
     *
     * @param jsonFile the file containing JSON formatted data
     * @param keyspace keyspace the data belongs to
     * @param cf column family the data belongs to
     * @param ssTablePath file to write the SSTable to
     *
     * @throws IOException for errors reading/writing input/output
     */
    public int importJson(String jsonFile, String keyspace, String cf, String ssTablePath) throws IOException
    {
        ColumnFamily columnFamily = ArrayBackedSortedColumns.factory.create(keyspace, cf);
        IPartitioner partitioner = DatabaseDescriptor.getPartitioner();

        int importedKeys = (isSorted) ? importSorted(jsonFile, columnFamily, ssTablePath, partitioner)
                                      : importUnsorted(jsonFile, columnFamily, ssTablePath, partitioner);

        if (importedKeys != -1)
            System.out.printf("%d keys imported successfully.%n", importedKeys);

        return importedKeys;
    }

    private int importUnsorted(String jsonFile, ColumnFamily columnFamily, String ssTablePath, IPartitioner partitioner) throws IOException
    {
        int importedKeys = 0;
        long start = System.nanoTime();

        JsonParser parser = getParser(jsonFile);

        Object[] data = parser.readValueAs(new TypeReference<Object[]>(){});

        keyCountToImport = (keyCountToImport == null) ? data.length : keyCountToImport;
        SSTableWriter writer = SSTableWriter.create(Descriptor.fromFilename(ssTablePath), keyCountToImport, ActiveRepairService.UNREPAIRED_SSTABLE, 0);

        System.out.printf("Importing %s keys...%n", keyCountToImport);

        // sort by dk representation, but hold onto the hex version
        SortedMap<DecoratedKey,Map<?, ?>> decoratedKeys = new TreeMap<DecoratedKey,Map<?, ?>>();

        for (Object row : data)
        {
            Map<?,?> rowAsMap = (Map<?, ?>)row;
            decoratedKeys.put(partitioner.decorateKey(getKeyValidator(columnFamily).fromString((String) rowAsMap.get("key"))), rowAsMap);
        }

        for (Map.Entry<DecoratedKey, Map<?, ?>> row : decoratedKeys.entrySet())
        {
            if (row.getValue().containsKey("metadata"))
            {
                parseMeta((Map<?, ?>) row.getValue().get("metadata"), columnFamily, null);
            }

            Object columns = row.getValue().get("cells");
            addColumnsToCF((List<?>) columns, columnFamily);


            writer.append(row.getKey(), columnFamily);
            columnFamily.clear();

            importedKeys++;

            long current = System.nanoTime();

            if (TimeUnit.NANOSECONDS.toSeconds(current - start) >= 5) // 5 secs.
            {
                System.out.printf("Currently imported %d keys.%n", importedKeys);
                start = current;
            }

            if (keyCountToImport == importedKeys)
                break;
        }

        writer.closeAndOpenReader();

        return importedKeys;
    }

    private int importSorted(String jsonFile, ColumnFamily columnFamily, String ssTablePath,
            IPartitioner partitioner) throws IOException
    {
        int importedKeys = 0; // already imported keys count
        long start = System.nanoTime();

        JsonParser parser = getParser(jsonFile);

        if (keyCountToImport == null)
        {
            keyCountToImport = 0;
            System.out.println("Counting keys to import, please wait... (NOTE: to skip this use -n <num_keys>)");

            parser.nextToken(); // START_ARRAY
            while (parser.nextToken() != null)
            {
                parser.skipChildren();
                if (parser.getCurrentToken() == JsonToken.END_ARRAY)
                    break;

                keyCountToImport++;
            }
        }

        System.out.printf("Importing %s keys...%n", keyCountToImport);

        parser = getParser(jsonFile); // renewing parser
        SSTableWriter writer = SSTableWriter.create(Descriptor.fromFilename(ssTablePath), keyCountToImport, ActiveRepairService.UNREPAIRED_SSTABLE);

        int lineNumber = 1;
        DecoratedKey prevStoredKey = null;

        parser.nextToken(); // START_ARRAY
        while (parser.nextToken() != null)
        {
            String key = parser.getCurrentName();
            Map<?, ?> row = parser.readValueAs(new TypeReference<Map<?, ?>>(){});
            DecoratedKey currentKey = partitioner.decorateKey(getKeyValidator(columnFamily).fromString((String) row.get("key")));

            if (row.containsKey("metadata"))
                parseMeta((Map<?, ?>) row.get("metadata"), columnFamily, null);

            addColumnsToCF((List<?>) row.get("cells"), columnFamily);

            if (prevStoredKey != null && prevStoredKey.compareTo(currentKey) != -1)
            {
                System.err
                        .printf("Line %d: Key %s is greater than previous, collection is not sorted properly. Aborting import. You might need to delete SSTables manually.%n",
                                lineNumber, key);
                return -1;
            }

            // saving decorated key
            writer.append(currentKey, columnFamily);
            columnFamily.clear();

            prevStoredKey = currentKey;
            importedKeys++;
            lineNumber++;

            long current = System.nanoTime();

            if (TimeUnit.NANOSECONDS.toSeconds(current - start) >= 5) // 5 secs.
            {
                System.out.printf("Currently imported %d keys.%n", importedKeys);
                start = current;
            }

            if (keyCountToImport == importedKeys)
                break;

        }

        writer.closeAndOpenReader();

        return importedKeys;
    }

    /**
     * Get key validator for column family
     * @param columnFamily column family instance
     * @return key validator for given column family
     */
    private AbstractType<?> getKeyValidator(ColumnFamily columnFamily) {
        // this is a fix to support backward compatibility
        // which allows to skip the current key validator
        // please, take a look onto CASSANDRA-7498 for more details
        if ("true".equals(System.getProperty("skip.key.validator", "false"))) {
            return BytesType.instance;
        }
        return columnFamily.metadata().getKeyValidator();
    }

    /**
     * Get JsonParser object for file
     * @param fileName name of the file
     * @return json parser instance for given file
     * @throws IOException if any I/O error.
     */
    private JsonParser getParser(String fileName) throws IOException
    {
        return factory.createJsonParser(new File(fileName));
    }

    /**
     * Converts JSON to an SSTable file. JSON input can either be a file specified
     * using an optional command line argument, or supplied on standard in.
     *
     * @param args command line arguments
     * @throws ParseException on failure to parse JSON input
     * @throws ConfigurationException on configuration error.
     */
    public static void main(String[] args) throws ParseException, ConfigurationException
    {
        CommandLineParser parser = new PosixParser();

        try
        {
            cmd = parser.parse(options, args);
        }
        catch (org.apache.commons.cli.ParseException e)
        {
            System.err.println(e.getMessage());
            printProgramUsage();
            System.exit(1);
        }

        if (cmd.getArgs().length != 2)
        {
            printProgramUsage();
            System.exit(1);
        }

        String json     = cmd.getArgs()[0];
        String ssTable  = cmd.getArgs()[1];
        String keyspace = cmd.getOptionValue(KEYSPACE_OPTION);
        String cfamily  = cmd.getOptionValue(COLUMN_FAMILY_OPTION);

        Integer keyCountToImport = null;
        boolean isSorted = false;

        if (cmd.hasOption(KEY_COUNT_OPTION))
        {
            keyCountToImport = Integer.valueOf(cmd.getOptionValue(KEY_COUNT_OPTION));
        }

        if (cmd.hasOption(IS_SORTED_OPTION))
        {
            isSorted = true;
        }

        Schema.instance.loadFromDisk(false);
        if (Schema.instance.getNonSystemKeyspaces().size() < 1)
        {
            String msg = "no non-system keyspaces are defined";
            System.err.println(msg);
            throw new ConfigurationException(msg);
        }

        try
        {
           new SSTableImport(keyCountToImport, isSorted).importJson(json, keyspace, cfamily, ssTable);
        }
        catch (Exception e)
        {
            JVMStabilityInspector.inspectThrowable(e);
            e.printStackTrace();
            System.err.println("ERROR: " + e.getMessage());
            System.exit(-1);
        }

        System.exit(0);
    }

    private static void printProgramUsage()
    {
        System.out.printf("Usage: %s -s -K <keyspace> -c <column_family> -n <num_keys> <json> <sstable>%n%n",
                            SSTableImport.class.getName());

        System.out.println("Options:");
        for (Object o :  options.getOptions())
        {
            Option opt = (Option) o;
            System.out.println("  -" +opt.getOpt() + " - " + opt.getDescription());
        }
    }

    /**
     * Convert a string to bytes (ByteBuffer) according to type
     * @param content string to convert
     * @param type type to use for conversion
     * @return byte buffer representation of the given string
     */
    private static ByteBuffer stringAsType(String content, AbstractType<?> type)
    {
        try
        {
            return type.fromString(content);
        }
        catch (MarshalException e)
        {
            throw new RuntimeException(e.getMessage());
        }
    }

}
