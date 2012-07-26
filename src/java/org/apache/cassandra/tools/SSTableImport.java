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

import static org.apache.cassandra.utils.ByteBufferUtil.hexToBytes;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ConfigurationException;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.db.AbstractColumnContainer;
import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.ColumnFamilyType;
import org.apache.cassandra.db.CounterColumn;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletionInfo;
import org.apache.cassandra.db.ExpiringColumn;
import org.apache.cassandra.db.RangeTombstone;
import org.apache.cassandra.db.SuperColumn;
import org.apache.cassandra.db.filter.QueryPath;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.db.marshal.MarshalException;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.io.sstable.SSTableWriter;
import org.apache.cassandra.utils.ByteBufferUtil;
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

        Option optColfamily = new Option(COLUMN_FAMILY_OPTION, true, "Column Family name.");
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

        public JsonColumn(T json, CFMetaData meta, boolean isSubColumn)
        {
            if (json instanceof List)
            {
                AbstractType<?> comparator = (isSubColumn) ? meta.subcolumnComparator : meta.comparator;
                List fields = (List<?>) json;

                assert fields.size() >= 3 : "Column definition should have at least 3";

                name  = stringAsType((String) fields.get(0), comparator);
                timestamp = (Long) fields.get(2);
                kind = "";

                if (fields.size() > 3)
                {
                    if (fields.get(3) instanceof Boolean)
                    {
                        // old format, reading this for backward compatibility sake
                        if (fields.size() == 6)
                        {
                            kind = "e";
                            ttl = (Integer) fields.get(4);
                            localExpirationTime = (Integer) fields.get(5);
                        }
                        else
                        {
                            kind = ((Boolean) fields.get(3)) ? "d" : "";
                        }
                    }
                    else
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
                }

                value = isDeleted() ? ByteBufferUtil.hexToBytes((String) fields.get(1))
                                    : stringAsType((String) fields.get(1), meta.getValueValidator(name.duplicate()));
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

    private void addToStandardCF(List<?> row, ColumnFamily cfamily)
    {
        addColumnsToCF(row, null, cfamily);
    }

    /**
     * Add columns to a column family.
     *
     * @param row the columns associated with a row
     * @param superName name of the super column if any
     * @param cfamily the column family to add columns to
     */
    private void addColumnsToCF(List<?> row, ByteBuffer superName, ColumnFamily cfamily)
    {
        CFMetaData cfm = cfamily.metadata();
        assert cfm != null;

        for (Object c : row)
        {
            JsonColumn col = new JsonColumn<List>((List) c, cfm, (superName != null));
            QueryPath path = new QueryPath(cfm.cfName, superName, col.getName());

            if (col.isExpiring())
            {
                cfamily.addColumn(null, new ExpiringColumn(col.getName(), col.getValue(), col.timestamp, col.ttl, col.localExpirationTime));
            }
            else if (col.isCounter())
            {
                cfamily.addColumn(null, new CounterColumn(col.getName(), col.getValue(), col.timestamp, col.timestampOfLastDelete));
            }
            else if (col.isDeleted())
            {
                cfamily.addTombstone(path, col.getValue(), col.timestamp);
            }
            else if (col.isRangeTombstone())
            {
                cfamily.addAtom(new RangeTombstone(col.getName(), col.getValue(), col.timestamp, col.localExpirationTime));
            }
            else
            {
                cfamily.addColumn(path, col.getValue(), col.timestamp);
            }
        }
    }

    private void parseMeta(Map<?, ?> map, AbstractColumnContainer columnContainer)
    {

        // deletionInfo is the only metadata we store for now
        if (map.containsKey("deletionInfo"))
        {
            Map<?, ?> unparsedDeletionInfo = (Map<?, ?>) map.get("deletionInfo");
            Number number = (Number) unparsedDeletionInfo.get("markedForDeleteAt");
            long markedForDeleteAt = number instanceof Long ? (Long) number : ((Integer) number).longValue();
            int localDeletionTime = (Integer) unparsedDeletionInfo.get("localDeletionTime");
            columnContainer.setDeletionInfo(new DeletionInfo(markedForDeleteAt, localDeletionTime));
        }
    }

    /**
     * Add super columns to a column family.
     *
     * @param row the super columns associated with a row
     * @param cfamily the column family to add columns to
     */
    private void addToSuperCF(Map<?, ?> row, ColumnFamily cfamily)
    {
        CFMetaData metaData = cfamily.metadata();
        assert metaData != null;

        AbstractType<?> comparator = metaData.comparator;

        // Super columns
        for (Map.Entry<?, ?> entry : row.entrySet())
        {
            Map<?, ?> data = (Map<?, ?>) entry.getValue();

            ByteBuffer superName = stringAsType((String) entry.getKey(), comparator);

            addColumnsToCF((List<?>) data.get("subColumns"), superName, cfamily);

            if (data.containsKey("metadata"))
            {
                parseMeta((Map<?, ?>) data.get("metadata"), (SuperColumn) cfamily.getColumn(superName));
            }
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
        ColumnFamily columnFamily = ColumnFamily.create(keyspace, cf);
        IPartitioner<?> partitioner = DatabaseDescriptor.getPartitioner();

        int importedKeys = (isSorted) ? importSorted(jsonFile, columnFamily, ssTablePath, partitioner)
                                      : importUnsorted(jsonFile, columnFamily, ssTablePath, partitioner);

        if (importedKeys != -1)
            System.out.printf("%d keys imported successfully.%n", importedKeys);

        return importedKeys;
    }

    private int importUnsorted(String jsonFile, ColumnFamily columnFamily, String ssTablePath, IPartitioner<?> partitioner) throws IOException
    {
        int importedKeys = 0;
        long start = System.currentTimeMillis();

        JsonParser parser = getParser(jsonFile);

        Object[] data = parser.readValueAs(new TypeReference<Object[]>(){});

        keyCountToImport = (keyCountToImport == null) ? data.length : keyCountToImport;
        SSTableWriter writer = new SSTableWriter(ssTablePath, keyCountToImport);

        System.out.printf("Importing %s keys...%n", keyCountToImport);

        // sort by dk representation, but hold onto the hex version
        SortedMap<DecoratedKey,Map<?, ?>> decoratedKeys = new TreeMap<DecoratedKey,Map<?, ?>>();

        for (Object row : data)
        {
            Map<?,?> rowAsMap = (Map<?, ?>)row;
            decoratedKeys.put(partitioner.decorateKey(hexToBytes((String)rowAsMap.get("key"))), rowAsMap);
        }

        for (Map.Entry<DecoratedKey, Map<?, ?>> row : decoratedKeys.entrySet())
        {
            if (row.getValue().containsKey("metadata"))
            {
                parseMeta((Map<?, ?>) row.getValue().get("metadata"), columnFamily);
            }

            Object columns = row.getValue().get("columns");
            if (columnFamily.getType() == ColumnFamilyType.Super)
                addToSuperCF((Map<?, ?>) columns, columnFamily);
            else
                addToStandardCF((List<?>) columns, columnFamily);


            writer.append(row.getKey(), columnFamily);
            columnFamily.clear();

            // ready the column family for the next row since we might have read deletionInfo metadata
            columnFamily.delete(DeletionInfo.LIVE);

            importedKeys++;

            long current = System.currentTimeMillis();

            if (current - start >= 5000) // 5 secs.
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
            IPartitioner<?> partitioner) throws IOException
    {
        int importedKeys = 0; // already imported keys count
        long start = System.currentTimeMillis();

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
        SSTableWriter writer = new SSTableWriter(ssTablePath, keyCountToImport);

        int lineNumber = 1;
        DecoratedKey prevStoredKey = null;

        parser.nextToken(); // START_ARRAY
        while (parser.nextToken() != null)
        {
            String key = parser.getCurrentName();
            Map<?, ?> row = parser.readValueAs(new TypeReference<Map<?, ?>>(){});
            DecoratedKey currentKey = partitioner.decorateKey(hexToBytes((String) row.get("key")));

            if (row.containsKey("metadata"))
                parseMeta((Map<?, ?>) row.get("metadata"), columnFamily);


            if (columnFamily.getType() == ColumnFamilyType.Super)
                addToSuperCF((Map<?, ?>)row.get("columns"), columnFamily);
            else
                addToStandardCF((List<?>)row.get("columns"), columnFamily);

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

            long current = System.currentTimeMillis();

            if (current - start >= 5000) // 5 secs.
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
     * @throws IOException on failure to open/read/write files or output streams
     * @throws ParseException on failure to parse JSON input
     * @throws ConfigurationException on configuration error.
     */
    public static void main(String[] args) throws IOException, ParseException, ConfigurationException
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

        DatabaseDescriptor.loadSchemas();
        if (Schema.instance.getNonSystemTables().size() < 1)
        {
            String msg = "no non-system tables are defined";
            System.err.println(msg);
            throw new ConfigurationException(msg);
        }

        try
        {
           new SSTableImport(keyCountToImport,isSorted).importJson(json, keyspace, cfamily, ssTable);
        }
        catch (Exception e)
        {
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
            return (type == BytesType.instance) ? hexToBytes(content) : type.fromString(content);
        }
        catch (MarshalException e)
        {
            throw new RuntimeException(e.getMessage());
        }
    }

}
