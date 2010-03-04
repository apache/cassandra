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

import java.io.FileReader;
import java.io.IOException;
import java.util.*;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.SuperColumn;
import org.apache.cassandra.db.filter.QueryPath;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.io.sstable.SSTableWriter;
import static org.apache.cassandra.utils.FBUtilities.hexToBytes;
import org.apache.commons.cli.*;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;
import org.json.simple.parser.ParseException;

/**
 * Create SSTables from JSON input
 */
public class SSTableImport
{
    private static final String KEYSPACE_OPTION = "K";
    private static final String COLFAM_OPTION = "c";
    private static Options options;
    private static CommandLine cmd;

    static
    {
        options = new Options();
        Option optKeyspace = new Option(KEYSPACE_OPTION, true, "Keyspace name");
        optKeyspace.setRequired(true);
        options.addOption(optKeyspace);
        Option optColfamily = new Option(COLFAM_OPTION, true, "Column family");
        optColfamily.setRequired(true);
        options.addOption(optColfamily);
    }
    
    private static class JsonColumn
    {
        private String name;
        private String value;
        private long timestamp;
        private boolean isDeleted;
        
        private JsonColumn(Object obj) throws ClassCastException
        {
            JSONArray colSpec = (JSONArray)obj;
            assert colSpec.size() == 4;
            name = (String)colSpec.get(0);
            value = (String)colSpec.get(1);
            timestamp = (Long)colSpec.get(2);
            isDeleted = (Boolean)colSpec.get(3);
        }
    }

    /**
     * Add columns to a column family.
     * 
     * @param row the columns associated with a row
     * @param cfamily the column family to add columns to
     */
    private static void addToStandardCF(JSONArray row, ColumnFamily cfamily)
    {
        for (Object c : row)
        {
            JsonColumn col = new JsonColumn(c);
            QueryPath path = new QueryPath(cfamily.name(), null, hexToBytes(col.name));
            cfamily.addColumn(path, hexToBytes(col.value), col.timestamp, col.isDeleted);
        }
    }
    
    /**
     * Add super columns to a column family.
     * 
     * @param row the super columns associated with a row
     * @param cfamily the column family to add columns to
     */
    private static void addToSuperCF(JSONObject row, ColumnFamily cfamily)
    {
        // Super columns
        for (Map.Entry<String, JSONObject> entry : (Set<Map.Entry<String, JSONObject>>)row.entrySet())
        {
            byte[] superName = hexToBytes(entry.getKey());
            long deletedAt = (Long)entry.getValue().get("deletedAt");
            JSONArray subColumns = (JSONArray)entry.getValue().get("subColumns");
            
            // Add sub-columns
            for (Object c : subColumns)
            {
                JsonColumn col = new JsonColumn(c);
                QueryPath path = new QueryPath(cfamily.name(), superName, hexToBytes(col.name));
                cfamily.addColumn(path, hexToBytes(col.value), col.timestamp, col.isDeleted);
            }
            
            SuperColumn superColumn = (SuperColumn)cfamily.getColumn(superName);
            superColumn.markForDeleteAt((int)(System.currentTimeMillis()/1000), deletedAt);
        }
    }

    /**
     * Convert a JSON formatted file to an SSTable.
     * 
     * @param jsonFile the file containing JSON formatted data
     * @param keyspace keyspace the data belongs to
     * @param cf column family the data belongs to
     * @param ssTablePath file to write the SSTable to
     * @throws IOException for errors reading/writing input/output
     * @throws ParseException for errors encountered parsing JSON input
     */
    public static void importJson(String jsonFile, String keyspace, String cf, String ssTablePath)
    throws IOException, ParseException
    {
        ColumnFamily cfamily = ColumnFamily.create(keyspace, cf);
        String cfType = cfamily.type();    // Super or Standard
        IPartitioner<?> partitioner = DatabaseDescriptor.getPartitioner();
        DataOutputBuffer dob = new DataOutputBuffer();
        
        try
        {
            JSONObject json = (JSONObject)JSONValue.parseWithException(new FileReader(jsonFile));
            
            SSTableWriter writer = new SSTableWriter(ssTablePath, json.size(), partitioner);
            List<DecoratedKey<?>> decoratedKeys = new ArrayList<DecoratedKey<?>>();
            
            for (String key : (Set<String>)json.keySet())
                decoratedKeys.add(partitioner.decorateKey(key));
            Collections.sort(decoratedKeys);

            for (DecoratedKey<?> rowKey : decoratedKeys)
            {
                if (cfType.equals("Super"))
                    addToSuperCF((JSONObject)json.get(rowKey.key), cfamily);
                else
                    addToStandardCF((JSONArray)json.get(rowKey.key), cfamily);
                           
                ColumnFamily.serializer().serializeWithIndexes(cfamily, dob);
                writer.append(rowKey, dob);
                dob.reset();
                cfamily.clear();
            }
            
            writer.closeAndOpenReader();
        }
        catch (ClassCastException cce)
        {
            throw new RuntimeException("Invalid JSON input, or incorrect column family.", cce);
        }
    }

    /**
     * Converts JSON to an SSTable file. JSON input can either be a file specified
     * using an optional command line argument, or supplied on standard in.
     * 
     * @param args command line arguments
     * @throws IOException on failure to open/read/write files or output streams
     * @throws ParseException on failure to parse JSON input
     */
    public static void main(String[] args) throws IOException, ParseException
    {
        String usage = String.format("Usage: %s -K keyspace -c column_family <json> <sstable>%n",
                SSTableImport.class.getName());

        CommandLineParser parser = new PosixParser();
        try
        {
            cmd = parser.parse(options, args);
        } catch (org.apache.commons.cli.ParseException e1)
        {
            System.err.println(e1.getMessage());
            System.err.println(usage);
            System.exit(1);
        }

        if (cmd.getArgs().length != 2)
        {
            System.err.println(usage);
            System.exit(1);
        }

        String json = cmd.getArgs()[0];
        String ssTable = cmd.getArgs()[1];
        String keyspace = cmd.getOptionValue(KEYSPACE_OPTION);
        String cfamily = cmd.getOptionValue(COLFAM_OPTION);

        importJson(json, keyspace, cfamily, ssTable);
        
        System.exit(0);
    }

}
