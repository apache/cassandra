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

package org.apache.cassandra.db.compression;

import java.time.Instant;
import java.util.Arrays;

import javax.management.openmbean.ArrayType;
import javax.management.openmbean.CompositeData;
import javax.management.openmbean.CompositeDataSupport;
import javax.management.openmbean.CompositeType;
import javax.management.openmbean.OpenDataException;
import javax.management.openmbean.OpenType;
import javax.management.openmbean.SimpleType;
import javax.management.openmbean.TabularType;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import org.apache.cassandra.db.compression.CompressionDictionary.LightweightCompressionDictionary;

import static java.lang.String.format;

public class CompressionDictionaryDetailsTabularData
{
    /**
     * Position inside index names of tabular type of tabular data returned upon
     * listing dictionaries where table id is expected to be located.
     * We do not need to process this entry at all time, e.g. when not listing
     * orphaned compression dictionaries.
     */
    public static final int TABULAR_DATA_TYPE_TABLE_ID_INDEX = 2;

    /**
     * Position inside index names of tabular type of tabular data returned upon
     * listing dictionaries where raw dictionary is expected to be located.
     * We do not need to process this entry as listing does not contain any raw dictionary,
     * only exporting does.
     */
    public static final int TABULAR_DATA_TYPE_RAW_DICTIONARY_INDEX = 4;

    public static final String KEYSPACE_NAME = "Keyspace";
    public static final String TABLE_NAME = "Table";
    public static final String TABLE_ID_NAME = "TableId";
    public static final String DICT_ID_NAME = "DictId";
    public static final String DICT_NAME = "Dict";
    public static final String KIND_NAME = "Kind";
    public static final String CHECKSUM_NAME = "Checksum";
    public static final String SIZE_NAME = "Size";
    public static final String CREATED_AT_NAME = "CreatedAt";

    private static final String[] ITEM_NAMES = new String[]{ KEYSPACE_NAME,
                                                             TABLE_NAME,
                                                             TABLE_ID_NAME,
                                                             DICT_ID_NAME,
                                                             DICT_NAME,
                                                             KIND_NAME,
                                                             CHECKSUM_NAME,
                                                             SIZE_NAME,
                                                             CREATED_AT_NAME };

    private static final String[] ITEM_DESCS = new String[]{ "keyspace",
                                                             "table",
                                                             "table_id",
                                                             "dictionary_id",
                                                             "dictionary_bytes",
                                                             "kind",
                                                             "checksum",
                                                             "size",
                                                             "created_at" };

    private static final String TYPE_NAME = "DictionaryDetails";
    private static final String ROW_DESC = "DictionaryDetails";
    private static final OpenType<?>[] ITEM_TYPES;
    private static final CompositeType COMPOSITE_TYPE;
    public static final TabularType TABULAR_TYPE;

    static
    {
        try
        {
            ITEM_TYPES = new OpenType[]{ SimpleType.STRING, // keyspace
                                         SimpleType.STRING, // table
                                         SimpleType.STRING, // tableId
                                         SimpleType.LONG, // dict id
                                         new ArrayType<String[]>(SimpleType.BYTE, true), // dict bytes
                                         SimpleType.STRING, // kind
                                         SimpleType.INTEGER, // checksum
                                         SimpleType.INTEGER, // size of dict bytes
                                         SimpleType.STRING }; // created_at

            COMPOSITE_TYPE = new CompositeType(TYPE_NAME, ROW_DESC, ITEM_NAMES, ITEM_DESCS, ITEM_TYPES);
            TABULAR_TYPE = new TabularType(TYPE_NAME, ROW_DESC, COMPOSITE_TYPE, ITEM_NAMES);
        }
        catch (OpenDataException e)
        {
            throw new RuntimeException(e);
        }
    }

    /**
     * This method is meant to be call when listing dictionaries, we do not need actual dictionary byte arrays.
     *
     * @param dictionary lightweight dictionary to create composite data from
     * @return composite data representing dictionary
     */
    public static CompositeData fromLightweightCompressionDictionary(LightweightCompressionDictionary dictionary)
    {
        try
        {
            return new CompositeDataSupport(COMPOSITE_TYPE,
                                            ITEM_NAMES,
                                            new Object[]
                                            {
                                            dictionary.keyspaceName,
                                            dictionary.tableName,
                                            dictionary.tableId,
                                            dictionary.dictId.id,
                                            null, // on purpose not returning actual dictionary
                                            dictionary.dictId.kind.name(),
                                            dictionary.checksum,
                                            dictionary.size,
                                            dictionary.createdAt.toString()
                                            });
        }
        catch (OpenDataException e)
        {
            throw new RuntimeException(e);
        }
    }

    /**
     * Used upon exporting a dictionary.
     *
     * @param keyspace   keyspace of a dictionary
     * @param table      table of a dictionary
     * @param tableId    id of a table dictionary is for
     * @param dictionary dictionary itself
     * @return composite data representing dictionary
     */
    public static CompositeData fromCompressionDictionary(String keyspace, String table, String tableId, CompressionDictionary dictionary)
    {
        try
        {
            return new CompositeDataSupport(COMPOSITE_TYPE,
                                            ITEM_NAMES,
                                            new Object[]
                                            {
                                            keyspace,
                                            table,
                                            tableId,
                                            dictionary.dictId().id,
                                            dictionary.rawDictionary(),
                                            dictionary.kind().name(),
                                            dictionary.checksum(),
                                            dictionary.rawDictionary().length,
                                            dictionary.createdAt().toString(),
                                            });
        }
        catch (OpenDataException e)
        {
            throw new RuntimeException(e);
        }
    }

    /**
     * Used upon deserialisation of data to composite data, e.g. upon importing a dictionary from JSON
     * string to CompositeData before they are sent over JMX to import method.
     *
     * @param dataObject data class object to get composite data from
     * @return composite data representing data class
     */
    public static CompositeData fromCompressionDictionaryDataObject(CompressionDictionaryDataObject dataObject)
    {
        try
        {
            return new CompositeDataSupport(COMPOSITE_TYPE,
                                            ITEM_NAMES,
                                            new Object[]
                                            {
                                            dataObject.keyspace,
                                            dataObject.table,
                                            dataObject.tableId,
                                            dataObject.dictId,
                                            dataObject.dict,
                                            dataObject.kind,
                                            dataObject.dictChecksum,
                                            dataObject.dictLength,
                                            dataObject.createdAt.toString()
                                            });
        }
        catch (OpenDataException e)
        {
            throw new RuntimeException(e);
        }
    }

    /**
     * Deserializes data to convenience object to work further with.
     *
     * @param compositeData data to create data object from
     * @return deserialized composite data to convenience object
     * @throws IllegalArgumentException if values in deserialized object are invalid.
     */
    public static CompressionDictionaryDataObject fromCompositeData(CompositeData compositeData)
    {
        return new CompressionDictionaryDataObject((String) compositeData.get(CompressionDictionaryDetailsTabularData.KEYSPACE_NAME),
                                                   (String) compositeData.get(CompressionDictionaryDetailsTabularData.TABLE_NAME),
                                                   (String) compositeData.get(CompressionDictionaryDetailsTabularData.TABLE_ID_NAME),
                                                   (Long) compositeData.get(CompressionDictionaryDetailsTabularData.DICT_ID_NAME),
                                                   (byte[]) compositeData.get(CompressionDictionaryDetailsTabularData.DICT_NAME),
                                                   (String) compositeData.get(CompressionDictionaryDetailsTabularData.KIND_NAME),
                                                   (Integer) compositeData.get(CompressionDictionaryDetailsTabularData.CHECKSUM_NAME),
                                                   (Integer) compositeData.get(CompressionDictionaryDetailsTabularData.SIZE_NAME),
                                                   Instant.parse((String) compositeData.get(CompressionDictionaryDetailsTabularData.CREATED_AT_NAME)));
    }

    public static class CompressionDictionaryDataObject
    {
        public final String keyspace;
        public final String table;
        public final String tableId;
        public final long dictId;
        public final byte[] dict;
        public final String kind;
        public final int dictChecksum;
        public final int dictLength;
        public final Instant createdAt;

        @JsonCreator
        public CompressionDictionaryDataObject(@JsonProperty("keyspace") String keyspace,
                                               @JsonProperty("table") String table,
                                               @JsonProperty("tableId") String tableId,
                                               @JsonProperty("dictId") long dictId,
                                               @JsonProperty("dict") byte[] dict,
                                               @JsonProperty("kind") String kind,
                                               @JsonProperty("dictChecksum") int dictChecksum,
                                               @JsonProperty("dictLength") int dictLength,
                                               @JsonProperty("createdAt") Instant createdAt)
        {
            this.keyspace = keyspace;
            this.table = table;
            this.tableId = tableId;
            this.dictId = dictId;
            this.dict = dict;
            this.kind = kind;
            this.dictChecksum = dictChecksum;
            this.dictLength = dictLength;
            this.createdAt = createdAt;

            validate();
        }

        /**
         * An object of this class is considered to be valid if:
         *
         * <ul>
         *     <li>keyspace, table and table id are not null</li>
         *     <li>dict id is lower than 0</li>
         *     <li>dict is not null nor empty</li>
         *     <li>dict length is less than or equal to 1MiB</li>
         *     <li>kind is not null and there is such {@link org.apache.cassandra.db.compression.CompressionDictionary.Kind}</li>
         *     <li>dictLength is bigger than 0</li>
         *     <li>dictLength has to be equal to dict's length</li>
         *     <li>dictChecksum has to be equal to checksum computed as part of this method</li>
         *     <li>creation date is not null</li>
         * </ul>
         */
        private void validate()
        {
            if (keyspace == null)
                throw new IllegalArgumentException("Keyspace not specified.");
            if (table == null)
                throw new IllegalArgumentException("Table not specified.");
            if (tableId == null)
                throw new IllegalArgumentException("Table id not specified.");
            if (dictId <= 0)
                throw new IllegalArgumentException("Provided dictionary id must be positive but it is '" + dictId + "'.");
            if (dict == null || dict.length == 0)
                throw new IllegalArgumentException("Provided dictionary byte array is null or empty.");
            if (kind == null)
                throw new IllegalArgumentException("Provided kind is null.");

            CompressionDictionary.Kind dictionaryKind;

            try
            {
                dictionaryKind = CompressionDictionary.Kind.valueOf(kind);
            }
            catch (IllegalArgumentException ex)
            {
                throw new IllegalArgumentException("There is no such dictionary kind like '" + kind + "'. Available kinds: " + Arrays.asList(CompressionDictionary.Kind.values()));
            }

            if (dictLength <= 0)
                throw new IllegalArgumentException("Size has to be strictly positive number, it is '" + dictLength + "'.");
            if (dict.length != dictLength)
                throw new IllegalArgumentException("The length of the provided dictionary array (" + dict.length + ") is not equal to provided length value (" + dictLength + ").");
            if (createdAt == null)
                throw new IllegalArgumentException("The creation date not specified.");

            int checksumOfDictionaryToImport = CompressionDictionary.calculateChecksum((byte) dictionaryKind.ordinal(), dictId, dict);
            if (checksumOfDictionaryToImport != dictChecksum)
            {
                throw new IllegalArgumentException(format("Computed checksum of dictionary to import (%s) is different from checksum specified on input (%s).",
                                                          checksumOfDictionaryToImport,
                                                          dictChecksum));
            }
        }
    }
}
