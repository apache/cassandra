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
package org.apache.cassandra.index.sai;

import java.util.HashMap;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.cql3.statements.schema.IndexTarget;
import org.apache.cassandra.db.marshal.DoubleType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.marshal.UUIDType;
import org.apache.cassandra.schema.IndexMetadata;
import org.apache.cassandra.schema.Indexes;
import org.apache.cassandra.schema.TableMetadata;

public class IndexingSchemaLoader extends SchemaLoader
{
    public static TableMetadata.Builder ndiCFMD(String ksName, String cfName)
    {
        TableMetadata.Builder builder =
                TableMetadata.builder(ksName, cfName)
                             .addPartitionKeyColumn("id", UTF8Type.instance)
                             .addRegularColumn("first_name", UTF8Type.instance)
                             .addRegularColumn("last_name", UTF8Type.instance)
                             .addRegularColumn("age", Int32Type.instance)
                             .addRegularColumn("height", Int32Type.instance)
                             .addRegularColumn("timestamp", LongType.instance)
                             .addRegularColumn("address", UTF8Type.instance)
                             .addRegularColumn("score", DoubleType.instance)
                             .addRegularColumn("comment", UTF8Type.instance)
                             .addRegularColumn("comment_suffix_split", UTF8Type.instance)
                             .addRegularColumn("/output/full-name/", UTF8Type.instance)
                             .addRegularColumn("/data/output/id", UTF8Type.instance)
                             .addRegularColumn("first_name_prefix", UTF8Type.instance);

        Indexes.Builder indexes = Indexes.builder();

        indexes.add(IndexMetadata.fromSchemaMetadata(cfName + "_first_name", IndexMetadata.Kind.CUSTOM, new HashMap<String, String>()
        {{
            put(IndexTarget.CUSTOM_INDEX_OPTION_NAME, StorageAttachedIndex.class.getName());
            put(IndexTarget.TARGET_OPTION_NAME, "first_name");
        }}))
                .add(IndexMetadata.fromSchemaMetadata(cfName + "_last_name", IndexMetadata.Kind.CUSTOM, new HashMap<String, String>()
                {{
                    put(IndexTarget.CUSTOM_INDEX_OPTION_NAME, StorageAttachedIndex.class.getName());
                    put(IndexTarget.TARGET_OPTION_NAME, "last_name");
                }}))
                .add(IndexMetadata.fromSchemaMetadata(cfName + "_age", IndexMetadata.Kind.CUSTOM, new HashMap<String, String>()
                {{
                    put(IndexTarget.CUSTOM_INDEX_OPTION_NAME, StorageAttachedIndex.class.getName());
                    put(IndexTarget.TARGET_OPTION_NAME, "age");
                }}))
                .add(IndexMetadata.fromSchemaMetadata(cfName + "_timestamp", IndexMetadata.Kind.CUSTOM, new HashMap<String, String>()
                {{
                    put(IndexTarget.CUSTOM_INDEX_OPTION_NAME, StorageAttachedIndex.class.getName());
                    put(IndexTarget.TARGET_OPTION_NAME, "timestamp");

                }}))
                .add(IndexMetadata.fromSchemaMetadata(cfName + "_address", IndexMetadata.Kind.CUSTOM, new HashMap<String, String>()
                {{
                    put(IndexTarget.CUSTOM_INDEX_OPTION_NAME, StorageAttachedIndex.class.getName());
                    put(IndexTarget.TARGET_OPTION_NAME, "address");
                    put("case_sensitive", "false");
                }}))
                .add(IndexMetadata.fromSchemaMetadata(cfName + "_score", IndexMetadata.Kind.CUSTOM, new HashMap<String, String>()
                {{
                    put(IndexTarget.CUSTOM_INDEX_OPTION_NAME, StorageAttachedIndex.class.getName());
                    put(IndexTarget.TARGET_OPTION_NAME, "score");
                }}))
                .add(IndexMetadata.fromSchemaMetadata(cfName + "_comment", IndexMetadata.Kind.CUSTOM, new HashMap<String, String>()
                {{
                    put(IndexTarget.CUSTOM_INDEX_OPTION_NAME, StorageAttachedIndex.class.getName());
                    put(IndexTarget.TARGET_OPTION_NAME, "comment");
                    put("case_sensitive", "true");
                }}))
                .add(IndexMetadata.fromSchemaMetadata(cfName + "_comment_suffix_split", IndexMetadata.Kind.CUSTOM, new HashMap<String, String>()
                {{
                    put(IndexTarget.CUSTOM_INDEX_OPTION_NAME, StorageAttachedIndex.class.getName());
                    put(IndexTarget.TARGET_OPTION_NAME, "comment_suffix_split");
                }}))
                .add(IndexMetadata.fromSchemaMetadata(cfName + "_output_full_name", IndexMetadata.Kind.CUSTOM, new HashMap<String, String>()
                {{
                    put(IndexTarget.CUSTOM_INDEX_OPTION_NAME, StorageAttachedIndex.class.getName());
                    put(IndexTarget.TARGET_OPTION_NAME, "/output/full-name/");
                    put("case_sensitive", "false");
                }}))
                .add(IndexMetadata.fromSchemaMetadata(cfName + "_data_output_id", IndexMetadata.Kind.CUSTOM, new HashMap<String, String>()
                {{
                    put(IndexTarget.CUSTOM_INDEX_OPTION_NAME, StorageAttachedIndex.class.getName());
                    put(IndexTarget.TARGET_OPTION_NAME, "/data/output/id");
                }}))
                .add(IndexMetadata.fromSchemaMetadata(cfName + "_first_name_prefix", IndexMetadata.Kind.CUSTOM, new HashMap<String, String>()
                {{
                    put(IndexTarget.CUSTOM_INDEX_OPTION_NAME, StorageAttachedIndex.class.getName());
                    put(IndexTarget.TARGET_OPTION_NAME, "first_name_prefix");
                }}));

        return builder.indexes(indexes.build());
    }

    public static TableMetadata.Builder clusteringNDICFMD(String ksName, String cfName)
    {
        return clusteringNDICFMD(ksName, cfName, "location", "age", "height", "score");
    }

    public static TableMetadata.Builder clusteringNDICFMD(String ksName, String cfName, String...indexedColumns)
    {
        Indexes.Builder indexes = Indexes.builder();
        for (String indexedColumn : indexedColumns)
        {
            indexes.add(IndexMetadata.fromSchemaMetadata(cfName + "_" + indexedColumn, IndexMetadata.Kind.CUSTOM, new HashMap<String, String>()
            {{
                put(IndexTarget.CUSTOM_INDEX_OPTION_NAME, StorageAttachedIndex.class.getName());
                put(IndexTarget.TARGET_OPTION_NAME, indexedColumn);
            }}));
        }

        return TableMetadata.builder(ksName, cfName)
                            .addPartitionKeyColumn("name", UTF8Type.instance)
                            .addClusteringColumn("location", UTF8Type.instance)
                            .addClusteringColumn("age", Int32Type.instance)
                            .addRegularColumn("height", Int32Type.instance)
                            .addRegularColumn("score", DoubleType.instance)
                            .addStaticColumn("nickname", UTF8Type.instance)
                            .indexes(indexes.build());
    }

    public static TableMetadata.Builder staticNDICFMD(String ksName, String cfName)
    {
        TableMetadata.Builder builder =
                TableMetadata.builder(ksName, cfName)
                             .addPartitionKeyColumn("sensor_id", Int32Type.instance)
                             .addStaticColumn("sensor_type", UTF8Type.instance)
                             .addClusteringColumn("date", LongType.instance)
                             .addRegularColumn("value", DoubleType.instance)
                             .addRegularColumn("variance", Int32Type.instance);

        Indexes.Builder indexes = Indexes.builder();

        indexes.add(IndexMetadata.fromSchemaMetadata(cfName + "_sensor_type", IndexMetadata.Kind.CUSTOM, new HashMap<String, String>()
        {{
            put(IndexTarget.CUSTOM_INDEX_OPTION_NAME, StorageAttachedIndex.class.getName());
            put(IndexTarget.TARGET_OPTION_NAME, "sensor_type");
            put("case_sensitive", "false");
        }}));

        indexes.add(IndexMetadata.fromSchemaMetadata(cfName + "_value", IndexMetadata.Kind.CUSTOM, new HashMap<String, String>()
        {{
            put(IndexTarget.CUSTOM_INDEX_OPTION_NAME, StorageAttachedIndex.class.getName());
            put(IndexTarget.TARGET_OPTION_NAME, "value");
        }}));

        indexes.add(IndexMetadata.fromSchemaMetadata(cfName + "_variance", IndexMetadata.Kind.CUSTOM, new HashMap<String, String>()
        {{
            put(IndexTarget.CUSTOM_INDEX_OPTION_NAME, StorageAttachedIndex.class.getName());
            put(IndexTarget.TARGET_OPTION_NAME, "variance");
        }}));

        return builder.indexes(indexes.build());
    }

    public static TableMetadata.Builder fullTextSearchNDICFMD(String ksName, String cfName)
    {
        TableMetadata.Builder builder =
                TableMetadata.builder(ksName, cfName)
                             .addPartitionKeyColumn("song_id", UUIDType.instance)
                             .addRegularColumn("title", UTF8Type.instance)
                             .addRegularColumn("artist", UTF8Type.instance);

        Indexes.Builder indexes = Indexes.builder();

        indexes.add(IndexMetadata.fromSchemaMetadata(cfName + "_title", IndexMetadata.Kind.CUSTOM, new HashMap<String, String>()
        {{
            put(IndexTarget.CUSTOM_INDEX_OPTION_NAME, StorageAttachedIndex.class.getName());
            put(IndexTarget.TARGET_OPTION_NAME, "title");
        }}));

        indexes.add(IndexMetadata.fromSchemaMetadata(cfName + "_artist", IndexMetadata.Kind.CUSTOM, new HashMap<String, String>()
        {{
            put(IndexTarget.CUSTOM_INDEX_OPTION_NAME, StorageAttachedIndex.class.getName());
            put(IndexTarget.TARGET_OPTION_NAME, "artist");
            put("case_sensitive", "false");

        }}));

        return builder.indexes(indexes.build());
    }
}
