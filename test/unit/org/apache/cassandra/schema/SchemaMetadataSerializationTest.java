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
package org.apache.cassandra.schema;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.cql3.FieldIdentifier;
import org.apache.cassandra.cql3.functions.Function;
import org.apache.cassandra.cql3.functions.FunctionName;
import org.apache.cassandra.cql3.functions.FunctionResolver;
import org.apache.cassandra.cql3.functions.ScalarFunction;
import org.apache.cassandra.cql3.functions.masking.ColumnMask;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.marshal.UserType;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.tcm.serialization.Version;

import static org.apache.cassandra.utils.ByteBufferUtil.bytes;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * Tests serialization and deserialization of schema metadata (keyspace, table, type, field)
 * to verify that comments and security labels persist correctly across serialization boundaries.
 */
public class SchemaMetadataSerializationTest
{
    static
    {
        DatabaseDescriptor.daemonInitialization();
    }

    private static final String KEYSPACE = "test_ks";

    @Test
    public void testKeyspaceMetadataSerialization() throws IOException
    {
        KeyspaceMetadata original = KeyspaceMetadata.create(KEYSPACE,
                                                             KeyspaceParams.simple(1)
                                                                           .withComment("Test keyspace")
                                                                           .withSecurityLabel("TEST"));

        KeyspaceMetadata deserialized = serializeAndDeserializeKeyspace(original);

        assertNotNull("Deserialized keyspace should not be null", deserialized);
        assertEquals("Test keyspace", deserialized.params.comment);
        assertEquals("TEST", deserialized.params.securityLabel);
    }

    @Test
    public void testTableMetadataSerialization() throws IOException
    {
        TableMetadata original = createSimpleTable("users", "Users table", "SENSITIVE");
        TableMetadata deserialized = serializeAndDeserializeTable(original);

        assertNotNull("Deserialized table should not be null", deserialized);
        assertEquals("Users table", deserialized.params.comment);
        assertEquals("SENSITIVE", deserialized.params.securityLabel);
    }

    @Test
    public void testColumnMetadataSerialization() throws IOException
    {
        ColumnIdentifier emailCol = ColumnIdentifier.getInterned("email", false);
        TableMetadata original = TableMetadata.builder(KEYSPACE, "users")
                                              .addPartitionKeyColumn("id", Int32Type.instance)
                                              .addRegularColumn(emailCol, UTF8Type.instance)
                                              .alterColumnComment(emailCol, "User email")
                                              .alterColumnSecurityLabel(emailCol, "PII-EMAIL")
                                              .build();

        TableMetadata deserialized = serializeAndDeserializeTable(original);

        ColumnMetadata emailColumn = deserialized.getColumn(bytes("email"));
        assertNotNull("Email column should exist", emailColumn);
        assertEquals("User email", emailColumn.comment);
        assertEquals("PII-EMAIL", emailColumn.securityLabel);
    }

    /**
     * Round-trips a column masked with a function that takes no partial arguments, e.g. {@code MASKED WITH DEFAULT}.
     */
    @Test
    public void testMaskedColumnMetadataSerializationWithoutPartialArguments() throws IOException
    {
        ColumnMask mask = mask("mask_default", UTF8Type.instance);
        ColumnMetadata original = maskedColumn(mask);

        ColumnMetadata deserialized = serializeAndDeserializeColumn(original);

        assertEquals("Mask should survive serialization", mask, deserialized.getMask());
    }

    /**
     * Round-trips a column masked with a function that takes partial arguments, e.g.
     * {@code MASKED WITH mask_inner(2, 1)}. Both the arguments and the fields serialized after the mask need to
     * survive, since losing the arguments would also leave the input stream misaligned.
     */
    @Test
    public void testMaskedColumnMetadataSerializationWithPartialArguments() throws IOException
    {
        ColumnMask mask = mask("mask_inner",
                               UTF8Type.instance, Int32Type.instance, Int32Type.instance,
                               Int32Type.instance.decompose(2), Int32Type.instance.decompose(1));
        ColumnMetadata original = maskedColumn(mask);

        ColumnMetadata deserialized = serializeAndDeserializeColumn(original);

        assertEquals("Mask should survive serialization", mask, deserialized.getMask());
        assertEquals("Masked column comment should survive serialization", "User email", deserialized.comment);
        assertEquals("Masked column security label should survive serialization", "PII-EMAIL", deserialized.securityLabel);
    }

    /**
     * Round-trips a column masked with a function that takes a null partial argument, e.g.
     * {@code MASKED WITH mask_inner(null, 1)}.
     */
    @Test
    public void testMaskedColumnMetadataSerializationWithNullPartialArgument() throws IOException
    {
        ColumnMask mask = mask("mask_inner",
                               UTF8Type.instance, Int32Type.instance, Int32Type.instance,
                               null, Int32Type.instance.decompose(1));
        ColumnMetadata original = maskedColumn(mask);

        ColumnMetadata deserialized = serializeAndDeserializeColumn(original);

        assertEquals("Mask should survive serialization", mask, deserialized.getMask());
        assertEquals("Masked column comment should survive serialization", "User email", deserialized.comment);
    }

    @Test
    public void testUserTypeSerialization() throws IOException
    {
        UserType original = createUserType("address", "Address type", "PUBLIC");
        UserType deserialized = serializeAndDeserializeType(original);

        assertNotNull("Deserialized type should not be null", deserialized);
        assertEquals("Address type", deserialized.comment);
        assertEquals("PUBLIC", deserialized.securityLabel);
    }

    @Test
    public void testTypeFieldSerialization() throws IOException
    {
        FieldIdentifier streetField = FieldIdentifier.forUnquoted("street");
        FieldIdentifier cityField = FieldIdentifier.forUnquoted("city");

        Map<FieldIdentifier, String> fieldComments = new HashMap<>();
        fieldComments.put(streetField, "Street address");
        fieldComments.put(cityField, "City name");

        Map<FieldIdentifier, String> fieldLabels = new HashMap<>();
        fieldLabels.put(streetField, "PII");
        fieldLabels.put(cityField, "PUBLIC");

        UserType original = new UserType(KEYSPACE,
                                         bytes("address"),
                                         Arrays.asList(streetField, cityField),
                                         Arrays.asList(UTF8Type.instance, UTF8Type.instance),
                                         true,
                                         "Address type",
                                         "PUBLIC",
                                         fieldComments,
                                         fieldLabels);

        UserType deserialized = serializeAndDeserializeType(original);

        assertNotNull("Deserialized type should not be null", deserialized);
        assertEquals("Street address", deserialized.fieldComment(streetField));
        assertEquals("City name", deserialized.fieldComment(cityField));
        assertEquals("PII", deserialized.fieldSecurityLabel(streetField));
        assertEquals("PUBLIC", deserialized.fieldSecurityLabel(cityField));
    }

    @Test
    public void testTypesSerialization() throws IOException
    {
        UserType type1 = createUserType("type1", "Type 1 comment", "Type 1 label");
        UserType type2 = createUserType("type2", "Type 2 comment", "Type 2 label");
        Types original = Types.of(type1, type2);

        Types deserialized = serializeAndDeserializeTypes(original);

        UserType deserializedType1 = deserialized.getNullable(bytes("type1"));
        UserType deserializedType2 = deserialized.getNullable(bytes("type2"));

        assertNotNull("Type1 should exist", deserializedType1);
        assertNotNull("Type2 should exist", deserializedType2);
        assertEquals("Type 1 comment", deserializedType1.comment);
        assertEquals("Type 1 label", deserializedType1.securityLabel);
        assertEquals("Type 2 comment", deserializedType2.comment);
        assertEquals("Type 2 label", deserializedType2.securityLabel);
    }

    @Test
    public void testCompleteKeyspaceMetadataSerialization() throws IOException
    {
        UserType addressType = createUserType("address", "Address type", "PUBLIC");

        ColumnIdentifier nameCol = ColumnIdentifier.getInterned("name", false);
        TableMetadata table = TableMetadata.builder(KEYSPACE, "users")
                                           .addPartitionKeyColumn("id", Int32Type.instance)
                                           .addRegularColumn(nameCol, UTF8Type.instance)
                                           .alterColumnComment(nameCol, "User name")
                                           .alterColumnSecurityLabel(nameCol, "PUBLIC")
                                           .comment("Users table")
                                           .securityLabel("SENSITIVE")
                                           .build();

        KeyspaceMetadata original = KeyspaceMetadata.create(KEYSPACE,
                                                             KeyspaceParams.simple(1)
                                                                           .withComment("Test keyspace")
                                                                           .withSecurityLabel("TEST"),
                                                             Tables.of(table),
                                                             Views.none(),
                                                             Types.of(addressType),
                                                             UserFunctions.none());

        KeyspaceMetadata deserialized = serializeAndDeserializeKeyspace(original);

        // Verify keyspace metadata
        assertEquals("Test keyspace", deserialized.params.comment);
        assertEquals("TEST", deserialized.params.securityLabel);

        // Verify table metadata
        TableMetadata deserializedTable = deserialized.tables.getNullable("users");
        assertNotNull("Table should exist", deserializedTable);
        assertEquals("Users table", deserializedTable.params.comment);
        assertEquals("SENSITIVE", deserializedTable.params.securityLabel);

        // Verify column metadata
        ColumnMetadata nameColumn = deserializedTable.getColumn(bytes("name"));
        assertNotNull("Name column should exist", nameColumn);
        assertEquals("User name", nameColumn.comment);
        assertEquals("PUBLIC", nameColumn.securityLabel);

        // Verify type metadata
        UserType deserializedType = deserialized.types.getNullable(bytes("address"));
        assertNotNull("Type should exist", deserializedType);
        assertEquals("Address type", deserializedType.comment);
        assertEquals("PUBLIC", deserializedType.securityLabel);
    }

    @Test
    public void testBackwardCompatibilityV7() throws IOException
    {
        UserType type = createUserType("test_type", "Test comment", "Test label");

        // Serialize with V7 (should not include metadata)
        DataOutputBuffer out = new DataOutputBuffer();
        Types.serializer.serialize(Types.of(type), out, Version.V7);

        // Deserialize with V7
        DataInputBuffer in = new DataInputBuffer(out.toByteArray());
        Types deserializedTypes = Types.serializer.deserialize(KEYSPACE, in, Version.V7);

        // Verify metadata is empty for V7
        UserType deserializedType = deserializedTypes.getNullable(bytes("test_type"));
        assertNotNull("Type should exist", deserializedType);
        assertEquals("", deserializedType.comment);
        assertEquals("", deserializedType.securityLabel);
    }

    // Helper methods for creating test objects

    private TableMetadata createSimpleTable(String name, String comment, String securityLabel)
    {
        return TableMetadata.builder(KEYSPACE, name)
                            .addPartitionKeyColumn("id", Int32Type.instance)
                            .addRegularColumn("email", UTF8Type.instance)
                            .comment(comment)
                            .securityLabel(securityLabel)
                            .build();
    }

    /**
     * @param functionName the name of a native masking function
     * @param argTypes the types of the arguments of the function, including the type of the masked column
     * @param partialArgumentValues the values of the arguments of the function, excluding the masked column
     */
    private static ColumnMask mask(String functionName, List<AbstractType<?>> argTypes, ByteBuffer... partialArgumentValues)
    {
        Function function = FunctionResolver.get(KEYSPACE,
                                                 FunctionName.nativeFunction(functionName),
                                                 argTypes,
                                                 KEYSPACE,
                                                 "users",
                                                 argTypes.get(0),
                                                 UserFunctions.none());
        assertNotNull("Masking function should be resolvable", function);
        return new ColumnMask((ScalarFunction) function, partialArgumentValues);
    }

    private static ColumnMask mask(String functionName, AbstractType<?> columnType)
    {
        return mask(functionName, Collections.singletonList(columnType));
    }

    private static ColumnMask mask(String functionName,
                                   AbstractType<?> columnType,
                                   AbstractType<?> firstArgType,
                                   AbstractType<?> secondArgType,
                                   ByteBuffer firstArgValue,
                                   ByteBuffer secondArgValue)
    {
        return mask(functionName,
                    Arrays.asList(columnType, firstArgType, secondArgType),
                    firstArgValue,
                    secondArgValue);
    }

    private static ColumnMetadata maskedColumn(ColumnMask mask)
    {
        return ColumnMetadata.regularColumn(KEYSPACE, "users", "email", UTF8Type.instance, ColumnMetadata.NO_UNIQUE_ID)
                             .withNewMask(mask)
                             .withNewComment("User email")
                             .withNewSecurityLabel("PII-EMAIL");
    }

    private UserType createUserType(String name, String comment, String securityLabel)
    {
        FieldIdentifier field = FieldIdentifier.forUnquoted("field1");
        return new UserType(KEYSPACE,
                           bytes(name),
                           Arrays.asList(field),
                           Arrays.asList(UTF8Type.instance),
                           true,
                           comment,
                           securityLabel);
    }

    // Helper methods for serialization/deserialization

    private KeyspaceMetadata serializeAndDeserializeKeyspace(KeyspaceMetadata original) throws IOException
    {
        DataOutputBuffer out = new DataOutputBuffer();
        KeyspaceMetadata.serializer.serialize(original, out, Version.V8);

        DataInputBuffer in = new DataInputBuffer(out.toByteArray());
        return KeyspaceMetadata.serializer.deserialize(in, Version.V8);
    }

    private ColumnMetadata serializeAndDeserializeColumn(ColumnMetadata original) throws IOException
    {
        DataOutputBuffer out = new DataOutputBuffer();
        ColumnMetadata.serializer.serialize(original, out, Version.V8);

        DataInputBuffer in = new DataInputBuffer(out.toByteArray());
        return ColumnMetadata.serializer.deserialize(in, Types.none(), UserFunctions.none(), Version.V8);
    }

    private TableMetadata serializeAndDeserializeTable(TableMetadata original) throws IOException
    {
        DataOutputBuffer out = new DataOutputBuffer();
        TableMetadata.serializer.serialize(original, out, Version.V8);

        DataInputBuffer in = new DataInputBuffer(out.toByteArray());
        return TableMetadata.serializer.deserialize(in, Types.none(), UserFunctions.none(), Version.V8);
    }

    private UserType serializeAndDeserializeType(UserType original) throws IOException
    {
        Types types = Types.of(original);
        return serializeAndDeserializeTypes(types).getNullable(original.name);
    }

    private Types serializeAndDeserializeTypes(Types original) throws IOException
    {
        DataOutputBuffer out = new DataOutputBuffer();
        Types.serializer.serialize(original, out, Version.V8);

        DataInputBuffer in = new DataInputBuffer(out.toByteArray());
        return Types.serializer.deserialize(KEYSPACE, in, Version.V8);
    }
}
