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
package org.apache.cassandra.cql3;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.cassandra.cql3.functions.Function;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.serializers.MarshalException;
import org.apache.cassandra.utils.JsonUtils;

import static java.lang.String.format;

/**
 * Term-related classes for INSERT JSON support.
 */
public final class Json
{
    public static final ColumnIdentifier JSON_COLUMN_ID = new ColumnIdentifier("[json]", true);

    private Json()
    {
    }

    public interface Raw
    {
        public Prepared prepareAndCollectMarkers(TableMetadata metadata, Collection<ColumnMetadata> receivers, VariableSpecifications boundNames);
    }

    /**
     * Represents a literal JSON string in an INSERT JSON statement.
     * For example: INSERT INTO mytable (key, col) JSON '{"key": 0, "col": 0}';
     */
    public static class Literal implements Raw
    {
        private final String text;

        public Literal(String text)
        {
            this.text = text;
        }

        public Prepared prepareAndCollectMarkers(TableMetadata metadata, Collection<ColumnMetadata> receivers, VariableSpecifications boundNames)
        {
            return new PreparedLiteral(parseJson(text, receivers));
        }
    }

    /**
     * Represents a marker for a JSON string in an INSERT JSON statement.
     * For example: INSERT INTO mytable (key, col) JSON ?;
     */
    public static class Marker implements Raw
    {
        protected final int bindIndex;

        public Marker(int bindIndex)
        {
            this.bindIndex = bindIndex;
        }

        public Prepared prepareAndCollectMarkers(TableMetadata metadata, Collection<ColumnMetadata> receivers, VariableSpecifications boundNames)
        {
            boundNames.add(bindIndex, makeReceiver(metadata));
            return new PreparedMarker(bindIndex, receivers);
        }

        private ColumnSpecification makeReceiver(TableMetadata metadata)
        {
            return new ColumnSpecification(metadata.keyspace, metadata.name, JSON_COLUMN_ID, UTF8Type.instance);
        }
    }

    /**
     * A prepared, full set of JSON values.
     */
    public static abstract class Prepared
    {
        public abstract Term.Raw getRawTermForColumn(ColumnMetadata def, boolean defaultUnset);
    }

    /**
     * A prepared literal set of JSON values
     */
    private static class PreparedLiteral extends Prepared
    {
        private final Map<ColumnIdentifier, Term> columnMap;

        public PreparedLiteral(Map<ColumnIdentifier, Term> columnMap)
        {
            this.columnMap = columnMap;
        }

        public Term.Raw getRawTermForColumn(ColumnMetadata def, boolean defaultUnset)
        {
            Term value = columnMap.get(def.name);
            return value == null
                   ? (defaultUnset ? Constants.UNSET_LITERAL : Constants.NULL_LITERAL)
                   : new ColumnValue(value);
        }
    }

    /**
     * A prepared bind marker for a set of JSON values
     */
    private static class PreparedMarker extends Prepared
    {
        private final int bindIndex;
        private final Collection<ColumnMetadata> columns;

        public PreparedMarker(int bindIndex, Collection<ColumnMetadata> columns)
        {
            this.bindIndex = bindIndex;
            this.columns = columns;
        }

        public RawDelayedColumnValue getRawTermForColumn(ColumnMetadata def, boolean defaultUnset)
        {
            return new RawDelayedColumnValue(this, def, defaultUnset);
        }
    }

    /**
     * A Terminal for a single column.
     * <p>
     * Note that this is intrinsically an already prepared term, but this still implements Term.Raw so that we can
     * easily use it to create raw operations.
     */
    private static class ColumnValue extends Term.Raw
    {
        private final Term term;

        public ColumnValue(Term term)
        {
            this.term = term;
        }

        @Override
        public Term prepare(String keyspace, ColumnSpecification receiver) throws InvalidRequestException
        {
            return term;
        }

        @Override
        public TestResult testAssignment(String keyspace, ColumnSpecification receiver)
        {
            return TestResult.NOT_ASSIGNABLE;
        }

        public AbstractType<?> getExactTypeIfKnown(String keyspace)
        {
            return null;
        }

        public String getText()
        {
            return term.toString();
        }
    }

    /**
     * A Raw term for a single column. Like ColumnValue, this is intrinsically already prepared.
     */
    private static class RawDelayedColumnValue extends Term.Raw
    {
        private final PreparedMarker marker;
        private final ColumnMetadata column;
        private final boolean defaultUnset;

        public RawDelayedColumnValue(PreparedMarker prepared, ColumnMetadata column, boolean defaultUnset)
        {
            this.marker = prepared;
            this.column = column;
            this.defaultUnset = defaultUnset;
        }

        @Override
        public Term prepare(String keyspace, ColumnSpecification receiver) throws InvalidRequestException
        {
            return new DelayedColumnValue(marker, column, defaultUnset);
        }

        @Override
        public TestResult testAssignment(String keyspace, ColumnSpecification receiver)
        {
            return TestResult.WEAKLY_ASSIGNABLE;
        }

        public AbstractType<?> getExactTypeIfKnown(String keyspace)
        {
            return null;
        }

        public String getText()
        {
            return marker.toString();
        }
    }

    /**
     * A NonTerminal for a single column. As with {@code ColumnValue}, this is intrinsically a prepared.
     */
    private static class DelayedColumnValue extends Term.NonTerminal
    {
        private final PreparedMarker marker;
        private final ColumnMetadata column;
        private final boolean defaultUnset;

        public DelayedColumnValue(PreparedMarker prepared, ColumnMetadata column, boolean defaultUnset)
        {
            this.marker = prepared;
            this.column = column;
            this.defaultUnset = defaultUnset;
        }

        @Override
        public void collectMarkerSpecification(VariableSpecifications boundNames)
        {
            // We've already collected what we should (and in practice this method is never called).
        }

        @Override
        public boolean containsBindMarker()
        {
            return true;
        }

        @Override
        public Terminal bind(QueryOptions options) throws InvalidRequestException
        {
            Term term = options.getJsonColumnValue(marker.bindIndex, column.name, marker.columns);
            return term == null
                   ? (defaultUnset ? Constants.UNSET_VALUE : null)
                   : term.bind(options);
        }

        @Override
        public void addFunctionsTo(List<Function> functions)
        {
        }
    }

    /**
     * Given a JSON string, return a map of columns to their values for the insert.
     */
    static Map<ColumnIdentifier, Term> parseJson(String jsonString, Collection<ColumnMetadata> expectedReceivers)
    {
        try
        {
            Map<String, Object> valueMap = JsonUtils.JSON_OBJECT_MAPPER.readValue(jsonString, Map.class);

            if (valueMap == null)
                throw new InvalidRequestException("Got null for INSERT JSON values");

            JsonUtils.handleCaseSensitivity(valueMap);

            Map<ColumnIdentifier, Term> columnMap = new HashMap<>(expectedReceivers.size());
            for (ColumnSpecification spec : expectedReceivers)
            {
                // We explicitely test containsKey() because the value itself can be null, and we want to distinguish an
                // explicit null value from no value
                if (!valueMap.containsKey(spec.name.toString()))
                    continue;

                Object parsedJsonObject = valueMap.remove(spec.name.toString());
                if (parsedJsonObject == null)
                {
                    // This is an explicit user null
                    columnMap.put(spec.name, Constants.NULL_VALUE);
                }
                else
                {
                    try
                    {
                        columnMap.put(spec.name, spec.type.fromJSONObject(parsedJsonObject));
                    }
                    catch (MarshalException exc)
                    {
                        throw new InvalidRequestException(format("Error decoding JSON value for %s: %s", spec.name, exc.getMessage()));
                    }
                }
            }

            if (!valueMap.isEmpty())
            {
                throw new InvalidRequestException(format("JSON values map contains unrecognized column: %s",
                                                         valueMap.keySet().iterator().next()));
            }

            return columnMap;
        }
        catch (IOException exc)
        {
            throw new InvalidRequestException(format("Could not decode JSON string as a map: %s. (String was: %s)", exc.toString(), jsonString));
        }
        catch (MarshalException exc)
        {
            throw new InvalidRequestException(exc.getMessage());
        }
    }
}
