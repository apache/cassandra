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
package org.apache.cassandra.cql3.operations;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;

import org.apache.cassandra.cql3.*;
import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.Column;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.CollectionType;
import org.apache.cassandra.db.marshal.ListType;
import org.apache.cassandra.db.marshal.MapType;
import org.apache.cassandra.db.marshal.SetType;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.utils.Pair;

public class PreparedOperation implements Operation
{
    public enum Kind { SET, PREPARED_PLUS, PLUS_PREPARED, MINUS_PREPARED }

    private final Term preparedValue;
    private final Kind kind;

    public PreparedOperation(Term value, Kind kind)
    {
        assert value.isBindMarker();
        this.preparedValue = value;
        this.kind = kind;
    }

    public void execute(ColumnFamily cf,
                        ColumnNameBuilder builder,
                        AbstractType<?> validator,
                        UpdateParameters params,
                        List<Pair<ByteBuffer, Column>> list) throws InvalidRequestException
    {
        if (validator instanceof CollectionType)
        {
            switch (((CollectionType)validator).kind)
            {
                case LIST:
                    switch (kind)
                    {
                        case SET:
                            ListOperation.doSetFromPrepared(cf, builder, (ListType)validator, preparedValue, params);
                            break;
                        case PREPARED_PLUS:
                            ListOperation.doPrependFromPrepared(cf, builder, (ListType)validator, preparedValue, params);
                            break;
                        case PLUS_PREPARED:
                            ListOperation.doAppendFromPrepared(cf, builder, (ListType)validator, preparedValue, params);
                            break;
                        case MINUS_PREPARED:
                            ListOperation.doDiscardFromPrepared(cf, builder, (ListType)validator, preparedValue, params, list);
                            break;
                    }
                    break;
                case SET:
                    switch (kind)
                    {
                        case SET:
                            SetOperation.doSetFromPrepared(cf, builder, (SetType)validator, preparedValue, params);
                            break;
                        case PREPARED_PLUS:
                            throw new InvalidRequestException("Unsupported syntax, cannot add to a prepared set");
                        case PLUS_PREPARED:
                            SetOperation.doAddFromPrepared(cf, builder, (SetType)validator, preparedValue, params);
                            break;
                        case MINUS_PREPARED:
                            SetOperation.doDiscardFromPrepared(cf, builder, (SetType)validator, preparedValue, params);
                            break;
                    }
                    break;
                case MAP:
                    switch (kind)
                    {
                        case SET:
                            MapOperation.doSetFromPrepared(cf, builder, (MapType)validator, preparedValue, params);
                            break;
                        case PREPARED_PLUS:
                            throw new InvalidRequestException("Unsupported syntax, cannot put to a prepared map");
                        case PLUS_PREPARED:
                            MapOperation.doPutFromPrepared(cf, builder, (MapType)validator, preparedValue, params);
                            break;
                        case MINUS_PREPARED:
                            throw new InvalidRequestException("Unsuppoted syntax, discard syntax for map not supported");
                    }
                    break;
            }
        }
        else
        {
            switch (kind)
            {
                case SET:
                    ColumnOperation.Set(preparedValue).execute(cf, builder, validator, params, null);
                    break;
                case PREPARED_PLUS:
                    throw new InvalidRequestException("Unsupported syntax for increment, must be of the form X = X + <value>");
                case PLUS_PREPARED:
                    ColumnOperation.CounterInc(preparedValue).execute(cf, builder, validator, params, null);
                    break;
                case MINUS_PREPARED:
                    ColumnOperation.CounterDec(preparedValue).execute(cf, builder, validator, params, null);
                    break;
            }
        }
    }

    public void addBoundNames(ColumnSpecification column, ColumnSpecification[] boundNames) throws InvalidRequestException
    {
        if (preparedValue.isBindMarker())
            boundNames[preparedValue.bindIndex] = column;
    }

    public List<Term> getValues()
    {
        return Collections.singletonList(preparedValue);
    }

    public boolean requiresRead(AbstractType<?> validator)
    {
        // Only prepared operation requiring a read is list discard
        return (validator instanceof ListType) && kind == Kind.MINUS_PREPARED;
    }

    public boolean isPotentialCounterOperation() {
        return kind == Kind.PLUS_PREPARED || kind == Kind.MINUS_PREPARED;
    }

    public Type getType()
    {
        return Type.PREPARED;
    }
}
