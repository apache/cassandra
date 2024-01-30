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

package org.apache.cassandra.io.util;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;
import java.util.zip.CRC32C;
import java.util.zip.Checksum;

import org.junit.Test;

import accord.utils.Gen;
import accord.utils.Gens;
import accord.utils.Property.Command;
import accord.utils.Property.Commands;
import accord.utils.Property.UnitCommand;
import org.apache.cassandra.utils.FailingConsumer;
import org.assertj.core.api.Assertions;

import static accord.utils.Property.stateful;

public class ChecksumedDataTest
{
    public static final Supplier<Checksum> CHECKSUM_SUPPLIER = CRC32C::new;

    @Test
    public void singleType()
    {
        DataOutputBuffer out = new DataOutputBuffer();
        stateful().check(new Commands<DataOutputBuffer, DataOutputBuffer>()
        {
            @Override
            public Gen<DataOutputBuffer> genInitialState()
            {
                return ignore -> {
                    out.clear();
                    return out;
                };
            }

            @Override
            public DataOutputBuffer createSut(DataOutputBuffer dataOutputBuffer)
            {
                return dataOutputBuffer;
            }

            @Override
            public Gen<Command<DataOutputBuffer, DataOutputBuffer, ?>> commands(DataOutputBuffer dataOutputBuffer)
            {
                return Gens.oneOf(
                rs -> {
                    boolean b = rs.nextBoolean();
                    return new StatelessChecksumCommand<>(out -> out.writeBoolean(b), DataInputPlus::readBoolean, () -> b);
                },
                rs -> {
                    short s = (short) rs.nextInt(Short.MIN_VALUE, Short.MAX_VALUE);
                    return new StatelessChecksumCommand<>(out -> out.writeShort(s), DataInputPlus::readShort, () -> s);
                },
                rs -> {
                    char c = (char) rs.nextInt(Character.MIN_VALUE, Character.MAX_VALUE);
                    return new StatelessChecksumCommand<>(out -> out.writeChar(c), DataInputPlus::readChar, () -> c);
                },
                rs -> {
                    int value = rs.nextInt();
                    return new StatelessChecksumCommand<>(out -> out.writeInt(value), DataInputPlus::readInt, () -> value);
                },
                rs -> {
                    float value = rs.nextFloat();
                    return new StatelessChecksumCommand<>(out -> out.writeFloat(value), DataInputPlus::readFloat, () -> value);
                },
                rs -> {
                    double value = rs.nextDouble();
                    return new StatelessChecksumCommand<>(out -> out.writeDouble(value), DataInputPlus::readDouble, () -> value);
                }
                );
            }
        });
    }

    @Test
    public void withState()
    {
        DataOutputBuffer out = new DataOutputBuffer();
        ChecksumedDataOutputPlus checksummedOut = new ChecksumedDataOutputPlus(out, CHECKSUM_SUPPLIER);
        checksummedOut.resetChecksum();
        stateful().check(new Commands<ChecksumedDataOutputPlus, List<StatefulChecksumCommand<?>>>() {
            @Override
            public Gen<ChecksumedDataOutputPlus> genInitialState()
            {
                return ignore -> {
                    out.clear();
                    checksummedOut.resetChecksum();
                    return checksummedOut;
                };
            }

            @Override
            public List<StatefulChecksumCommand<?>> createSut(ChecksumedDataOutputPlus checksumedDataOutputPlus)
            {
                return new ArrayList<>(1000);
            }

            @Override
            public Gen<Command<ChecksumedDataOutputPlus, List<StatefulChecksumCommand<?>>, ?>> commands(ChecksumedDataOutputPlus checksumedDataOutputPlus)
            {
                return Gens.oneOf(
                rs -> {
                    boolean b = rs.nextBoolean();
                    return new StatefulChecksumCommand<>(out -> out.writeBoolean(b), DataInputPlus::readBoolean, () -> b);
                },
                rs -> {
                    short s = (short) rs.nextInt(Short.MIN_VALUE, Short.MAX_VALUE);
                    return new StatefulChecksumCommand<>(out -> out.writeShort(s), DataInputPlus::readShort, () -> s);
                },
                rs -> {
                    char c = (char) rs.nextInt(Character.MIN_VALUE, Character.MAX_VALUE);
                    return new StatefulChecksumCommand<>(out -> out.writeChar(c), DataInputPlus::readChar, () -> c);
                },
                rs -> {
                    int value = rs.nextInt();
                    return new StatefulChecksumCommand<>(out -> out.writeInt(value), DataInputPlus::readInt, () -> value);
                },
                rs -> {
                    float value = rs.nextFloat();
                    return new StatefulChecksumCommand<>(out -> out.writeFloat(value), DataInputPlus::readFloat, () -> value);
                },
                rs -> {
                    double value = rs.nextDouble();
                    return new StatefulChecksumCommand<>(out -> out.writeDouble(value), DataInputPlus::readDouble, () -> value);
                }
                );
            }

            @Override
            public void destroySut(List<StatefulChecksumCommand<?>> sut) throws Throwable
            {
                ChecksumedDataInputPlus in = new ChecksumedDataInputPlus(new DataInputBuffer(out.unsafeGetBufferAndFlip(), false), CHECKSUM_SUPPLIER);
                for (StatefulChecksumCommand<?> cmd : sut)
                {
                    Assertions.assertThat(cmd.read.apply(in)).isEqualTo(cmd.expected.get());
                    Assertions.assertThat(in.checksum().getValue()).isEqualTo(cmd.checksum);
                }
            }
        });
    }

    public interface FailingFunction<I, O>
    {
        O apply(I input) throws Throwable;
    }

    private static class StatefulChecksumCommand<T> implements UnitCommand<ChecksumedDataOutputPlus, List<StatefulChecksumCommand<?>>>
    {
        private final FailingConsumer<DataOutputPlus> update;
        private final FailingFunction<DataInputPlus, T> read;
        private final Supplier<T> expected;
        private Long checksum = null;

        private StatefulChecksumCommand(FailingConsumer<DataOutputPlus> update, FailingFunction<DataInputPlus, T> read, Supplier<T> expected)
        {
            this.update = update;
            this.read = read;
            this.expected = expected;
        }

        @Override
        public void applyUnit(ChecksumedDataOutputPlus out) throws Throwable
        {
            update.doAccept(out);
            checksum = out.checksum().getValue();
        }

        @Override
        public void runUnit(List<StatefulChecksumCommand<?>> sut)
        {
            sut.add(this);
        }
    }

    private static class StatelessChecksumCommand<T> implements Command<DataOutputBuffer, DataOutputBuffer, Long>
    {
        private final FailingConsumer<DataOutputPlus> update;
        private final FailingFunction<DataInputPlus, T> read;
        private final Supplier<T> expected;

        private StatelessChecksumCommand(FailingConsumer<DataOutputPlus> update,
                                         FailingFunction<DataInputPlus, T> read,
                                         Supplier<T> expected)
        {
            this.update = update;
            this.read = read;
            this.expected = expected;
        }

        @Override
        public Long apply(DataOutputBuffer out) throws Throwable
        {
            out.clear();
            ChecksumedDataOutputPlus c = new ChecksumedDataOutputPlus(out, CHECKSUM_SUPPLIER);
            update.doAccept(c);
            return c.checksum().getValue();
        }

        @Override
        public Long run(DataOutputBuffer out) throws Throwable
        {
            out.clear();
            update.doAccept(out);
            ChecksumedDataInputPlus i = new ChecksumedDataInputPlus(new DataInputBuffer(out.unsafeGetBufferAndFlip(), false), CHECKSUM_SUPPLIER);
            Assertions.assertThat(read.apply(i)).isEqualTo(expected.get());
            return i.checksum().getValue();
        }

        @Override
        public void checkPostconditions(DataOutputBuffer dataOutputBuffer, Long expected,
                                        DataOutputBuffer sut, Long actual)
        {
            Assertions.assertThat(actual).isEqualTo(expected);
        }

        @Override
        public String detailed(DataOutputBuffer dataOutputBuffer)
        {
            return expected.get().getClass().getSimpleName();
        }
    }
}