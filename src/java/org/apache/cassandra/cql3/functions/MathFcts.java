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

 package org.apache.cassandra.cql3.functions;

 import java.nio.ByteBuffer;
 import java.util.Collection;
 import java.util.List;
 import java.util.stream.Collectors;

 import com.google.common.collect.ImmutableList;
 import org.slf4j.Logger;
 import org.slf4j.LoggerFactory;

 import org.apache.cassandra.db.marshal.*;
 import org.apache.cassandra.transport.ProtocolVersion;

 public class MathFcts
 {
     public static Logger logger = LoggerFactory.getLogger(MathFcts.class);

     private static final ImmutableList<NumberType<?>> numTypes = ImmutableList.of(ByteType.instance,
                                                                                   ShortType.instance,
                                                                                   Int32Type.instance,
                                                                                   FloatType.instance,
                                                                                   LongType.instance,
                                                                                   DoubleType.instance,
                                                                                   IntegerType.instance,
                                                                                   DecimalType.instance,
                                                                                   CounterColumnType.instance);

     public static Collection<Function> all()
     {
         return numTypes.stream()
                        .map(t -> ImmutableList.of(
                        absFct(t),
                        expFct(t),
                        logFct(t),
                        log10Fct(t),
                        roundFct(t)
                        ))
                        .flatMap(Collection::stream)
                        .collect(Collectors.toList());
     }


     public static NativeScalarFunction absFct(final NumberType<?> type)
     {
         return new NativeScalarFunction("abs", type, type)
         {
             @Override
             public ByteBuffer execute(ProtocolVersion protocolVersion, List<ByteBuffer> parameters)
             {
                 ByteBuffer bb = parameters.get(0);
                 if (bb == null)
                     return null;
                 return type.abs(bb);
             }
         };
     }

     public static NativeScalarFunction expFct(final NumberType<?> type)
     {
         return new NativeScalarFunction("exp", type, type)
         {
             @Override
             public ByteBuffer execute(ProtocolVersion protocolVersion, List<ByteBuffer> parameters)
             {
                 ByteBuffer bb = parameters.get(0);
                 if (bb == null)
                     return null;
                 return type.exp(bb);
             }
         };
     }

     public static NativeScalarFunction logFct(final NumberType<?> type)
     {
         return new NativeScalarFunction("log", type, type)
         {
             @Override
             public ByteBuffer execute(ProtocolVersion protocolVersion, List<ByteBuffer> parameters)
             {
                 ByteBuffer bb = parameters.get(0);
                 if (bb == null)
                     return null;
                 return type.log(bb);

             }
         };
     }

     public static NativeScalarFunction log10Fct(final NumberType<?> type)
     {
         return new NativeScalarFunction("log10", type, type)
         {
             @Override
             public ByteBuffer execute(ProtocolVersion protocolVersion, List<ByteBuffer> parameters)
             {
                 ByteBuffer bb = parameters.get(0);
                 if (bb == null)
                     return null;
                 return type.log10(bb);

             }
         };
     }

     public static NativeScalarFunction roundFct(final NumberType<?> type)
     {
         return new NativeScalarFunction("round", type, type)
         {
             @Override
             public ByteBuffer execute(ProtocolVersion protocolVersion, List<ByteBuffer> parameters)
             {
                 ByteBuffer bb = parameters.get(0);
                 if (bb == null)
                     return null;
                 return type.round(bb);

             }
         };
     }

 }
