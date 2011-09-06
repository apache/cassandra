package org.apache.cassandra.db.marshal;
/*
 * 
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 * 
 */

import java.nio.ByteBuffer;

import org.apache.cassandra.cql.jdbc.JdbcBoolean;
import org.apache.cassandra.utils.ByteBufferUtil;

public class BooleanType extends AbstractType<Boolean>
{
  public static final BooleanType instance = new BooleanType();

  BooleanType() {} // singleton

  public Boolean compose(ByteBuffer bytes)
  {
      return JdbcBoolean.instance.compose(bytes);
  }

  public ByteBuffer decompose(Boolean value)
  {
    return (value==null) ? ByteBufferUtil.EMPTY_BYTE_BUFFER
                         : value ? ByteBuffer.wrap(new byte[]{1})  // true
                                 : ByteBuffer.wrap(new byte[]{0}); // false
  }
  
  public int compare(ByteBuffer o1, ByteBuffer o2)
  {
      if ((o1 == null) || (o1.remaining() != 1))
        return ((o2 == null) || (o2.remaining() != 1)) ? 0 : -1;
      if ((o2 == null) || (o2.remaining() != 1))
        return 1;

      return o1.compareTo(o2);
  }

  public String getString(ByteBuffer bytes)
  {
      try
      {
          return JdbcBoolean.instance.getString(bytes);
      }
      catch (org.apache.cassandra.cql.jdbc.MarshalException e)
      {
          throw new MarshalException(e.getMessage());
      }
  }

  public ByteBuffer fromString(String source) throws MarshalException
  {
    
      if (source.isEmpty()|| source.equalsIgnoreCase(Boolean.FALSE.toString()))
          return decompose(false);
      
      if (source.equalsIgnoreCase(Boolean.TRUE.toString()))
          return decompose(true);
      
      throw new MarshalException(String.format("unable to make boolean from '%s'", source));
      
 }

  public void validate(ByteBuffer bytes) throws MarshalException
  {
      if (bytes.remaining() != 1 && bytes.remaining() != 0)
          throw new MarshalException(String.format("Expected 1 or 0 byte value (%d)", bytes.remaining()));
  }
}
