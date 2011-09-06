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

import org.apache.cassandra.cql.jdbc.JdbcDouble;
import org.apache.cassandra.utils.ByteBufferUtil;

public class DoubleType extends AbstractType<Double>
{
    public static final DoubleType instance = new DoubleType();

    DoubleType() {} // singleton    

    public Double compose(ByteBuffer bytes)
    {
        return JdbcDouble.instance.compose(bytes);
    }
    
    public ByteBuffer decompose(Double value)
    {
        return (value==null) ? ByteBufferUtil.EMPTY_BYTE_BUFFER : ByteBufferUtil.bytes(value);
    }
    

    public int compare(ByteBuffer o1, ByteBuffer o2)
    {
        if (o1.remaining() == 0)
        {
            return o2.remaining() == 0 ? 0 : -1;
        }
        if (o2.remaining() == 0)
        {
            return 1;
        }
        
        return compose(o1).compareTo(compose(o2));
    }

    public String getString(ByteBuffer bytes)
    {   
        try
        {
            return JdbcDouble.instance.getString(bytes);
        }
        catch (org.apache.cassandra.cql.jdbc.MarshalException e)
        {
            throw new MarshalException(e.getMessage());
        }
    }

    public ByteBuffer fromString(String source) throws MarshalException
    {
      // Return an empty ByteBuffer for an empty string.
      if (source.isEmpty())
          return ByteBufferUtil.EMPTY_BYTE_BUFFER;
      
      Double d;
      try
      {
          d = Double.parseDouble(source);
      }
      catch (NumberFormatException e1)
      {
          throw new MarshalException(String.format("unable to coerce '%s' to a double", source), e1);
      }
          
      return decompose(d);
    }

    public void validate(ByteBuffer bytes) throws MarshalException
    {
        if (bytes.remaining() != 8 && bytes.remaining() != 0)
            throw new MarshalException(String.format("Expected 8 or 0 byte value for a double (%d)", bytes.remaining()));
    }
}
