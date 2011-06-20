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
import static org.apache.cassandra.db.marshal.TimeUUIDType.iso8601Patterns;

import java.nio.ByteBuffer;
import java.sql.Types;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.commons.lang.time.DateUtils;

public class DateType extends AbstractType<Date>
{
    public static final DateType instance = new DateType();

    static final String DEFAULT_FORMAT = iso8601Patterns[3];
    
    static final SimpleDateFormat FORMATTER = new SimpleDateFormat(DEFAULT_FORMAT);

    DateType() {} // singleton

    public Date compose(ByteBuffer bytes)
    {
        return new Date(ByteBufferUtil.toLong(bytes));
    }
    
    public ByteBuffer decompose(Date value)
    {
      return (value==null) ? ByteBufferUtil.EMPTY_BYTE_BUFFER
                           : ByteBufferUtil.bytes(value.getTime());
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

        return ByteBufferUtil.compareUnsigned(o1, o2);
    }

    public String getString(ByteBuffer bytes)
    {
        if (bytes.remaining() == 0)
        {
            return "";
        }
        if (bytes.remaining() != 8)
        {
            throw new MarshalException("A date is exactly 8 bytes (stored as a long): "+bytes.remaining());
        }
        
        // uses ISO-8601 formatted string
        return FORMATTER.format(new Date(bytes.getLong(bytes.position())));
    }

    public String toString(Date d)
    {
      // uses ISO-8601 formatted string
      return FORMATTER.format(d);
    }

    public ByteBuffer fromString(String source) throws MarshalException
    {
      // Return an empty ByteBuffer for an empty string.
      if (source.isEmpty())
          return ByteBufferUtil.EMPTY_BYTE_BUFFER;
      
      long millis;
      ByteBuffer idBytes = null;
      
      if (source.toLowerCase().equals("now"))
      {
          millis = System.currentTimeMillis();
          idBytes = ByteBufferUtil.bytes(millis);
      }
      // Milliseconds since epoch?
      else if (source.matches("^\\d+$"))
      {
          try
          {
              idBytes = ByteBufferUtil.bytes(Long.parseLong(source));
          }
          catch (NumberFormatException e)
          {
              throw new MarshalException(String.format("unable to make long (for date) from:  '%s'", source), e);
          }
      }
      // Last chance, attempt to parse as date-time string
      else
      {
          try
          {
              millis = DateUtils.parseDate(source, iso8601Patterns).getTime();
              idBytes = ByteBufferUtil.bytes(millis);
          }
          catch (ParseException e1)
          {
              throw new MarshalException(String.format("unable to coerce '%s' to a  formatted date (long)", source), e1);
          }
      }
          
      return idBytes;
    }

    public void validate(ByteBuffer bytes) throws MarshalException
    {
        if (bytes.remaining() != 8 && bytes.remaining() != 0)
            throw new MarshalException(String.format("Expected 8 or 0 byte long for date (%d)", bytes.remaining()));
    }

    public Class<Date> getType()
    {
        return Date.class;
    }

    public boolean isSigned()
    {
      return false;
    }

    public boolean isCaseSensitive()
    {
      return false;
    }

    public boolean isCurrency()
    {
      return false;
    }

    public int getPrecision(Date obj)
    {
      return -1;
    }

    public int getScale(Date obj)
    {
      return -1;
    }

    public int getJdbcType()
    {
      return Types.DATE;
    }

    public boolean needsQuotes()
    {
      return false;
    }
}
