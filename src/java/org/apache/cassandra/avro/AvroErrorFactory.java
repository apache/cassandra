package org.apache.cassandra.avro;

import java.util.List;

import org.apache.avro.util.Utf8;

public class AvroErrorFactory
{
    public static InvalidRequestException newInvalidRequestException(Utf8 why)
    {
        InvalidRequestException exception = new InvalidRequestException();
        exception.why = why;
        return exception;
    }
    
    public static InvalidRequestException newInvalidRequestException(String why)
    {
        return newInvalidRequestException(new Utf8(why));
    }

    public static InvalidRequestException newInvalidRequestException(Throwable e)
    {
        InvalidRequestException exception = newInvalidRequestException(e.getMessage());
        exception.initCause(e);
        return exception;
    }
    
    public static NotFoundException newNotFoundException(Utf8 why)
    {
        NotFoundException exception = new NotFoundException();
        exception.why = why;
        return exception;
    }
    
    public static NotFoundException newNotFoundException(String why)
    {
        return newNotFoundException(new Utf8(why));
    }
    
    public static NotFoundException newNotFoundException()
    {
        return newNotFoundException(new Utf8());
    }
    
    public static TimedOutException newTimedOutException(Utf8 why)
    {
        TimedOutException exception = new TimedOutException();
        exception.why = why;
        return exception;
    }
    
    public static TimedOutException newTimedOutException(String why)
    {
        return newTimedOutException(new Utf8(why));
    }

    public static TimedOutException newTimedOutException()
    {
        return newTimedOutException(new Utf8());
    }
    
    public static UnavailableException newUnavailableException(Utf8 why)
    {
        UnavailableException exception = new UnavailableException();
        exception.why = why;
        return exception;
    }
    
    public static UnavailableException newUnavailableException(String why)
    {
        return newUnavailableException(new Utf8(why));
    }

    public static UnavailableException newUnavailableException(Throwable t) 
    {
        UnavailableException exception = newUnavailableException(t.getMessage());
        exception.initCause(t);
        return exception;
    }
    
    public static UnavailableException newUnavailableException()
    {
        return newUnavailableException(new Utf8());
    }
    
    public static TokenRange newTokenRange(String startRange, String endRange, List<? extends CharSequence> endpoints)
    {
        TokenRange tRange = new TokenRange();
        tRange.start_token = startRange;
        tRange.end_token = endRange;
        tRange.endpoints = (List<CharSequence>) endpoints;
        return tRange;
    }
}
