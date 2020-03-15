package org.apache.cassandra.config;

public interface Converter<Original, Current>
{
    Class<Original> getInputType();

    Current apply(Original value);

    public static final class IdentityConverter implements Converter<Object, Object>
    {
        public Class<Object> getInputType()
        {
            return null; // null means 'unchanged'  mostly used for renames
        }

        public Object apply(Object value)
        {
            return value;
        }
    }

    public static final class MillisDurationConverter implements Converter<Long, Duration>
    {
        public Class<Long> getInputType()
        {
            return Long.class;
        }

        public Duration apply(Long value)
        {
            if (value == null)
                return null;
            return Duration.inMilliseconds(value.longValue());
        }
    }

    public static final class MillisDurationInDoubleConverter implements Converter<Double, Duration>
    {

        public Class<Double> getInputType()
        {
            return Double.class;
        }

        public Duration apply(Double value)
        {
            if (value == null)
                return null;
            return Duration.inMilliseconds((long)value.doubleValue());
        }
    }

    public static final class MillisDurationConverterCustom implements Converter<Long, Duration>
    {
        public Class<Long> getInputType()
        {
            return Long.class;
        }

        public Duration apply(Long value)
        {
            if (value == null)
                return null;

            if (value.equals((long) -1))
                value = 0L;

            return Duration.inMilliseconds(value.longValue());
        }
    }

    public static final class SecondsDurationConverter implements Converter<Long, Duration>
    {
        public Class<Long> getInputType()
        {
            return Long.class;
        }

        public Duration apply(Long value)
        {
            if (value == null)
                return null;
            return Duration.inSeconds(value.longValue());
        }
    }

    public static final class MinutesDurationConverter implements Converter<Long, Duration>
    {
        public Class<Long> getInputType()
        {
            return Long.class;
        }

        public Duration apply(Long value)
        {
            if (value == null)
                return null;
            return Duration.inMinutes(value.longValue());
        }
    }

    public static final class MegabytesDataStorageConverter implements Converter<Long, DataStorage>
    {
        public Class<Long> getInputType()
        {
            return Long.class;
        }

        public DataStorage apply(Long value)
        {
            if (value == null)
                return null;
            return DataStorage.inMegabytes(value.longValue());
        }
    }

    public static final class KilobytesDataStorageConverter implements Converter<Long, DataStorage>
    {
        public Class<Long> getInputType()
        {
            return Long.class;
        }

        public DataStorage apply(Long value)
        {
            if (value == null)
                return null;
            return DataStorage.inKilobytes(value.longValue());
        }
    }

    public static final class BytesDataStorageConverter implements Converter<Long, DataStorage>
    {
        public Class<Long> getInputType()
        {
            return Long.class;
        }

        public DataStorage apply(Long value)
        {
            if (value == null)
                return null;
            return DataStorage.inBytes(value.longValue());
        }
    }

    public static final class MegabitsPerSecondBitRateConverter implements Converter<Long, BitRate>
    {
        public Class<Long> getInputType()
        {
            return Long.class;
        }

        public BitRate apply(Long value)
        {
            if (value == null)
                return null;
            return BitRate.inMegabitsPerSecond(value.longValue());
        }
    }
}
