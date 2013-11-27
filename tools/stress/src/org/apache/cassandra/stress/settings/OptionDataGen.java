package org.apache.cassandra.stress.settings;

import org.apache.cassandra.stress.generatedata.DataGen;
import org.apache.cassandra.stress.generatedata.DataGenBytesRandom;
import org.apache.cassandra.stress.generatedata.DataGenFactory;
import org.apache.cassandra.stress.generatedata.DataGenStringDictionary;
import org.apache.cassandra.stress.generatedata.DataGenStringRepeats;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * For selecting a data generator
 */
class OptionDataGen extends Option
{

    private static final Pattern FULL = Pattern.compile("([A-Z]+)\\(([^)]+)\\)", Pattern.CASE_INSENSITIVE);
    private static final Pattern ARGS = Pattern.compile("[^,]+");

    final String prefix;
    private DataGenFactory factory;
    private final DataGenFactory defaultFactory;

    public OptionDataGen(String prefix, String defaultSpec)
    {
        this.prefix = prefix;
        this.defaultFactory = defaultSpec == null ? null : get(defaultSpec);
    }

    @Override
    public boolean accept(String param)
    {
        if (!param.toLowerCase().startsWith(prefix))
            return false;
        factory = get(param.substring(prefix.length()));
        return true;
    }

    private static DataGenFactory get(String spec)
    {
        Matcher m = FULL.matcher(spec);
        if (!m.matches())
            throw new IllegalArgumentException("Illegal data generator specification: " + spec);
        String name = m.group(1);
        Impl impl = LOOKUP.get(name.toLowerCase());
        if (impl == null)
            throw new IllegalArgumentException("Illegal data generator type: " + name);
        List<String> params = new ArrayList<>();
        m = ARGS.matcher(m.group(2));
        while (m.find())
            params.add(m.group());
        return impl.getFactory(params);
    }

    public DataGenFactory get()
    {
        return factory != null ? factory : defaultFactory;
    }

    @Override
    public boolean happy()
    {
        return factory != null || defaultFactory != null;
    }

    @Override
    public String shortDisplay()
    {
        return prefix + "ALG()";
    }

    public String longDisplay()
    {
        return shortDisplay() + ": Specify a data generator from:";
    }

    @Override
    public List<String> multiLineDisplay()
    {
        return Arrays.asList(
                GroupedOptions.formatMultiLine("RANDOM()", "Completely random byte generation"),
                GroupedOptions.formatMultiLine("REPEAT(<freq>)", "An MD5 hash of (opIndex % freq) combined with the column index"),
                GroupedOptions.formatMultiLine("DICT(<file>)","Random words from a dictionary; the file should be in the format \"<freq> <word>\"")
        );
    }

    private static final Map<String, Impl> LOOKUP;
    static
    {
        final Map<String, Impl> lookup = new HashMap<>();
        lookup.put("random", new RandomImpl());
        lookup.put("rand", new RandomImpl());
        lookup.put("rnd", new RandomImpl());
        lookup.put("repeat", new RepeatImpl());
        lookup.put("dict", new DictionaryImpl());
        lookup.put("dictionary", new DictionaryImpl());
        LOOKUP = lookup;
    }

    private static interface Impl
    {
        public DataGenFactory getFactory(List<String> params);
    }

    private static final class RandomImpl implements Impl
    {
        @Override
        public DataGenFactory getFactory(List<String> params)
        {
            if (params.size() != 0)
                throw new IllegalArgumentException("Invalid parameter list for random generator: " + params);
            return new RandomFactory();
        }
    }

    private static final class RepeatImpl implements Impl
    {

        @Override
        public DataGenFactory getFactory(List<String> params)
        {
            if (params.size() != 1)
                throw new IllegalArgumentException("Invalid parameter list for repeating generator: " + params);
            try
            {
                int repeatFrequency = Integer.parseInt(params.get(0));
                return new RepeatsFactory(repeatFrequency);
            } catch (Exception _)
            {
                throw new IllegalArgumentException("Invalid parameter list for repeating generator: " + params);
            }
        }
    }

    private static final class DictionaryImpl implements Impl
    {

        @Override
        public DataGenFactory getFactory(List<String> params)
        {
            if (params.size() != 1)
                throw new IllegalArgumentException("Invalid parameter list for dictionary generator: " + params);
            try
            {
                final File file = new File(params.get(0));
                return DataGenStringDictionary.getFactory(file);
            } catch (Exception e)
            {
                throw new IllegalArgumentException("Invalid parameter list for dictionary generator: " + params, e);
            }
        }
    }

    private static final class RandomFactory implements DataGenFactory
    {
        @Override
        public DataGen get()
        {
            return new DataGenBytesRandom();
        }
    }

    private static final class RepeatsFactory implements DataGenFactory
    {
        final int frequency;
        private RepeatsFactory(int frequency)
        {
            this.frequency = frequency;
        }

        @Override
        public DataGen get()
        {
            return new DataGenStringRepeats(frequency);
        }
    }

}
