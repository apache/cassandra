package org.apache.cassandra.stress.settings;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.thrift.transport.TTransportFactory;

public class SettingsTransport implements Serializable
{

    private final String fqFactoryClass;
    private TTransportFactory factory;

    public SettingsTransport(TOptions options)
    {
        if (options instanceof SSLOptions)
        {
            throw new UnsupportedOperationException();
        }
        else
        {
            this.fqFactoryClass = options.factory.value();
            try
            {
                Class<?> clazz = Class.forName(fqFactoryClass);
                if (!TTransportFactory.class.isAssignableFrom(clazz))
                    throw new ClassCastException();
                // check we can instantiate it
                clazz.newInstance();
            }
            catch (Exception e)
            {
                throw new IllegalArgumentException("Invalid transport factory class: " + options.factory.value(), e);
            }

        }
    }

    public synchronized TTransportFactory getFactory()
    {
        if (factory == null)
        {
            try
            {
                this.factory = (TTransportFactory) Class.forName(fqFactoryClass).newInstance();
            }
            catch (Exception e)
            {
                throw new RuntimeException(e);
            }
        }
        return factory;
    }

    // Option Declarations

    static class TOptions extends GroupedOptions
    {
        final OptionSimple factory = new OptionSimple("factory=", ".*", "org.apache.cassandra.cli.transport.FramedTransportFactory", "Fully-qualified TTransportFactory class name for creating a connection. Note: For Thrift over SSL, use org.apache.cassandra.stress.SSLTransportFactory.", false);

        @Override
        public List<? extends Option> options()
        {
            return Arrays.asList(factory);
        }
    }

    static final class SSLOptions extends TOptions
    {
        final OptionSimple trustStore = new OptionSimple("truststore=", ".*", null, "SSL: full path to truststore", false);
        final OptionSimple trustStorePw = new OptionSimple("truststore-password=", ".*", null, "", false);
        final OptionSimple protocol = new OptionSimple("ssl-protocol=", ".*", "TLS", "SSL: connections protocol to use", false);
        final OptionSimple alg = new OptionSimple("ssl-alg=", ".*", "SunX509", "SSL: algorithm", false);
        final OptionSimple storeType = new OptionSimple("store-type=", ".*", "TLS", "SSL: comma delimited list of encryption suites to use", false);
        final OptionSimple ciphers = new OptionSimple("ssl-ciphers=", ".*", "TLS", "SSL: comma delimited list of encryption suites to use", false);

        @Override
        public List<? extends Option> options()
        {
            return Arrays.asList(factory, trustStore, trustStorePw, protocol, alg, storeType, ciphers);
        }
    }

    // CLI Utility Methods

    public static SettingsTransport get(Map<String, String[]> clArgs)
    {
        String[] params = clArgs.remove("-transport");
        if (params == null)
            return new SettingsTransport(new TOptions());

        GroupedOptions options = GroupedOptions.select(params, new TOptions());
        if (options == null)
        {
            printHelp();
            System.out.println("Invalid -transport options provided, see output for valid options");
            System.exit(1);
        }
        return new SettingsTransport((TOptions) options);
    }

    public static void printHelp()
    {
        GroupedOptions.printOptions(System.out, "-transport", new TOptions());
    }

    public static Runnable helpPrinter()
    {
        return new Runnable()
        {
            @Override
            public void run()
            {
                printHelp();
            }
        };
    }

}
