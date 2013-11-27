package org.apache.cassandra.stress.settings;

import java.util.Collections;
import java.util.List;
import java.util.regex.Pattern;

/**
 * For parsing a simple (sub)option for a command/major option
 */
class OptionSimple extends Option
{

    final String displayPrefix;
    final Pattern matchPrefix;
    final String defaultValue;
    final Pattern pattern;
    final String description;
    final boolean required;
    String value;

    public OptionSimple(String prefix, String valuePattern, String defaultValue, String description, boolean required)
    {
        this.displayPrefix = prefix;
        this.matchPrefix = Pattern.compile(Pattern.quote(prefix), Pattern.CASE_INSENSITIVE);
        this.pattern = Pattern.compile(valuePattern, Pattern.CASE_INSENSITIVE);
        this.defaultValue = defaultValue;
        this.description = description;
        this.required = required;
    }

    public OptionSimple(String displayPrefix, Pattern matchPrefix, Pattern valuePattern, String defaultValue, String description, boolean required)
    {
        this.displayPrefix = displayPrefix;
        this.matchPrefix = matchPrefix;
        this.pattern = valuePattern;
        this.defaultValue = defaultValue;
        this.description = description;
        this.required = required;
    }

    public boolean setByUser()
    {
        return value != null;
    }

    public boolean present()
    {
        return value != null || defaultValue != null;
    }

    public String value()
    {
        return value != null ? value : defaultValue;
    }

    public boolean accept(String param)
    {
        if (matchPrefix.matcher(param).lookingAt())
        {
            if (value != null)
                throw new IllegalArgumentException("Suboption " + displayPrefix + " has been specified more than once");
            String v = param.substring(displayPrefix.length());
            if (!pattern.matcher(v).matches())
                throw new IllegalArgumentException("Invalid option " + param + "; must match pattern " + pattern);
            value = v;
            return true;
        }
        return false;
    }

    @Override
    public boolean happy()
    {
        return !required || value != null;
    }

    public String shortDisplay()
    {
        StringBuilder sb = new StringBuilder();
        if (!required)
            sb.append("[");
        sb.append(displayPrefix);
        if (displayPrefix.endsWith("="))
            sb.append("?");
        if (displayPrefix.endsWith("<"))
            sb.append("?");
        if (displayPrefix.endsWith(">"))
            sb.append("?");
        if (!required)
            sb.append("]");
        return sb.toString();
    }

    public String longDisplay()
    {
        if (description.equals("") && defaultValue == null && pattern.pattern().equals(""))
            return null;
        StringBuilder sb = new StringBuilder();
        sb.append(displayPrefix);
        if (displayPrefix.endsWith("="))
            sb.append("?");
        if (displayPrefix.endsWith("<"))
            sb.append("?");
        if (displayPrefix.endsWith(">"))
            sb.append("?");
        if (defaultValue != null)
        {
            sb.append(" (default=");
            sb.append(defaultValue);
            sb.append(")");
        }
        return GroupedOptions.formatLong(sb.toString(), description);
    }

    public List<String> multiLineDisplay()
    {
        return Collections.emptyList();
    }

    public int hashCode()
    {
        return displayPrefix.hashCode();
    }

    @Override
    public boolean equals(Object that)
    {
        return that instanceof OptionSimple && ((OptionSimple) that).displayPrefix.equals(this.displayPrefix);
    }

}
