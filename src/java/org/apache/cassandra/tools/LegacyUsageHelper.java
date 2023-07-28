package org.apache.cassandra.tools;

import com.github.rvesse.airline.model.ArgumentsMetadata;
import com.github.rvesse.airline.model.CommandMetadata;
import com.github.rvesse.airline.model.OptionMetadata;
import com.google.common.collect.ComparisonChain;

import java.util.Comparator;
import java.util.List;
import java.util.Set;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.stream.Collectors.joining;

public class LegacyUsageHelper
{
    public static final Comparator<OptionMetadata> DEFAULT_OPTION_COMPARATOR = new Comparator<OptionMetadata>()
    {
        @Override
        public int compare(OptionMetadata o1, OptionMetadata o2)
        {
            String option1 = o1.getOptions().iterator().next();
            option1 = option1.replaceFirst("^-+", "");

            String option2 = o2.getOptions().iterator().next();
            option2 = option2.replaceFirst("^-+", "");

            return ComparisonChain.start()
                    .compare(option1.toLowerCase(), option2.toLowerCase())
                    .compare(option2, option1) // print lower case letters before upper case
                    .compare(System.identityHashCode(o1), System.identityHashCode(o2))
                    .result();
        }
    };
    public static final Comparator<CommandMetadata> DEFAULT_COMMAND_COMPARATOR = new Comparator<CommandMetadata>()
    {
        @Override
        public int compare(CommandMetadata o1, CommandMetadata o2)
        {
            return ComparisonChain.start()
                    .compare(o1.getName().toLowerCase(), o2.getName().toLowerCase())
                    .compare(o2.getName(), o1.getName()) // print lower case letters before upper case
                    .compare(System.identityHashCode(o1), System.identityHashCode(o2))
                    .result();
        }
    };

    public static String toDescription(OptionMetadata option)
    {
        return optionString(option, ", ");
    }

    private static String optionString(OptionMetadata option, String delimiter)
    {
        String argument = (option.getArity() > 0) ? ("<" + option.getTitle() + ">") : null;

        return option.getOptions().stream()
                .map(value -> {
                    if (argument != null) {
                        return value + " " + argument;
                    }
                    return value;
                })
                .collect(joining(delimiter));
    }

    public static String toUsage(OptionMetadata option)
    {
        Set<String> options = option.getOptions();
        boolean required = option.isRequired();
        StringBuilder stringBuilder = new StringBuilder();
        if (!required) {
            stringBuilder.append('[');
        }

        if (options.size() > 1) {
            stringBuilder.append('(');
        }

        stringBuilder.append(optionString(option, " | "));

        if (options.size() > 1) {
            stringBuilder.append(')');
        }

        if (option.isMultiValued()) {
            stringBuilder.append("...");
        }

        if (!required) {
            stringBuilder.append(']');
        }
        return stringBuilder.toString();
    }

    public static String toUsage(ArgumentsMetadata arguments) {
        boolean required = arguments.isRequired();
        StringBuilder stringBuilder = new StringBuilder();
        if (!required) {
            stringBuilder.append("[");
        }

        stringBuilder.append(toDescription(arguments));
        if (arguments.isMultiValued()) {
            stringBuilder.append("...");
        }

        if (!required) {
            stringBuilder.append("]");
        }

        return stringBuilder.toString();
    }

    public static String toDescription(ArgumentsMetadata arguments) {
        List<String> descriptionTitles = arguments.getTitle();
        StringBuilder stringBuilder = new StringBuilder();
        for (String title : descriptionTitles) {
            if (stringBuilder.length() > 0) {
                stringBuilder.append(" ");
            }
            stringBuilder.append("<");
            stringBuilder.append(title);
            stringBuilder.append(">");
        }

        return stringBuilder.toString();
    }

    public static List<String> toSynopsisUsage(List<OptionMetadata> options)
    {
        return options.stream()
                .filter(input -> !input.isHidden())
                .map(LegacyUsageHelper::toUsage)
                .collect(toImmutableList());
    }
}
