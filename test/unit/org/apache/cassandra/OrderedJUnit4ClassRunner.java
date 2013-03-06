package org.apache.cassandra;

import org.junit.runners.BlockJUnit4ClassRunner;
import org.junit.runners.model.FrameworkMethod;
import org.junit.runners.model.InitializationError;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class OrderedJUnit4ClassRunner extends BlockJUnit4ClassRunner
{

    public OrderedJUnit4ClassRunner(Class aClass) throws InitializationError
    {
        super(aClass);
    }

    @Override
    protected List<FrameworkMethod> computeTestMethods()
    {
        final List<FrameworkMethod> list = super.computeTestMethods();
        try
        {
            final List<FrameworkMethod> copy = new ArrayList<FrameworkMethod>(list);
            Collections.sort(copy, MethodComparator.getFrameworkMethodComparatorForJUnit4());
            return copy;
        }
        catch (Throwable throwable)
        {
            return list;
        }
    }
}