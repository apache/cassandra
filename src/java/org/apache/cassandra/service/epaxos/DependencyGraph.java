package org.apache.cassandra.service.epaxos;

import java.util.*;

/**
 * Used by the execution stage to build up a directed (sometimes cyclic) graph,
 * which is then sorted to determine execution order.
 *
 * DependencyGraph is not thread safe, and getExecutionOrder should only be called once
 */
public class DependencyGraph
{
    private class Vertex
    {
        public final UUID id;
        public final Set<UUID> out;
        public int index = 0;
        public int lowlink = 0;
        public boolean stacked = false;
        public boolean visited = false;

        private Vertex(UUID id, Set<UUID> out)
        {
            this.id = id;
            this.out = out;
        }
    }

    private Map<UUID, Vertex> graph = new HashMap<>();
    private List<Vertex> vertices = new LinkedList<>();

    // bookkeeping for the tarjan sort
    private Stack<Vertex> stack = new Stack<>();
    private int idx = 0;

    // reverse topsorted strongly connected components
    private List<List<UUID>> scc = new LinkedList<>();

    private boolean used = false;

    static final Comparator<UUID> comparator = new Comparator<UUID>()
    {
        @Override
        public int compare(UUID o1, UUID o2)
        {
            long tsDiff = o1.timestamp() - o2.timestamp();
            if (tsDiff != 0)
                return (int) tsDiff;

            return o1.compareTo(o2);
        }
    };

    public void addVertex(UUID id, Set<UUID> out)
    {
        Vertex vertex = new Vertex(id, out);
        graph.put(id, vertex);
        vertices.add(vertex);
    }


    private void strongConnect(Vertex v)
    {
        // min index will be 1
        idx++;

        v.index = idx;
        v.lowlink = idx;
        v.stacked = true;
        v.visited = true;

        int initialStackSize = stack.size();
        stack.push(v);

        for (UUID vid: v.out)
        {
            Vertex w = graph.get(vid);

            // older instances aren't put into the
            // graph, so nulls are just ignored
            if (w == null)
                continue;

            if (!w.visited)
            {
                // vertex hasn't been visited yet
                strongConnect(w);

                // if w's lowlink is lower than v, they're
                // part of the same scc
                v.lowlink = Math.min(v.lowlink, w.lowlink);
            }
            else if (w.stacked)
            {
                // w is already in the stack, so v & w are part
                // of the same strongly connected component
                v.lowlink = Math.min(v.lowlink, w.lowlink);
            }
        }

        // if v is the root for the current component
        if (v.index == v.lowlink)
        {
            // pop vertices off the stack until we get to
            // the current vertex, and add them to the new
            // strongly connected component
            List<UUID> component = new ArrayList<>(stack.size() - initialStackSize);
            Vertex w;
            do
            {
                w = stack.pop();
                w.stacked = false;
                component.add(w.id);
            } while (v != w);
            scc.add(component);
        }
    }

    /**
     * Reverse topsorts the strongly connected components using
     * Tarjan's algorithm, then sorts the strongly connected
     * components by the instance id.
     *
     * Don't call this more than once.
     */
    public List<List<UUID>> getExecutionOrder()
    {
        assert !used;
        used = true;

        for (Vertex v: vertices)
            if (!v.visited)
                strongConnect(v);

        // sort the strongly connected components
        for (List<UUID> component: scc)
            Collections.sort(component, comparator);
        return scc;
    }

    public boolean contains(UUID iid)
    {
        return graph.containsKey(iid);
    }
}
