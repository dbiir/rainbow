package cn.edu.ruc.iir.rainbow.redirect.index;

import cn.edu.ruc.iir.rainbow.redirect.domain.AccessPattern;
import cn.edu.ruc.iir.rainbow.redirect.domain.ColumnSet;

import java.util.*;

public class Inverted implements Index
{
    /**
     * key: column name;
     * value: bit map
     */
    private Map<String, BitSet> bitMapIndex = new HashMap<>();

    private List<AccessPattern> queryAccessPatterns = new ArrayList<>();

    public Inverted (List<AccessPattern> patterns)
    {
        this.queryAccessPatterns = new ArrayList<>(queryAccessPatterns);
        // TODO: build the index
    }

    @Override
    public AccessPattern search(ColumnSet columnSet)
    {
        // TODO: implement
        return null;
    }
}
