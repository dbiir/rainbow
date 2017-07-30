package cn.edu.ruc.iir.rainbow.redirect.index;

import cn.edu.ruc.iir.rainbow.redirect.domain.AccessPattern;

import java.util.*;

public class Inverted
{
    /**
     * key: column name;
     * value: bit map
     */
    private Map<String, BitSet> bitMapIndex = new HashMap<>();

    private List<AccessPattern> queryAccessPatterns = new ArrayList<>();
}
