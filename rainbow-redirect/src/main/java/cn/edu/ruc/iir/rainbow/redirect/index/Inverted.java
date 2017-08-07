package cn.edu.ruc.iir.rainbow.redirect.index;

import cn.edu.ruc.iir.rainbow.common.exception.ColumnOrderException;
import cn.edu.ruc.iir.rainbow.common.exception.ExceptionHandler;
import cn.edu.ruc.iir.rainbow.common.exception.ExceptionType;
import cn.edu.ruc.iir.rainbow.redirect.domain.AccessPattern;
import cn.edu.ruc.iir.rainbow.redirect.domain.ColumnSet;

import java.util.*;

public class Inverted implements Index
{
    /**
     * key: column name;
     * value: bit map
     */
    private Map<String, BitSet> bitMapIndex = null;

    private List<AccessPattern> queryAccessPatterns = null;

    private List<String> columnOrder = null;

    public Inverted (List<String> columnOrder, List<AccessPattern> patterns)
    {
        this.columnOrder = new ArrayList<>(columnOrder);
        this.queryAccessPatterns = new ArrayList<>(patterns);
        // filters column replicas.
        ColumnSet fullColumnSet = ColumnSet.toColumnSet(this.columnOrder);
        this.bitMapIndex = new HashMap<>(fullColumnSet.size());

        for (String column : fullColumnSet.toArrayList())
        {
            BitSet bitMap = new BitSet(this.queryAccessPatterns.size());
            for (int i = 0; i < this.queryAccessPatterns.size(); ++i)
            {
                if (this.queryAccessPatterns.get(i).contaiansColumn(column))
                {
                    bitMap.set(i, true);
                }
            }
            this.bitMapIndex.put(column, bitMap);
        }
    }

    @Override
    public AccessPattern search(ColumnSet columnSet)
    {
        List<String> columns = columnSet.toArrayList();
        List<BitSet> bitMaps = new ArrayList<>();
        BitSet and = new BitSet(this.queryAccessPatterns.size());
        and.set(0, this.queryAccessPatterns.size(), true);
        for (String column : columns)
        {
            BitSet bitMap = this.bitMapIndex.get(column);
            bitMaps.add(bitMap);
            and.and(bitMap);
        }

        AccessPattern bestPattern = null;
        if (and.nextSetBit(0) < 0)
        {
            // no exact access pattern found.
            int[] counts = new int[this.queryAccessPatterns.size()];
            for (BitSet bitMap : bitMaps)
            {
                int i = 0;
                while ((i = bitMap.nextSetBit(i)) >= 0)
                {
                    counts[i]++;
                }
            }
            int maxCount = -1;
            int minPatternSize = Integer.MAX_VALUE;

            for (int i = 0; i < counts.length; ++i)
            {
                if (counts[i] > maxCount ||
                        ((counts[i] == maxCount) && (this.queryAccessPatterns.get(i).size() < minPatternSize)))
                {
                    maxCount = counts[i];
                    bestPattern = this.queryAccessPatterns.get(i);
                    minPatternSize = bestPattern.size();
                }
            }

            try
            {
                bestPattern = bestPattern.generatePattern(columnSet, this.columnOrder);
            } catch (ColumnOrderException e)
            {
                ExceptionHandler.Instance().log(ExceptionType.ERROR,
                        "error occurs when generating best pattern", e);
            }
        }
        else
        {
            int minPatternSize = Integer.MAX_VALUE;
            int i =0;
            while ((i = and.nextSetBit(i)) >= 0)
            {
                if (this.queryAccessPatterns.get(i).size() < minPatternSize)
                {
                    bestPattern = this.queryAccessPatterns.get(i);
                    minPatternSize = bestPattern.size();
                }
            }
        }

        return bestPattern;
    }
}
