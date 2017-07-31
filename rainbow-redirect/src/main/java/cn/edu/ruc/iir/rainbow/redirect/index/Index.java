package cn.edu.ruc.iir.rainbow.redirect.index;

import cn.edu.ruc.iir.rainbow.redirect.domain.AccessPattern;
import cn.edu.ruc.iir.rainbow.redirect.domain.ColumnSet;

public interface Index
{
    /**
     * search viable access pattern for a column set
     * @param columnSet
     * @return
     */
    public AccessPattern search (ColumnSet columnSet);
}
