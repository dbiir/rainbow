package cn.edu.ruc.iir.rainbow.workload;

import java.util.HashMap;
import java.util.Map;

public class APCFactory
{
    private Map<String, AccessPatternCache> map = null;

    private APCFactory ()
    {
        this.map = new HashMap<>();
    }

    private static APCFactory instance = null;

    public static APCFactory Instance ()
    {
        if (instance == null)
        {
            instance = new APCFactory();
        }
        return instance;
    }

    public void put (String pipelineId, AccessPatternCache APC)
    {
        this.map.put(pipelineId, APC);
    }

    public AccessPatternCache get (String pipelineId)
    {
        return this.map.get(pipelineId);
    }
}
