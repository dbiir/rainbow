package cn.edu.ruc.iir.rainbow.layout.algorithm;

import cn.edu.ruc.iir.rainbow.layout.domian.WorkloadPattern;

/**
 * Created by hank on 17-4-7.
 *
 * This is the supper class for duplication algorithm
 */
public abstract class DupAlgorithm extends Algorithm
{
    public abstract WorkloadPattern getWorkloadPattern ();
}
