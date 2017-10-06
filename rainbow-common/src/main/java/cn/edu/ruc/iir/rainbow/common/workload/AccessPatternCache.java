package cn.edu.ruc.iir.rainbow.common.workload;

import cn.edu.ruc.iir.rainbow.common.exception.ExceptionHandler;
import cn.edu.ruc.iir.rainbow.common.exception.ExceptionType;
import cn.edu.ruc.iir.rainbow.common.util.OutputFactory;

import java.io.BufferedWriter;
import java.io.IOException;
import java.util.*;

public class AccessPatternCache
{
    private Set<AccessPattern> APC = null;
    private Set<AccessPattern> prevAPC = null;
    private int updateCounter = 0;
    private int prevSize = 0;
    private long LifeTime;
    private final double Threshold;

    public AccessPatternCache(long lifeTime, double threshold)
    {
        this.LifeTime = lifeTime;
        this.Threshold = threshold;
        this.APC = new HashSet<>();
    }

    public void saveAsWorkloadFile (String path)
    {
        try (BufferedWriter writer = OutputFactory.Instance().getWriter(path))
        {
            for (AccessPattern pattern : this.APC)
            {
                writer.write(pattern.toString());
                writer.newLine();
            }
            writer.flush();
        } catch (IOException e)
        {
            ExceptionHandler.Instance().log(ExceptionType.ERROR, "I/O error when saving workload file.", e);
        }
    }

    public boolean cache(AccessPattern pattern)
    {
        long timeStamp = System.currentTimeMillis();
        SortedSet<AccessPattern> sortedAPC = new TreeSet<>(this.APC);
        if (this.APC.contains(pattern))
        {
            // hits
            AccessPattern hit = sortedAPC.tailSet(pattern).first();
            hit.setTimeStamp(timeStamp);
            hit.incrementWeight();
        }
        else
        {
            // insert
            pattern.setTimeStamp(timeStamp);
            this.APC.add(pattern);
            sortedAPC.add(pattern);
            this.updateCounter ++;

            AccessPattern APe = sortedAPC.first();
            if (APe.getTimeStamp() < timeStamp - this.LifeTime)
            {
                if (prevSize == 0)
                {
                    this.updateCounter = 0;
                    prevSize = this.APC.size();
                    System.out.println(this.updateCounter + ", * " + this.prevSize);
                    this.prevAPC = new HashSet<>(this.APC);
                    return true;
                }
                this.APC.remove(APe);
                this.updateCounter ++;
            }
        }

        if (this.prevSize > 0 && 1.0*this.updateCounter/this.prevSize > this.Threshold)
        {
            double sim = 0;
            for (AccessPattern p1 : this.prevAPC)
            {
                if (this.APC.contains(p1))
                {
                    sim++;
                }
            }
            sim /= this.APC.size();
            //System.out.println(LifeTime);
            //System.out.println(sim);
            //System.out.println(this.updateCounter + ", " + this.prevSize);
            this.updateCounter = 0;
            this.prevSize = this.APC.size();
            this.prevAPC = new HashSet<>(this.APC);
            //--------------------------
            // this tries to adjust LifeTime adaptively.
            if (sim > 0.93)
            {
                this.LifeTime *= 1.2;
                return false;
            }
            else if (sim < 0.905)
            {
                this.LifeTime *= 0.8;
            }
            //--------------------------

            return true;
        }
        sortedAPC.clear();
        return false;
    }
}
