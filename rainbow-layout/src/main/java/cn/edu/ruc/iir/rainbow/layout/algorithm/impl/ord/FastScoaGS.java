package cn.edu.ruc.iir.rainbow.layout.algorithm.impl.ord;

import cn.edu.ruc.iir.rainbow.common.exception.AlgoException;
import cn.edu.ruc.iir.rainbow.common.exception.ExceptionHandler;
import cn.edu.ruc.iir.rainbow.common.exception.ExceptionType;
import cn.edu.ruc.iir.rainbow.layout.domian.Column;

import java.util.List;

public class FastScoaGS extends FastScoa
{
    public static class RowGroupSize
    {
        public static final long MB64 = 1024l*1024l*64l;
        public static final long MB128 = 1024l*1024l*128l;
        public static final long MB256 = 1024l*1024l*256l;
        public static final long MB512 = 1024l*1024l*512l;
        public static final long MB1024 = 1024l*1024l*1024l;
        public static final long MB2048 = 1024l*1024l*2048l;

        // this is the amplification of in-memory data. for example,
        // the data on disk is 1MB, and the deserialized data in memory is 2MB, then the
        // amplification is 2.
        public static final double Amplification = 1.5;

        /**
         * this function ensures that all the slots can be used.
         * @param numMapSlots
         * @param totalMemory
         * @return
         * @throws AlgoException
         */
        public static long BestRowGroupSize (int numMapSlots, long totalMemory) throws AlgoException
        {
            if (numMapSlots <= 0)
            {
                throw new AlgoException("numMapSlots " + numMapSlots + " less then 1.");
            }
            long maxRowGroupSize = (long)(totalMemory / numMapSlots / Amplification);

            if (maxRowGroupSize < MB64)
            {
                throw new AlgoException("with numMapSlots " + numMapSlots + " and totalMemory " + totalMemory +
                        ", the row group size is less then 64MB");
            }

            if (maxRowGroupSize >= MB2048)
            {
                return MB2048;
            }
            if (maxRowGroupSize >= MB1024)
            {
                return MB1024;
            }
            if (maxRowGroupSize >= MB512)
            {
                return MB512;
            }
            if (maxRowGroupSize >= MB256)
            {
                return MB256;
            }
            if (maxRowGroupSize >= MB128)
            {
                return MB128;
            }
            if (maxRowGroupSize >= MB64)
            {
                return MB64;
            }
            return MB64;
        }
    }

    private int taskInitMs = 0;

    private long rowGroupSize = 0;

    private long totalMemory = 0;

    private int numMapSlots = 0;

    private int numRowGroups = 0;

    public int getNumRowGroups()
    {
        return numRowGroups;
    }

    public void setNumRowGroups(int numRowGroups)
    {
        this.numRowGroups = numRowGroups;
    }

    public long getRowGroupSize()
    {
        return rowGroupSize;
    }

    public void setRowGroupSize(long rowGroupSize)
    {
        this.rowGroupSize = rowGroupSize;
    }

    public long getTotalMemory()
    {
        return totalMemory;
    }

    public void setTotalMemory(long totalMemory)
    {
        this.totalMemory = totalMemory;
    }

    public int getNumMapSlots()
    {
        return numMapSlots;
    }

    public void setNumMapSlots(int numMapSlots)
    {
        this.numMapSlots = numMapSlots;
    }

    public int getTaskInitMs()
    {
        return taskInitMs;
    }

    public void setTaskInitMs(int taskInitMs)
    {
        this.taskInitMs = taskInitMs;
    }

    public double getSchemaOverhead ()
    {
        return this.getNumRowGroups() * this.getWorkload().size() *  this.getTaskInitMs() +
                this.getSchemaSeekCost() * this.getNumRowGroups();
    }

    public double getCurrentOverhead ()
    {
        return this.getCurrentWorkloadSeekCost() * this.numRowGroups +
                this.getWorkload().size() * this.getTaskInitMs() * this.numRowGroups;
    }

    private void increaseRowGroupSize ()
    {
        this.rowGroupSize *= 2;
        List<Column> columnOrder = this.getColumnOrder();
        for (Column column : columnOrder)
        {
            column.setSize(column.getSize()*2);
        }
        this.numRowGroups = (this.numRowGroups+1)/2;
    }

    private void decreaseRowGroupSize ()
    {
        this.rowGroupSize /= 2;
        List<Column> columnOrder = this.getColumnOrder();
        for (Column column : columnOrder)
        {
            column.setSize(column.getSize()/2);
        }
        this.numRowGroups *= 2;
    }

    @Override
    public void runAlgorithm()
    {
        try
        {
            long bestRowGroupSize = RowGroupSize.BestRowGroupSize(this.getNumMapSlots(),
                    this.getTotalMemory());

            while (this.getRowGroupSize() > bestRowGroupSize)
            {
                this.decreaseRowGroupSize();
            }
            while (this.getRowGroupSize() < bestRowGroupSize)
            {
                this.increaseRowGroupSize();
            }
            this.currentEnergy = super.getCurrentWorkloadSeekCost();
            long startSeconds = System.currentTimeMillis() / 1000;
            for (long currentSeconds = System.currentTimeMillis() / 1000;
                 (currentSeconds - startSeconds) < this.getComputationBudget();
                 currentSeconds = System.currentTimeMillis() / 1000, ++this.iterations)
            {
                //generate two random indices
                int i = rand.nextInt(this.getColumnOrder().size());
                int j = i;
                while (j == i)
                    j = rand.nextInt(this.getColumnOrder().size());
                rand.setSeed(System.nanoTime());

                //calculate new cost
                double neighbourEnergy = getNeighbourSeekCost(i, j);

                //try to accept it
                double temperature = this.getTemperature();
                if (this.probability(currentEnergy, neighbourEnergy, temperature) > Math.random())
                {
                    currentEnergy = neighbourEnergy;
                    updateColumnOrder(i, j);
                }
            }
        } catch (AlgoException e)
        {
            ExceptionHandler.Instance().log(ExceptionType.ERROR,
                    "algo error when running fastscoa with group size optimization.", e);
        }
    }
}
