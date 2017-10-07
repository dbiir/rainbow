package cn.edu.ruc.iir.rainbow.layout.algorithm.impl.ord;

import cn.edu.ruc.iir.rainbow.common.exception.AlgoException;
import cn.edu.ruc.iir.rainbow.common.exception.ExceptionHandler;
import cn.edu.ruc.iir.rainbow.common.exception.ExceptionType;
import cn.edu.ruc.iir.rainbow.common.util.ConfigFactory;
import cn.edu.ruc.iir.rainbow.layout.domian.Column;
import cn.edu.ruc.iir.rainbow.layout.domian.Query;

import java.util.*;

public class FastScoaGSR extends FastScoa
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
        public static List<Long> FeasibleRowGroupSizes (int numMapSlots, long totalMemory) throws AlgoException
        {
            List<Long> res = new ArrayList<>();
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

            if (MB64 < maxRowGroupSize)
            {
                res.add(MB64);
            }
            if (MB128 < maxRowGroupSize)
            {
                res.add(MB128);
            }
            if (MB256 < maxRowGroupSize)
            {
                res.add(MB256);
            }
            if (MB512 < maxRowGroupSize)
            {
                res.add(MB512);
            }
            if (MB1024 < maxRowGroupSize)
            {
                res.add(MB1024);
            }
            if (MB2048 < maxRowGroupSize)
            {
                res.add(MB2048);
            }

            return res;
        }
    }

    private int taskInitMs = 0;

    private long rowGroupSize = 0;

    private long totalMemory = 0;

    private int numMapSlots = 0;

    private int numRowGroups = 0;

    private RoundState bestState = null;

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

    public RoundState getBestState ()
    {
        return this.bestState;
    }

    public double getSchemaOverhead ()
    {
        return this.getNumRowGroups() * this.getWorkload().size() *  this.getTaskInitMs() +
                this.getSchemaSeekCost() * this.getNumRowGroups();
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

    public class RoundState
    {
        private double totalOverhead = 0;
        private long rowGroupSize = 0;
        private int numRowGroup = 0;
        private List<Column> columnOrder = null;

        public RoundState (double totalOverhead, long rowGroupSize, int numRowGroup, List<Column> columnOrder)
        {
            this.totalOverhead = totalOverhead;
            this.rowGroupSize = rowGroupSize;
            this.numRowGroup = numRowGroup;
            this.columnOrder = new ArrayList<>();
            for (Column column : columnOrder)
            {
                this.columnOrder.add(column.clone());
            }
        }

        /**
         * total overhead is the sum of total seek cost and total task initialize cost.
         * @return
         */
        public double getTotalOverhead ()
        {
            return this.totalOverhead;
        }

        public long getRowGroupSize()
        {
            return rowGroupSize;
        }

        public int getNumRowGroup()
        {
            return numRowGroup;
        }

        public List<Column> getColumnOrder()
        {
            return columnOrder;
        }
    }

    @Override
    public void runAlgorithm()
    {
        try
        {
            List<Long> feasibleRowGroupSizes = RowGroupSize.FeasibleRowGroupSizes(this.getNumMapSlots(),
                    this.getTotalMemory());

            int roundNum = feasibleRowGroupSizes.size();

            while (this.getRowGroupSize() > feasibleRowGroupSizes.get(0))
            {
                this.decreaseRowGroupSize();
            }
            while (this.getRowGroupSize() < feasibleRowGroupSizes.get(0))
            {
                this.increaseRowGroupSize();
            }
            if (this.getRowGroupSize() > feasibleRowGroupSizes.get(0))
            {
                roundNum--;
            }

            long roundBudget = this.getComputationBudget() / roundNum;

            for (int r = 0; r < roundNum; ++r)
            {
                // this is a round of fast scoa.
                this.currentEnergy = super.getCurrentWorkloadSeekCost();
                long startSeconds = System.currentTimeMillis() / 1000;
                for (long currentSeconds = System.currentTimeMillis() / 1000;
                     (currentSeconds - startSeconds) < roundBudget;
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

                double totalSeekCost = this.getCurrentWorkloadSeekCost() * this.numRowGroups +
                        this.getWorkload().size() * this.getTaskInitMs() * this.numRowGroups;

                if (this.bestState == null || this.bestState.getTotalOverhead() > totalSeekCost)
                {
                    bestState = new RoundState(totalSeekCost, this.rowGroupSize,
                            this.numRowGroups, this.getColumnOrder());
                }

                //System.out.println("row group size: " + this.getRowGroupSize());
                //System.out.println("number of row groups: " + this.getNumRowGroups());
                //System.out.println("total seek cost: " + totalSeekCost);

                this.increaseRowGroupSize();

                // recover the origin column order.
                List<Column> schema = this.getSchema();
                List<Column> columnOrder = this.getColumnOrder();
                List<Column> originColumnOrder = new ArrayList<>();
                for (Column column : schema)
                {
                    originColumnOrder.add(columnOrder.get(columnOrder.indexOf(column)));
                }
                this.setColumnOrder(originColumnOrder);

                // recover the initial cooling rate and the initial temperature.
                String strCoolingRate = ConfigFactory.Instance().getProperty("scoa.cooling_rate");
                String strInitTemp = ConfigFactory.Instance().getProperty("scoa.init.temperature");
                if (strCoolingRate != null)
                {
                    this.coolingRate = Double.parseDouble(strCoolingRate);
                }
                if (strInitTemp != null)
                {
                    this.temperature = Double.parseDouble(strInitTemp);
                }
                // make initial columnId-columnIndex map
                Map<Integer, Integer> cidToCIdxMap = new HashMap<>();
                for (int i = 0; i < this.getColumnOrder().size(); i ++)
                {
                    cidToCIdxMap.put(this.getColumnOrder().get(i).getId(), i);
                }

                // build initial querys' accessed column index sets.
                for (int i = 0; i < this.getWorkload().size(); i ++)
                {
                    Query curQuery = this.getWorkload().get(i);
                    queryAccessedPos.add(new TreeSet<Integer>());
                    for (int colIds : curQuery.getColumnIds())
                    {
                        // add the column indexes to query i's tree set.
                        queryAccessedPos.get(i).add(cidToCIdxMap.get(colIds));
                    }
                }
            }

            this.setRowGroupSize(bestState.getRowGroupSize());
            this.setNumRowGroups(bestState.getNumRowGroup());
            this.setColumnOrder(bestState.getColumnOrder());
        } catch (AlgoException e)
        {
            ExceptionHandler.Instance().log(ExceptionType.ERROR,
                    "algo error when running fastscoa with group size optimization.", e);
        }
    }
}
