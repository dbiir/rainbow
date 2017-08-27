package cn.edu.ruc.iir.rainbow.layout.algorithm.impl.dup;

import cn.edu.ruc.iir.rainbow.common.exception.AlgoException;
import cn.edu.ruc.iir.rainbow.common.exception.ExceptionHandler;
import cn.edu.ruc.iir.rainbow.common.exception.ExceptionType;
import cn.edu.ruc.iir.rainbow.common.util.ConfigFactory;
import cn.edu.ruc.iir.rainbow.common.util.LogFactory;
import cn.edu.ruc.iir.rainbow.layout.algorithm.AlgorithmFactory;
import cn.edu.ruc.iir.rainbow.layout.algorithm.DupAlgorithm;
import cn.edu.ruc.iir.rainbow.layout.algorithm.RefineAlgorithm;
import cn.edu.ruc.iir.rainbow.layout.domian.Column;
import cn.edu.ruc.iir.rainbow.layout.domian.Query;
import cn.edu.ruc.iir.rainbow.layout.domian.WorkloadPattern;
import cn.edu.ruc.iir.rainbow.layout.seekcost.SeekCostFunction;

import java.util.*;

/**
 * Algorithm designed by Wenbo on 2015/11/3.
 * Created by hank on 17-5-2.
 */
public class FastInsertionDup extends DupAlgorithm
{
    private List<Map<Integer, Integer>> queryAccessedColIdToPos = new ArrayList<>();
    private List<TreeSet<Integer>> queryAccessedPos = new ArrayList<>();
    private double[] startOffset;
    private double[] endOffset;
    private List<Integer> colIdsOriginal = new ArrayList<>();
    private List<Integer> colIdsSelected = new ArrayList<>();
    private Map<Integer, Double> colIdToSize = new HashMap<>();
    private double[] columnDupGain;
    private int[] columnDupPos;
    private double[] delta1;
    private double[] delta2;

    private double initialSeekCost = 0.0;

    private WorkloadPattern workloadPattern = null;

    // configurations
    private int selectStride = 10;
    //private int refineFreq = 5;
    private int refineStride = 10;
    //private int refineBudget = 800;
    private int refineBudget = 200;
    //private int maxDupedColumnNum = 250;
    private int maxDupedColumnNum = 200;
    private double headroom = 0.05;
    //private int candidateColumnNum = 300;
    private int candidateColumnNum = 200;
    private int threadNum = 2;

    @Override
    public WorkloadPattern getWorkloadPattern()
    {
        return this.workloadPattern;
    }

    @Override
    public boolean isMultiThreaded()
    {
        return false;
    }

    /**
     * setup configurations for the algorithm.
     */
    @Override
    public void setup()
    {
        this.setColumnOrder(new ArrayList<>(this.getSchema()));
        this.maxDupedColumnNum = Integer.parseInt(ConfigFactory.Instance().getProperty("dup.max.duped.columns"));
        this.headroom = Double.parseDouble(ConfigFactory.Instance().getProperty("dup.storage.headroom"));
        this.refineBudget = Integer.parseInt(ConfigFactory.Instance().getProperty("refine.budget"));
        this.candidateColumnNum = Integer.parseInt(ConfigFactory.Instance().getProperty("insertion.candidate.column.num"));
        this.threadNum = Integer.parseInt(ConfigFactory.Instance().getProperty("refine.thread.num"));
        this.selectStride = Integer.parseInt(ConfigFactory.Instance().getProperty("insertion.select.stride"));
        this.refineStride = Integer.parseInt(ConfigFactory.Instance().getProperty("insertion.refine.stride"));

        this.columnDupGain = new double[this.getColumnOrder().size()];
        this.columnDupPos = new int[this.getColumnOrder().size()];

        int columnNum = this.getColumnOrder().size();
        int queryNum = this.getWorkload().size();
        List<Query> queries = this.getWorkload();
        List<Column> columns = this.getColumnOrder();

        //make colIds & colIdToSize
        for (int i = 0; i < columnNum; i ++)
        {
            this.colIdsOriginal.add(columns.get(i).getId());
            this.colIdToSize.put(columns.get(i).getId(), columns.get(i).getSize());
        }

        this.colIdsSelected = this.colIdsOriginal;

        //
        Map<Integer, Integer> colIdToPosMap = new HashMap<>();
        for (int i = 0; i < columnNum; i ++)
        {
            colIdToPosMap.put(columns.get(i).getId(), i);
        }

        //
        for (int i = 0; i < queryNum; i ++)
        {
            this.queryAccessedColIdToPos.add(new HashMap<>());
            this.queryAccessedPos.add(new TreeSet<>());
            Query query = queries.get(i);
            for (int colId : query.getColumnIds())
            {
                this.queryAccessedColIdToPos.get(i).put(colId, colIdToPosMap.get(colId));
                this.queryAccessedPos.get(i).add(colIdToPosMap.get(colId));
            }
        }

        //
        this.startOffset = new double[columnNum + this.maxDupedColumnNum];
        this.endOffset = new double[columnNum + this.maxDupedColumnNum];
        for (int i = 0; i < columnNum; i++)
        {
            this.startOffset[i] = (i == 0 ? 0 :
                    this.startOffset[i - 1] + columns.get(i - 1).getSize());
            this.endOffset[i] = (i == 0 ? columns.get(0).getSize() :
                    this.endOffset[i - 1] + columns.get(i).getSize());
        }

        //initialize delta1 & delta2
        this.delta1 = new double[queryNum];
        this.delta2 = new double[queryNum];

        //get initial seek cost
        this.initialSeekCost = this.getCurrentWorkloadSeekCost();
    }

    /**
     * release resources and collect results.
     */
    @Override
    public void cleanup()
    {
        this.workloadPattern = new WorkloadPattern();

        List<Column> columns = this.getColumnOrder();
        for (int i = 0; i < this.getWorkload().size(); ++i)
        {
            Query query = this.getWorkload().get(i);
            Map<Integer, Integer> colIdToPosMap = this.queryAccessedColIdToPos.get(i);
            Set<Integer> colIds = query.getColumnIds();
            Set<Column> accessedColumns = new HashSet<>();
            for (int colId : colIds)
            {
                int pos = colIdToPosMap.get(colId);
                accessedColumns.add(columns.get(pos));
            }
            this.workloadPattern.setPattern(query, accessedColumns);
        }
    }

    /**
     * get the seek cost of a query (on the given column order).
     * this is a general function, sub classes can override it.
     *
     * @param columnOrder
     * @param query
     * @return
     */
    @Override
    protected double getQuerySeekCost(List<Column> columnOrder, Query query)
    {
        int lastPos = -1;
        int queryIndex = this.getWorkload().indexOf(query);
        double seekCost = 0;
        SeekCostFunction sc = this.getSeekCostFunction();
        for (int pos : this.queryAccessedPos.get(queryIndex))
        {
            if (lastPos != -1)
            {
                seekCost += sc.calculate(this.startOffset[pos] - this.endOffset[lastPos]);
            }
            lastPos = pos;
        }
        return seekCost;
    }

    @Override
    public void runAlgorithm()
    {
        LogFactory.Instance().getLog().info(
                "[FastInsertionDup] Duplicating... Initial workload seek cost: " + this.initialSeekCost);
        //get total size
        double totalSize = 0;
        for (int i = 0; i < this.getColumnOrder().size(); i ++)
        {
            totalSize += this.getColumnOrder().get(i).getSize();
        }

        //duplicated columns, overall gain, sum of used Volume
        List<Integer> dupedColIds = new ArrayList<>();
        double overallGain = 0;
        double usedVolume = 0;

        //duplication
        while (dupedColIds.size() < this.maxDupedColumnNum && usedVolume < this.headroom * totalSize)
        {
            // select the columns for duplication
            if (dupedColIds.size() % selectStride == 0)
            {
                this.selectColumnsForDup();
            }

            long start = System.currentTimeMillis();

            this.calculateGainOfSelectedColumns();

            // find the best column to duplicate
            double maxGain = -1e30;
            int bestColIdIndex = -1;
            for (int i = 0; i < this.colIdsSelected.size(); i ++)
            {
                double columnSize = this.colIdToSize.get(this.colIdsSelected.get(i));
                if (columnSize > totalSize)
                {
                    continue;
                }
                if (this.columnDupGain[i] / columnSize > maxGain)
                {
                    maxGain = this.columnDupGain[i] / columnSize;
                    bestColIdIndex = i;
                }
            }

            // perform duplication of the best column
            if (bestColIdIndex == -1 || maxGain < 1e-6)
            {
                LogFactory.Instance().getLog().info("[FastInsertionDup] No more gains, quit...");
                System.out.println("[FastInsertionDup] No more gains, quit...");
                break;
            }
            dupedColIds.add(colIdsSelected.get(bestColIdIndex));
            overallGain += this.columnDupGain[bestColIdIndex];
            usedVolume += this.colIdToSize.get(this.colIdsSelected.get(bestColIdIndex));

            System.out.println("[FastInsertionDup] # duplicated columns: " + dupedColIds.size() +
                    "\n[FastInsertionDup] Used Volume: " + (usedVolume / totalSize * 100) + " %");
            System.out.println("[FastInsertionDup] gain: " + this.columnDupGain[bestColIdIndex] +
                    ", overall gain: " + overallGain);

            // update everything
            System.out.println("[FastInsertionDup] column id: " + this.colIdsSelected.get(bestColIdIndex) +
                    ", position to duplicate: " + this.columnDupPos[bestColIdIndex]);
            updateEverything(this.colIdsSelected.get(bestColIdIndex), this.columnDupPos[bestColIdIndex]);

            System.out.println("[FastInsertionDup] Current workload seek cost: " + getCurrentWorkloadSeekCost());
            long end = System.currentTimeMillis();
            System.out.println("[FastInsertionDup] Cost time: " + ((end - start)/1000));
            System.out.println();


            // refine
            if (dupedColIds.size() > 0 && dupedColIds.size() % refineStride == 0)
            {
                double refineGain = 0;
                try
                {
                    refineGain = this.refine();
                } catch (AlgoException e)
                {
                    ExceptionHandler.Instance().log(ExceptionType.ERROR,
                            "refine algorithm construction error.", e);
                    break;
                } catch (ClassNotFoundException e)
                {
                    ExceptionHandler.Instance().log(ExceptionType.ERROR,
                            "refine algorithm not found.", e);
                    break;
                }
                overallGain += refineGain;
                LogFactory.Instance().getLog().info("[FastInsertionDup] Refine Gain: " + refineGain + ", Overall gain: " + overallGain);
            }

            if (dupedColIds.size() >= this.maxDupedColumnNum)
            {
                LogFactory.Instance().getLog().info("[FastInsertionDup] Max duped column num reached, quit...");
                System.out.println("[FastInsertionDup] Max duped column num reached, quit...");
                break;
            }

            if (usedVolume > this.headroom * totalSize)
            {
                LogFactory.Instance().getLog().info("[FastInsertionDup] Max storage headroom reached, quit...");
                System.out.println("[FastInsertionDup] Max storage headroom reached, quit...");
                break;
            }
        }
        LogFactory.Instance().getLog().info("[FastInsertionDup] Final workload seek cost: " + getCurrentWorkloadSeekCost());
    }

    /**
     *
     * @return the gain the refine.
     */
    private double refine() throws AlgoException, ClassNotFoundException
    {
        RefineAlgorithm refine = (RefineAlgorithm) AlgorithmFactory.Instance().getAlgorithm("refine", this.refineBudget, this.getColumnOrder(), this.getWorkload(), this.getSeekCostFunction());

        refine.setQueryAccessedPos(this.queryAccessedPos);
        double initCost, refineCost;
        refine.setup();
        initCost = refine.getCurrentWorkloadSeekCost();
        refine.runAlgorithm();
        refine.cleanup();

        List<Column> columns = this.getColumnOrder();
        // update colIdtoPos
        for (int i = 0; i < this.getWorkload().size(); i ++)
        {
            this.queryAccessedColIdToPos.set(i, new HashMap<>());
            for (int pos : this.queryAccessedPos.get(i))
            {
                this.queryAccessedColIdToPos.get(i).put(columns.get(pos).getId(), pos);
            }
        }

        //
        for (int i = 0; i < columns.size(); i++)
        {
            this.startOffset[i] = (i == 0 ? 0 :
                    this.startOffset[i - 1] + columns.get(i - 1).getSize());
            this.endOffset[i] = (i == 0 ? columns.get(0).getSize() :
                    this.endOffset[i - 1] + columns.get(i).getSize());
        }

        refineCost = this.getCurrentWorkloadSeekCost();
        return initCost - refineCost;
    }

    private void selectColumnsForDup()
    {
        //calculate gravity
        double[] gravity = new double[this.colIdsOriginal.size()];
        List<Column> columns = this.getColumnOrder();
        SeekCostFunction sc = this.getSeekCostFunction();
        for (int i = 0; i < this.getWorkload().size(); i ++)
        {
            TreeSet<Integer> accessedPos = this.queryAccessedPos.get(i);
            int lastPos = -1;
            for (int pos : accessedPos)
            {
                double seekcost;
                if (lastPos != -1)
                {
                    seekcost = sc.calculate(this.startOffset[pos] - this.endOffset[lastPos]);
                    gravity[columns.get(pos).getId()] += seekcost;
                    gravity[columns.get(lastPos).getId()] += seekcost;
                }
                lastPos = pos;
            }
        }

        //sort the column ids by gravity, descending
        this.colIdsSelected = this.colIdsOriginal;
        for (int i = 0; i < this.colIdsSelected.size(); i ++)
        {
            for (int j = i + 1; j < this.colIdsSelected.size(); j ++)
            {
                if (gravity[colIdsSelected.get(i)] < gravity[colIdsSelected.get(j)])
                {
                    int colId = colIdsSelected.get(i);
                    colIdsSelected.set(i, colIdsSelected.get(j));
                    colIdsSelected.set(j, colId);
                }
            }
        }

        //top 200
        this.colIdsSelected = this.colIdsSelected.subList(0, this.candidateColumnNum);
        System.out.println("[FastInsertionDup] Selected columns to duplicate:");
        for (int colId : colIdsSelected)
        {
            System.out.print(colId + " ");
        }
        System.out.println();
        System.out.println();
    }

    private void calculateGainOfSelectedColumns()
    {
        Thread[] threads = new Thread[this.threadNum];
        int selectedNum = colIdsSelected.size();
        int blockSize = selectedNum / this.threadNum;
        for (int i = 0; i < this.threadNum; i ++)
        {
            int lo = i * blockSize;
            int hi = (i + 1) * blockSize - 1;
            if (this.threadNum - 1 == i)
            {
                // this is the last block
                hi = selectedNum - 1;
            }
            threads[i] = new GainCalculator(lo, hi);
            threads[i].start();
        }
        for (int i = 0; i < this.threadNum; i ++)
        {
            try
            {
                threads[i].join();
            }
            catch (InterruptedException e)
            {
                e.printStackTrace();
            }
        }
    }

    private void updateEverything(int colIdToDup, int posToDup)
    {
        // update columns, add the dupped column into it.
        Column dupped = null;
        List<Column> columns = this.getColumnOrder();
        for (int i = 0; i < columns.size() - 1; i ++)
        {
            if (columns.get(i).getId() == colIdToDup)
            {
                Column origin = columns.get(i);
                dupped = new Column(origin.getId(), origin.getName(), origin.getType(), origin.getSize());
                break;
            }
        }
        columns.add(posToDup, dupped);
        // update the dupId of the duplicated columns.
        int dupId = 0;
        for (int i = 0; i < columns.size(); i ++)
        {
            if (columns.get(i).getId() == colIdToDup)
            {
                columns.get(i).setDuplicated(true);
                columns.get(i).setDupId(dupId ++);
            }
        }

        //update delta1 and delta2
        this.calculateDeltaCost(colIdToDup, posToDup);

        for (int i = 0; i < this.getWorkload().size(); i ++)
        {
            // for each query, update the accessed positions.
            TreeSet<Integer> positions = this.queryAccessedPos.get(i);
            List<Integer> prevPositions = new ArrayList<>();
            for (int pos : positions)
            {
                prevPositions.add(pos);
            }
            positions = new TreeSet<>();
            for (int j = 0; j < prevPositions.size(); j ++)
            {
                if (prevPositions.get(j) < posToDup)
                {
                    positions.add(prevPositions.get(j));
                }
                else
                {
                    positions.add(prevPositions.get(j) + 1);
                }
            }
            if (delta2[i] < delta1[i])
            {
                for (int pos : positions)
                {
                    if (columns.get(pos).getId() == colIdToDup)
                    {
                        positions.remove(pos);
                        break;
                    }
                }
                positions.add(posToDup);
            }
            this.queryAccessedPos.set(i, positions);

            // update the accessed columnId-to-position map of this query.
            Map<Integer, Integer> accessedColIdToPosMap = new HashMap<>();
            for (int pos : positions)
            {
                accessedColIdToPosMap.put(columns.get(pos).getId(), pos);
            }
            this.queryAccessedColIdToPos.set(i, accessedColIdToPosMap);
        }

        //update start offset and end offset of each column
        for (int i = 0; i < columns.size(); i++)
        {
            this.startOffset[i] = (i == 0 ? 0 :
                    this.startOffset[i - 1] + columns.get(i - 1).getSize());
            this.endOffset[i] = (i == 0 ? columns.get(0).getSize() :
                    this.endOffset[i - 1] + columns.get(i).getSize());
        }
    }

    /**
     *
     * @param colIdToDup
     * @param posToDup
     * @return
     */
    private double calculateDeltaCost(int colIdToDup, int posToDup)
    {
        double delta = 0;
        SeekCostFunction sc = this.getSeekCostFunction();
        for (int i = 0; i < this.getWorkload().size(); i++)
        {
            // for each query,
            double d1 = 0, d2 = 1e40;
            // d1 is delta1
            // get the previous accessed positions, exclude the position for the duplicated column.
            SortedSet head = this.queryAccessedPos.get(i).headSet(posToDup, false);
            // get the successive accessed positions, include the position for the duplicated column.
            SortedSet tail = this.queryAccessedPos.get(i).tailSet(posToDup, true);
            int prevPos = (head.isEmpty() ? -1 : (int) head.last());
            int succPos = (tail.isEmpty() ? -1 : (int) tail.first());
            // if prev or succ is less than 0, the duplicated column is useless for this query.
            if (prevPos >= 0 && succPos >= 0)
            {
                d1 = -sc.calculate(this.startOffset[succPos] - this.endOffset[prevPos])
                        + sc.calculate(this.startOffset[succPos] -
                        this.endOffset[prevPos] + colIdToSize.get(colIdToDup));
            }

            //d2 is delta2
            if (this.getWorkload().get(i).getColumnIds().contains(colIdToDup))
            {
                // if this query can access the duplicated column.
                d2 = 0;
                int originPos = this.queryAccessedColIdToPos.get(i).get(colIdToDup);

                if (prevPos == originPos)
                {
                    head = this.queryAccessedPos.get(i).headSet(prevPos, false);
                    prevPos = (head.isEmpty() ? -1 : (int) head.last());
                }
                else if (succPos == originPos)
                {
                    tail = this.queryAccessedPos.get(i).tailSet(succPos, false);
                    succPos = (tail.isEmpty() ? -1 : (int) tail.first());
                }

                if (prevPos >= 0)
                {
                    d2 += sc.calculate(this.endOffset[posToDup - 1] - this.endOffset[prevPos]);
                }
                if (succPos >= 0)
                {
                    d2 += sc.calculate(this.startOffset[succPos] - this.startOffset[posToDup]);
                }
                if (prevPos >= 0 && succPos >= 0)
                {
                    d2 -= sc.calculate(this.startOffset[succPos] - this.endOffset[prevPos]);
                }

                head = this.queryAccessedPos.get(i).headSet(originPos, false);
                tail = this.queryAccessedPos.get(i).tailSet(originPos, false);
                prevPos = (head.isEmpty() ? -1 : (int) head.last());
                succPos = (tail.isEmpty() ? -1 : (int) tail.first());
                if (prevPos >= 0)
                {
                    d2 -= sc.calculate(this.startOffset[originPos] - this.endOffset[prevPos]);
                }
                if (succPos >= 0)
                {
                    d2 -= sc.calculate(this.startOffset[succPos] - this.endOffset[originPos]);
                }
                if (prevPos >= 0 && succPos >= 0)
                {
                    d2 += sc.calculate(this.startOffset[succPos] - this.endOffset[prevPos]);
                }
            }
            delta += Math.min(d1, d2) * this.getWorkload().get(i).getWeight();
            delta1[i] = d1;
            delta2[i] = d2;
        }
        return -delta;
    }

    public class GainCalculator extends Thread
    {
        private int lo, hi;

        public GainCalculator(int lo, int hi)
        {
            this.lo = lo;
            this.hi = hi;
        }

        public void run()
        {
            int columnNum = getColumnOrder().size();
            for (int i = lo; i <= hi; i++)
            {
                //record the maximum gain
                int colIdToDup = colIdsSelected.get(i);
                double maxGain = -1e30;
                for (int pos = 0; pos <= columnNum; pos++)
                {
                    double gain = calculateDeltaCost(colIdToDup, pos);
                    if (gain > maxGain)
                    {
                        maxGain = gain;
                        columnDupPos[i] = pos;
                    }
                }
                columnDupGain[i] = maxGain;
            }
        }
    }
}
