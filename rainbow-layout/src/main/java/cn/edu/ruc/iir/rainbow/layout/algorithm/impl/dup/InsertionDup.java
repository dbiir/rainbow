package cn.edu.ruc.iir.rainbow.layout.algorithm.impl.dup;

import cn.edu.ruc.iir.rainbow.common.exception.ColumnNotFoundException;
import cn.edu.ruc.iir.rainbow.common.exception.ColumnOrderException;
import cn.edu.ruc.iir.rainbow.common.exception.ExceptionHandler;
import cn.edu.ruc.iir.rainbow.common.exception.ExceptionType;
import cn.edu.ruc.iir.rainbow.common.util.ConfigFactory;
import cn.edu.ruc.iir.rainbow.layout.algorithm.DupAlgorithm;
import cn.edu.ruc.iir.rainbow.layout.seekcost.DistanceCalculator;
import cn.edu.ruc.iir.rainbow.layout.domian.Column;
import cn.edu.ruc.iir.rainbow.layout.domian.Query;
import cn.edu.ruc.iir.rainbow.layout.domian.WorkloadPattern;

import java.util.*;

/**
 * Created by hank on 17-4-28.
 * This is the insertion based column duplication (algorithm 3) algorithm used in our paper.
 * Fast calculation of seek cost (similar to algorithm 2) is applied. But this algorithm is base on
 * generating a cloned neighbour state, all the data structures will be copied and then changed when
 * generating a neighbour state. So it is still not efficient.
 */
public class InsertionDup extends DupAlgorithm
{
    private double headroom = 0.05;
    private int maxDupedColumnNum = 1000;
    private WorkloadPattern workloadPattern = null;
    private DistanceCalculator distanceCalculator = null;
    private Map<Integer, Integer> columnIdToMaxDupIdMap = null;
    private List<Column> selectedColumns = null;
    private double usedVolume = 0.0;

    @Override
    public void setup()
    {
        this.setColumnOrder(new ArrayList<>(this.getSchema()));
        this.headroom = Double.parseDouble(ConfigFactory.Instance().getProperty("dup.storage.overhead"));
        this.maxDupedColumnNum = Integer.parseInt(ConfigFactory.Instance().getProperty("dup.max.duped.columns"));
        double initTotalSize = 0;
        // initialize the total size and column id to max dupId map.
        this.columnIdToMaxDupIdMap = new HashMap<>();
        for (Column column : this.getSchema())
        {
            initTotalSize += column.getSize();
            this.columnIdToMaxDupIdMap.put(column.getId(), column.getDupId());
        }
        // changes headroom from percentage to actual size
        this.headroom *= initTotalSize;

        // initialize the distance calculator
        try
        {
            this.distanceCalculator = new DistanceCalculator(this.getSchema());
        } catch (ColumnOrderException e)
        {
            ExceptionHandler.Instance().log(ExceptionType.ERROR,
                    "Column order error when initializing distance calculator", e);
        }

        // initialize the workload access pattern
        this.workloadPattern = new WorkloadPattern();
        this.workloadPattern.setSeekCost(0.0);
        for (Query query : this.getWorkload())
        {
            Set<Column> columnSet = new HashSet<>();
            for (Column column : this.getSchema())
            {
                if (query.hasColumnId(column.getId()))
                {
                    columnSet.add(column);
                }
            }
            double seekCost = evaluateSeekCost(columnSet, this.distanceCalculator);
            this.workloadPattern.setPattern(query,columnSet);
            this.workloadPattern.increaseSeekCost(seekCost * query.getWeight());
        }

        this.selectedColumns = new ArrayList<>();
        List<Double> gravities = new ArrayList<>();
        Map<Column, List<Query>> columnToQueries = new HashMap<>();
        Map<Integer, Column> idToColumn = new HashMap<>();
        //System.out.println(this.getColumnOrder().size());
        for (Column column : this.getColumnOrder())
        {
            idToColumn.put(column.getId(), column);
        }
        for (Query query : this.getWorkload())
        {
            for (int cid : query.getColumnIds())
            {
                Column column = idToColumn.get(cid);
                if (!columnToQueries.containsKey(column))
                {
                    List<Query> queries = new ArrayList<>();
                    queries.add(query);
                    columnToQueries.put(column, queries);
                }
                else
                {
                    columnToQueries.get(column).add(query);
                }
            }
        }
        // calculate the gravity of each column
        for (Column column : this.getColumnOrder())
        {
            if (! columnToQueries.containsKey(column))
            {
                continue;
            }
            List<Column> neighbors = new ArrayList<>();
            for (Query query : columnToQueries.get(column))
            {
                //for (int cid : query.getColumnIds())
                //{
                //    neighbors.add(idToColumn.get(cid));
                //}
                neighbors.addAll(this.workloadPattern.getColumnSet(query));
            }
            double gravity = 0;
            for (Column neighbor : neighbors)
            {
                try
                {
                    gravity += this.getSeekCostFunction().calculate(this.distanceCalculator.calculateDistance(column, neighbor));
                } catch (ColumnNotFoundException e)
                {
                    ExceptionHandler.Instance().log(ExceptionType.ERROR,
                            "Column order error when initializing distance calculator", e);
                }
            }
            gravities.add(gravity);
            this.selectedColumns.add(column);
        }
        for (int i = 0; i < this.selectedColumns.size()-1; ++i)
        {
            for (int j = i+1; j < this.selectedColumns.size(); ++j)
            {
                if (gravities.get(i) < gravities.get(j))
                {
                    double gravity = gravities.get(i);
                    gravities.set(i, gravities.get(j));
                    gravities.set(j, gravity);
                    Column column = this.selectedColumns.get(i);
                    this.selectedColumns.set(i, this.selectedColumns.get(j));
                    this.selectedColumns.set(j, column);
                }
            }
        }
        this.selectedColumns = this.selectedColumns.subList(0, this.maxDupedColumnNum);

        //for (int i = 0; i < this.maxDupedColumnNum; ++i)
        //{
        //    System.out.println(this.selectedColumns.get(i).getId() + ", " + gravities.get(i));
        //}
    }

    @Override
    public void cleanup()
    {

    }

    @Override
    public void runAlgorithm()
    {
        int dupedColumnNum = 0;
        while (this.usedVolume < this.headroom && dupedColumnNum < this.maxDupedColumnNum)
        {
            List<Double> maxReducedCosts = new ArrayList<>();
            List<Integer> bestPoses = new ArrayList<>();
            for (Column column : this.selectedColumns)
            {
                double maxReducedCost = 0.0;
                int bestPos = 0;
                for (int i = 0; i <= this.getColumnOrder().size(); ++i)
                {
                    //long start = System.nanoTime();
                    List<Query> redirectQueries = this.getRedirectQueries(column, this.getWorkload());
                    //long end = System.nanoTime();
                    //System.out.println("get redirect queries: " + (end-start)/1000000.0);
                    if (redirectQueries == null || redirectQueries.isEmpty())
                    {
                        break;
                    }
                    //start = System.nanoTime();
                    List<Column> candidateOrder = generateCandidateOrder(this.getColumnOrder(), column, i);
                    //end = System.nanoTime();
                    //System.out.println("generate candidate order: " + (end-start)/1000000.0);
                    //start = System.nanoTime();
                    DistanceCalculator candidateCalculator = generateCandidateCalculator(this.distanceCalculator, candidateOrder.get(i), i);
                    //end = System.nanoTime();
                    //System.out.println("generate candidate calculator: " + (end-start)/1000000.0);
                    //start = System.nanoTime();
                    WorkloadPattern candidatePattern = generateCandidatePattern(candidateCalculator, column, candidateOrder.get(i), redirectQueries, this.workloadPattern);
                    //end = System.nanoTime();
                    //System.out.println("generate candidate pattern: " + (end-start)/1000000.0);
                    //start = System.nanoTime();
                    candidatePattern.setSeekCost(this.calculateWorkloadSeekCost(candidateCalculator, candidatePattern));
                    //end = System.nanoTime();
                    //System.out.println("calculate workload seek cost: " + (end-start)/1000000.0);
                    double reducedCost = this.workloadPattern.getSeekCost() - candidatePattern.getSeekCost();

                    if (reducedCost > maxReducedCost)
                    {
                        if (reducedCost > maxReducedCost)
                        {
                            maxReducedCost = reducedCost;
                            bestPos = i;
                        }
                    }
                }
                System.out.println(maxReducedCost + ", " + bestPos);
                maxReducedCosts.add(maxReducedCost);
                bestPoses.add(bestPos);
            }
            double maxGain = 0.0;
            int bestPos = 0;
            int originPos = 0;
            for (int i = 0; i < this.selectedColumns.size(); ++i)
            {
                double gain = maxReducedCosts.get(i)/this.selectedColumns.get(i).getSize();
                if (gain > maxGain)
                {
                    maxGain = gain;
                    bestPos = bestPoses.get(i);
                    originPos = i;
                }
            }
            if (maxGain > 0)
            {
                System.out.println("originPos: " + originPos + ", bestPos: " + bestPos + ", maxGain: " + maxGain + ", usedVolume: " + usedVolume);
                Column originColumn = this.selectedColumns.get(originPos);
                List<Column> candidateOrder = this.generateCandidateOrder(this.getColumnOrder(), originColumn, bestPos);
                Column dupedColumn = candidateOrder.get(bestPos);
                this.usedVolume += originColumn.getSize();
                List<Query> redirectQueries = this.getRedirectQueries(originColumn, this.getWorkload());
                DistanceCalculator candidateCalculator = this.generateCandidateCalculator(this.distanceCalculator, dupedColumn, bestPos);
                WorkloadPattern candidatePattern = this.generateCandidatePattern(candidateCalculator, originColumn, dupedColumn, redirectQueries, this.workloadPattern);
                this.setColumnOrder(candidateOrder);
                this.distanceCalculator = candidateCalculator;
                this.workloadPattern = candidatePattern;
                this.workloadPattern.setSeekCost(this.calculateWorkloadSeekCost(this.distanceCalculator, this.workloadPattern));
                this.columnIdToMaxDupIdMap.put(originColumn.getId(), dupedColumn.getDupId());
                dupedColumnNum++;
                System.out.println("seek cost: " + this.workloadPattern.getSeekCost() + ", column id: " + originColumn.getId() +
                        ", max dupId: " + this.columnIdToMaxDupIdMap.get(originColumn.getId()));
            }
            else
            {
                // if there is no more gains
                break;
            }
        }
    }

    @Override
    public boolean isMultiThreaded()
    {
        return false;
    }

    /**
     * Given the current column order, the column to be duplicated, and the pos the put the duplicated column,
     * return the candidate column order with the duplicated column inserted into it. The candidate column order
     * and the columns in the candidate column order are cloned, so that it will not affect the current column order.
     * @param currentOrder
     * @param originColumn
     * @param posForDupedColumn
     * @return
     */
    private List<Column> generateCandidateOrder (List<Column> currentOrder, Column originColumn, int posForDupedColumn)
    {
        Column dupedColumn = originColumn.clone();
        dupedColumn.setDuplicated(true);
        dupedColumn.setDupId(this.columnIdToMaxDupIdMap.get(originColumn.getId())+1);
        List<Column> candidate = new ArrayList<>(currentOrder);
        if (posForDupedColumn < 0 )
        {
            throw new IndexOutOfBoundsException("pos = " + posForDupedColumn);
        }
        if (posForDupedColumn < currentOrder.size())
        {
            candidate.add(posForDupedColumn, dupedColumn);
        }
        else
        {
            candidate.add(dupedColumn);
        }
        return candidate;
    }

    /**
     *
     * @param originCalculator
     * @param dupedColumn
     * @param posForDupedColumn
     * @return
     */
    private DistanceCalculator generateCandidateCalculator (DistanceCalculator originCalculator, Column dupedColumn, int posForDupedColumn)
    {
        if (posForDupedColumn < 0 )
        {
            throw new IndexOutOfBoundsException("pos = " + posForDupedColumn);
        }
        DistanceCalculator calculator = originCalculator.clone();
        calculator.insertColumn(dupedColumn, posForDupedColumn);
        return calculator;
    }

    private List<Query> getRedirectQueries (Column originalColumn, List<Query> workload)
    {
        List<Query> affectedQueryList = new ArrayList<>();
        for (Query query : workload)
        {
            if (query.hasColumnId(originalColumn.getId()))
            {
                affectedQueryList.add(query);
            }
        }
        return affectedQueryList;
    }

    /**
     * Given the candidate column order generated by generateCandidateOrder, the position of the duplicated column and
     * the current workload access pattern, generate the best workload access pattern.
     * @param candidateCalculator
     * @param originColumn
     * @param dupedColumn
     * @param currentPattern
     * @return
     */
    private WorkloadPattern generateCandidatePattern (DistanceCalculator candidateCalculator, Column originColumn, Column dupedColumn, List<Query> affectedQueries, WorkloadPattern currentPattern)
    {
        WorkloadPattern candidatePattern = currentPattern.clone();

        for (Query query : affectedQueries)
        {
            //for each affected queries
            Set<Column> currColumnSet = candidatePattern.getColumnSet(query);
            Set<Column> candidateColumnSet = new HashSet<>(currColumnSet);
            for (Column column : candidateColumnSet)
            {
                if (column.getId() == originColumn.getId())
                {
                    candidateColumnSet.remove(column);
                    break;
                }
            }
            candidateColumnSet.add(dupedColumn);
            double currentCost = evaluateSeekCost(currColumnSet, candidateCalculator);
            double candidateCost = evaluateSeekCost(candidateColumnSet, candidateCalculator);
            if (candidateCost < currentCost)
            {
                candidatePattern.setPattern(query, candidateColumnSet);
            }
        }
        return candidatePattern;
    }

    /**
     * Given the accessed columns and the current column order, return the seek cost
     * @param accessedColumns
     * @param calculator
     * @return
     */
    private double evaluateSeekCost (Set<Column> accessedColumns, DistanceCalculator calculator)
    {
        double seekCost = 0;
        TreeSet<Integer> accessedColumnIds = new TreeSet<>();
        for (Column column : accessedColumns)
        {
            accessedColumnIds.add(calculator.getColumnIndex(column));
        }
        int index1 = accessedColumnIds.pollFirst(), index2;
        while (! accessedColumnIds.isEmpty())
        {
            index2 = accessedColumnIds.pollFirst();
            double seekDistance = calculator.calculateDistance(index1, index2);
            seekCost += this.getSeekCostFunction().calculate(seekDistance);
            index1 = index2;
        }
        return seekCost;
    }

    @Override
    protected double getQuerySeekCost(List<Column> columnOrder, Query query)
    {
        Set<Column> accessedColumns = this.workloadPattern.getColumnSet(query);
        return evaluateSeekCost(accessedColumns, this.distanceCalculator);
    }

    private double calculateWorkloadSeekCost (DistanceCalculator calculator, WorkloadPattern workloadPattern)
    {
        double workloadSeekCost = 0;

        for (Query query : this.getWorkload())
        {
            Set<Column> accessedColumns = workloadPattern.getColumnSet(query);
            workloadSeekCost += query.getWeight() * evaluateSeekCost(accessedColumns, calculator);
        }

        return workloadSeekCost;
    }

    @Override
    public double getCurrentWorkloadSeekCost ()
    {
        return this.workloadPattern.getSeekCost();
    }

    @Override
    public WorkloadPattern getWorkloadPattern()
    {
        return this.workloadPattern;
    }
}
