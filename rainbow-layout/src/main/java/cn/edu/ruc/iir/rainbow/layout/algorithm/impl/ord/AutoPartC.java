package cn.edu.ruc.iir.rainbow.layout.algorithm.impl.ord;

import cn.edu.ruc.iir.rainbow.layout.algorithm.Algorithm;
import cn.edu.ruc.iir.rainbow.layout.domian.Column;
import cn.edu.ruc.iir.rainbow.layout.domian.Query;

import java.util.*;

/**
 * Created by hank on 17-5-3.
 * This is the AutoPart algorithm with HillClim combination.
 * It is the AutoPart-C algorithm in our paper.
 */
public class AutoPartC extends Algorithm
{
    private Map<Integer, Column> cidToColumnMap = null;
    private Set<Integer> processedColumns = null;
    private List<List<Column>> splits = null;
    private List<Query> processedQueries = null;

    @Override
    public boolean isMultiThreaded()
    {
        return false;
    }

    @Override
    public void setup()
    {
        // set the current column order to an empty array.
        super.setColumnOrder(new ArrayList<>());
        // init the column id to column map.
        this.cidToColumnMap = new HashMap<>();
        for (Column column : super.getSchema())
        {
            this.cidToColumnMap.put(column.getId(), column);
        }
        // sort the queries.
        Collections.sort(super.getWorkload(), (q1, q2) -> (int)(q2.getWeight()-q1.getWeight()));
        // init the hash set of processed columns.
        this.processedColumns = new HashSet<>();

        this.splits= new ArrayList<>();

        this.processedQueries = new ArrayList<>();
    }

    @Override
    public void runAlgorithm()
    {
        for (Query query : super.getWorkload())
        {
            this.processedQueries.add(query);
            // qas is the access set of the query.
            List<Column> split = new ArrayList<>();
            for (int cid : query.getColumnIds())
            {
                if (!this.processedColumns.contains(cid))
                {
                    split.add(this.cidToColumnMap.get(cid));
                    this.processedColumns.add(cid);
                }
            }
            if (split.size() == 0)
            {
                continue;
            }

            // neighbours is the set of candidate lists of query access set (qas).
            List<List<List<Column>>> neighbours = this.getNeighbourColumnOrders(this.splits, split);
            evaluateNeighbours (neighbours, this.processedQueries);
        }
        this.combine();
        List<Column> columnOrder = super.getColumnOrder();
        for (Column column : super.getSchema())
        {
            if (!this.processedColumns.contains(column.getId()))
            {
                columnOrder.add(0, column);
            }
        }
    }

    @Override
    public void cleanup()
    {
        super.cleanup();
    }


    private List<List<List<Column>>> getNeighbourColumnOrders (List<List<Column>> splits, List<Column> split0)
    {
        List<List<List<Column>>> neighbours = new ArrayList<>();
        List<Column> split = new ArrayList<>();
        split.addAll(split0);
        for (int i = 0; i < splits.size(); ++i)
        {
            List<List<Column>> neighbour = new ArrayList<>();
            for (int j = 0; j < i; ++j)
            {
                neighbour.add(splits.get(j));
            }
            neighbour.add(split);
            for (int j = i; j < splits.size(); ++j)
            {
                neighbour.add(splits.get(j));
            }
            neighbours.add(neighbour);
        }
        List<List<Column>> neighbour = new ArrayList<>();
        for (int i = 0; i < splits.size(); ++i)
        {
            neighbour.add(splits.get(i));
        }
        neighbour.add(split);
        neighbours.add(neighbour);
        return neighbours;
    }

    private double evaluateNeighbours (List<List<List<Column>>> neighbours, List<Query> workload)
    {
        double minSeekcost = Double.MAX_VALUE;
        for (List<List<Column>> neighbour : neighbours)
        {
            // build a column order from one neighbour.
            List<Column> columnOrder = new ArrayList<>();
            for (List<Column> qas0 : neighbour)
            {
                columnOrder.addAll(qas0);
            }

            double seekcost = super.getWorkloadSeekCost(workload, columnOrder);
            // if this neighbour is better.
            if (seekcost < minSeekcost)
            {
                minSeekcost = seekcost;
                this.splits = neighbour;
                super.setColumnOrder(columnOrder);
            }
        }
        return minSeekcost;
    }

    private void combine ()
    {
        double seekcost = super.getWorkloadSeekCost(this.processedQueries, this.getColumnOrder());

        List<List<Column>> newSplits = new ArrayList<>();
        List<Column> combineSplit = new ArrayList<>();

        System.out.println("init split size=" + this.splits.size());
        double minSeekCost = seekcost;
        do
        {
            List<List<List<Column>>> neighbours = new ArrayList<>();
            for (int i = 0; i < this.splits.size(); ++i)
            {
                System.out.println("i=" + i + ", neighbours size=" + neighbours.size());
                for (int j = i + 1; j < this.splits.size(); ++j)
                {
                    newSplits.clear();
                    newSplits.addAll(this.splits);
                    newSplits.remove(j);
                    newSplits.remove(i);

                    combineSplit.clear();
                    combineSplit.addAll(this.splits.get(i));
                    combineSplit.addAll(this.splits.get(j));
                    for (List<List<Column>> neighbour : this.getNeighbourColumnOrders(newSplits, combineSplit))
                    {
                        List<Column> columnOrder = new ArrayList<>();
                        for (List<Column> qas0 : neighbour)
                        {
                            columnOrder.addAll(qas0);
                        }
                        if (super.getWorkloadSeekCost(this.processedQueries, columnOrder) < minSeekCost)
                        {
                            neighbours.add(neighbour);
                        }
                    }

                    combineSplit.clear();
                    combineSplit.addAll(this.splits.get(j));
                    combineSplit.addAll(this.splits.get(i));
                    // in getNeighbourColumnOrders method, combineSplit is inserted into newSplits
                    // But combineSplit may be updated outside getNeighbourColumnOrders. It should be copied in the method or on update.
                    for (List<List<Column>> neighbour : this.getNeighbourColumnOrders(newSplits, combineSplit))
                    {
                        List<Column> columnOrder = new ArrayList<>();
                        for (List<Column> qas0 : neighbour)
                        {
                            columnOrder.addAll(qas0);
                        }
                        if (super.getWorkloadSeekCost(this.processedQueries, columnOrder) < minSeekCost)
                        {
                            neighbours.add(neighbour);
                        }
                    }
                }
            }
            seekcost = minSeekCost;
            minSeekCost = this.evaluateNeighbours(neighbours, this.processedQueries);
            System.out.println("current column order size:" + this.getColumnOrder().size());
            System.out.println("remaining splits=" + this.splits.size() + ", " + minSeekCost + ", " +
                    this.getWorkloadSeekCost(this.getWorkload(),this.getColumnOrder()));
        } while (minSeekCost < seekcost);
    }
}
