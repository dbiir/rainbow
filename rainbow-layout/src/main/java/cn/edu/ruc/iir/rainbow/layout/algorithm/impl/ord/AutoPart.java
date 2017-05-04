package cn.edu.ruc.iir.rainbow.layout.algorithm.impl.ord;

import cn.edu.ruc.iir.rainbow.layout.algorithm.Algorithm;
import cn.edu.ruc.iir.rainbow.layout.domian.Column;
import cn.edu.ruc.iir.rainbow.layout.domian.Query;

import java.util.*;

/**
 * Created by hank on 17-5-3.
 * This is the AutoPart algorithm. Does not have the HillClim combination.
 */
public class AutoPart extends Algorithm
{
    private Map<Integer, Column> cidToColumnMap = null;
    private Set<Integer> processedColumns = null;
    private List<List<Column>> qass = null;
    private List<Query> processedQueries = null;

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

        this.qass = new ArrayList<>();

        this.processedQueries = new ArrayList<>();
    }

    @Override
    public void runAlgorithm()
    {
        for (Query query : super.getWorkload())
        {
            this.processedQueries.add(query);
            // qas is the access set of the query.
            List<Column> qas = new ArrayList<>();
            for (int cid : query.getColumnIds())
            {
                if (!this.processedColumns.contains(cid))
                {
                    qas.add(this.cidToColumnMap.get(cid));
                    this.processedColumns.add(cid);
                }
            }
            if (qas.size() == 0)
            {
                continue;
            }

            // neighbours is the set of candidate lists of query access set (qas).
            List<List<List<Column>>> neighbours = this.getNeighbourColumnOrders(qas);
            double minSeekcost = Double.MAX_VALUE;
            for (List<List<Column>> neighbour : neighbours)
            {
                // build a column order from one neighbour.
                List<Column> columnOrder = new ArrayList<>();
                for (List<Column> qas0 : neighbour)
                {
                    columnOrder.addAll(qas0);
                }
                double seekcost = super.getWorkloadSeekCost(this.processedQueries, columnOrder);
                // if this neighbour is better.
                if (seekcost < minSeekcost)
                {
                    minSeekcost = seekcost;
                    this.qass = neighbour;
                    super.setColumnOrder(columnOrder);
                }
            }
        }
        List<Column> columnOrder = super.getColumnOrder();
        for (Column column : super.getSchema())
        {
            if (!this.processedColumns.contains(column.getId()))
            {
                columnOrder.add(column);
            }
        }
    }

    @Override
    public void cleanup()
    {
        super.cleanup();
    }

    @Override
    public boolean isMultiThreaded()
    {
        return false;
    }

    private List<List<List<Column>>> getNeighbourColumnOrders (List<Column> qas)
    {
        List<List<List<Column>>> neighbours = new ArrayList<>();
        for (int i = 0; i < this.qass.size(); ++i)
        {
            List<List<Column>> neighbour = new ArrayList<>();
            for (int j = 0; j < i; ++j)
            {
                neighbour.add(this.qass.get(j));
            }
            neighbour.add(qas);
            for (int j = i; j < this.qass.size(); ++j)
            {
                neighbour.add(this.qass.get(j));
            }
            neighbours.add(neighbour);
        }
        List<List<Column>> neighbour = new ArrayList<>();
        for (int i = 0; i < this.qass.size(); ++i)
        {
            neighbour.add(this.qass.get(i));
        }
        neighbour.add(qas);
        neighbours.add(neighbour);
        return neighbours;
    }
}
