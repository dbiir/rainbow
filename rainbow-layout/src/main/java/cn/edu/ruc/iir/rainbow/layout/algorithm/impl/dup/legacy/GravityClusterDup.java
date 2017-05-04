package cn.edu.ruc.iir.rainbow.layout.algorithm.impl.dup.legacy;

import cn.edu.ruc.iir.rainbow.common.util.ConfigFactory;
import cn.edu.ruc.iir.rainbow.layout.algorithm.Algorithm;
import cn.edu.ruc.iir.rainbow.layout.domian.Column;
import cn.edu.ruc.iir.rainbow.layout.domian.Gravity;
import cn.edu.ruc.iir.rainbow.layout.domian.NeighbourSet;
import cn.edu.ruc.iir.rainbow.layout.domian.Query;
import cn.edu.ruc.iir.rainbow.layout.seekcost.SeekCostFunction;

import java.util.*;

/**
 * Created by hank on 2015/9/4.
 *
 * This class calculate the gravity of each column, bind and sort them,
 * then dup the columns with maximum gravity.
 *
 * This class does not implement getQuerySeekCost function,
 * and do not output the access pattern of the workload on the duplicated layout.
 * It just use the first fit strategy to find the right replica of the columns.
 */
public class GravityClusterDup extends Algorithm
{
    private int divisions = 100000;
    private int gap = 10;
    private int maxClusterLength = 200;
    private Map<Column, List<Query>> columnToQueries = new HashMap<>();
    private Map<Query, List<Column>> queryToColumns = new HashMap<>();
    private double overhead = 0.05;
    private int maxDupedColumnNum = 1000;

    @Override
    public void setup()
    {
        this.divisions = Integer.parseInt(ConfigFactory.Instance().getProperty("cbd.divisions"));
        this.gap = Integer.parseInt(ConfigFactory.Instance().getProperty("cbd.gap"));
        this.maxClusterLength = Integer.parseInt(ConfigFactory.Instance().getProperty("cbd.max.cluster.length"));
        this.maxDupedColumnNum = Integer.parseInt(ConfigFactory.Instance().getProperty("dup.max.duped.columns"));
        this.overhead = Double.parseDouble(ConfigFactory.Instance().getProperty("dup.storage.overhead"));
        Map<Integer, Column> idToColumn = new HashMap<>();
        //System.out.println(this.getColumnOrder().size());
        for (Column column : this.getColumnOrder())
        {
            idToColumn.put(column.getId(), column);
        }
        //System.out.println(idToColumn.size());
        for (Query query : this.getWorkload())
        {
            List<Column> columns = new ArrayList<>();
            for (int cid : query.getColumnIds())
            {
                Column column = idToColumn.get(cid);
                columns.add(column);
                if (!this.columnToQueries.containsKey(column))
                {
                    List<Query> queries = new ArrayList<>();
                    queries.add(query);
                    this.columnToQueries.put(column, queries);
                }
                else
                {
                    this.columnToQueries.get(column).add(query);
                }
            }
            this.queryToColumns.put(query, columns);
        }

        //System.out.println(this.columnToQueries.size() + ", " + this.queryToColumns.size());
    }

    @Override
    public void cleanup()
    {
        this.columnToQueries.clear();
        this.queryToColumns.clear();
    }

    void printOrder()
    {
        System.out.println("Current cbd ordering : ");
        for (int i = 0; i < this.getColumnOrder().size(); i ++)
            System.out.print(this.getColumnOrder().get(i).getId() + " ");
        System.out.println();
    }

    @Override
    public void runAlgorithm()
    {
        // get the total size of the columns
        double totalSize = 0;
        for (Column column : this.getColumnOrder())
        {
            totalSize += column.getSize();
        }

        int[] columnLocations = new int[this.getColumnOrder().size()];

        double currentSize = 0;
        for (Column column : this.getColumnOrder())
        {
            int location = (int)(this.divisions*currentSize/totalSize);
            columnLocations[column.getId()] = location;
            currentSize += column.getSize();
        }

        List<Gravity> gravities = new ArrayList<>();
        for (Column currColumn : this.getColumnOrder())
        {
            //System.out.println(this.columnToQueries.size());
            // for each of the columns, find the one hop neighbours of it.
            if (this.columnToQueries.get(currColumn) == null)
            {
                continue;
            }

            NeighbourSet oneHopNeighbours = new NeighbourSet();

            for (Query query : this.columnToQueries.get(currColumn))
            {
                // for each query which accesses the current column, get the columns accessed by it
                // as the one hop neighbours of the current column.
                for (Column neighbour : this.queryToColumns.get(query))
                {
                    int location = columnLocations[neighbour.getId()];
                    int length = (int) (this.divisions*neighbour.getSize()/totalSize);
                    oneHopNeighbours.addNeighbour(neighbour, query, location, length);
                }
            }
            //System.out.println(oneHopNeighbours.getColumns().size());

            // fill in the coordinate
            // mark the coordinates accessed by the one-hop neighbours.
            int[] coordinate = new int[this.divisions+1];
            for (Column column : oneHopNeighbours.getColumns())
            {
                int location = columnLocations[column.getId()];
                int size = (int) (this.divisions*column.getSize()/totalSize);
                for (int i = 0; i < size; ++i)
                {
                    coordinate[i+location] = 1;
                }
            }

            for (int i = 0; i < this.divisions; )
            {
                List<Integer> cluster = new ArrayList<>();
                int length = i;
                // get the cluster
                for (; i < this.divisions; )
                {
                    while (i < this.divisions && coordinate[i] == 0)
                    {
                        ++i;
                    }
                    if (i >= this.divisions)
                    {
                        break;
                    }
                    // i is the start index of the current one-hop neighbour
                    cluster.add(i);
                    // to find the end index of the current one-hop neighbour
                    while (i < this.divisions && coordinate[i] == 1)
                    {
                        ++i;
                    }
                    int j = 0;
                    for (; j < this.gap && i+j < this.divisions; ++j)
                    {
                        if (coordinate[i + j] == 1)
                        {
                            break;
                        }
                    }
                    // j is the gap between the current and the next one-hop neighbour.
                    i += j;
                    if (j >= this.gap)
                    {
                        // no cluster is found.
                        break;
                    }
                    if (i - length > this.maxClusterLength)
                    {
                        // the cluster is too large.
                        break;
                    }
                    // a cluster is found, continue...
                }

                if (cluster.size() > 0)
                {
                    // if there is a cluster
                    Set<Query> neededBy = new HashSet<>();
                    long sumLocation = 0;
                    long numLocations = 0;
                    double sumSeekCost = 0;
                    for (int localtion : cluster)
                    {
                        // get the queries which has columns on the location
                        Set<Query> queries = oneHopNeighbours.getQueries(localtion);
                        sumLocation += queries.size() * localtion;
                        numLocations += queries.size();
                        neededBy.addAll(queries);
                        double distance = totalSize * (localtion - columnLocations[currColumn.getId()]);
                        if (distance < 0)
                        {
                            distance = -divisions;
                        }
                        SeekCostFunction seekCostFunction = this.getSeekCostFunction();
                        double weight = 0;
                        for (Query query : queries)
                        {
                            weight += query.getWeight();
                        }
                        double cost = seekCostFunction.calculate(distance) * weight;
                        sumSeekCost += cost;
                    }
                    double avgLocation = totalSize/numLocations*sumLocation;
                    Gravity gravity = new Gravity(currColumn, neededBy, sumSeekCost/currColumn.getSize(), avgLocation);
                    gravities.add(gravity);
                }
            }
        }

        Collections.sort(gravities);

        double dupedSize = 0;
        List<Column> dupedColumns = new ArrayList<>();
        Map<Integer, Integer> idToDupId = new HashMap<>();
        for (Column column : this.getColumnOrder())
        {
            idToDupId.put(column.getId(), 1);
        }

        Map<Integer, Set<Query>> cidToQueries = new HashMap<>();
        int dupedColumnNum = 0;
        for (Gravity gravity : gravities)
        {
            Column column = gravity.getColumn();
            dupedSize += column.getSize();
            if (dupedSize/totalSize > this.overhead)
            {
                break;
            }
            if (dupedColumnNum >= this.maxDupedColumnNum)
            {
                break;
            }

            Set<Query> queries = gravity.getQieries();
            Column dupedColumn = new Column(column.getId(), column.getName(), column.getType(), column.getSize());
            for (Query query : queries)
            {
                dupedColumn.addQueryId(query.getId());
            }
            column.setDuplicated(true);
            dupedColumn.setDuplicated(true);
            int dupId = idToDupId.get(column.getId());
            idToDupId.put(column.getId(), dupId + 1);
            dupedColumn.setDupId(dupId);
            dupedColumns.add(dupedColumn);
            dupedColumnNum++;

            /*if (cidToQueries.containsKey(column.getId()))
            {
                cidToQueries.get(column.getId()).addAll(queries);
            }
            else
            {
                Set<Query> qs = new HashSet<>();
                qs.addAll(queries);
                cidToQueries.put(column.getId(), qs);
            }*/
        }

        double storageOverhead = 0;
        for (Column column : dupedColumns)
        {
            storageOverhead += column.getSize();
        }
        System.out.println(storageOverhead + ", " + totalSize);

        this.getColumnOrder().addAll(dupedColumns);

        System.out.println("List of duplicated columns (id) :");
        for (Column col : dupedColumns)
            System.out.print(col.getId() + " ");
        System.out.println();
        // do not shuffle
        //Collections.shuffle(this.getColumnOrder());
    }

    @Override
    public boolean isMultiThreaded()
    {
        return false;
    }

    @Override
    protected double getQuerySeekCost(List<Column> columnOrder, Query query)
    {
        return 0;
    }
}
