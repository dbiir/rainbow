package cn.edu.ruc.iir.rainbow.layout.algorithm.impl.dup.legacy;

import cn.edu.ruc.iir.rainbow.common.util.ConfigFactory;
import cn.edu.ruc.iir.rainbow.layout.algorithm.Algorithm;
import cn.edu.ruc.iir.rainbow.layout.domian.Column;
import cn.edu.ruc.iir.rainbow.layout.domian.Gravity;
import cn.edu.ruc.iir.rainbow.layout.domian.NeighbourSet;
import cn.edu.ruc.iir.rainbow.layout.domian.Query;

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
public class GravityDup extends Algorithm
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
        int[] coordinateAll = new int[this.divisions+1];
        double unaccessedSize = 0;
        int unaccessedNum = 0;
        for (Column currColumn : this.getColumnOrder())
        {
            if (columnToQueries.containsKey(currColumn))
            {

                int location = columnLocations[currColumn.getId()];
                int size = (int) (this.divisions*currColumn.getSize()/totalSize);
                System.out.println(location + ", " + size + ", " + currColumn.getName());
                if (size == 0)
                {
                    size = 1;
                }
                for (int i = 0; i < size; ++i)
                {
                    coordinateAll[i+location] = 1;
                }
            }
            else
            {
                unaccessedSize += currColumn.getSize();
                unaccessedNum++;
            }
        }
        for(int v : coordinateAll)
        {
            System.out.print(v);
        }
        System.out.println();
        System.out.println(unaccessedNum);
        System.out.println(unaccessedSize);
        System.out.println(totalSize);

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
                if (size == 0)
                {
                    size = 1;
                }
                for (int i = 0; i < size; ++i)
                {
                    coordinate[i+location] = 1;
                }
            }
            int location = columnLocations[currColumn.getId()];
            int size = (int) (this.divisions*currColumn.getSize()/totalSize);
            if (size == 0)
            {
                size = 1;
            }
            for (int i = 0; i < size; ++i)
            {
                coordinate[i+location] = 2;
            }

            for(int v : coordinate)
            {
                System.out.print(v);
            }
            System.out.println();
        }
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
