package cn.edu.ruc.iir.rainbow.layout.algorithm.impl.dup;

import cn.edu.ruc.iir.rainbow.common.util.ConfigFactory;
import cn.edu.ruc.iir.rainbow.common.util.LogFactory;
import cn.edu.ruc.iir.rainbow.layout.algorithm.RefineAlgorithm;
import cn.edu.ruc.iir.rainbow.layout.domian.Column;
import cn.edu.ruc.iir.rainbow.layout.domian.Query;
import cn.edu.ruc.iir.rainbow.layout.seekcost.SeekCostFunction;

import java.util.*;

/**
 * Algorithm designed by Wenbo on 2015/11/5.
 * Created by hank on 2017/5/1.
 */
public class FastRefine extends RefineAlgorithm
{
    //SCOA
    private Random rand = new Random(System.nanoTime());
    private double coolingRate = 0.003;
    private volatile double temperature = 100000;
    private volatile long iterations = 0;
    private double currentEnergy = 0;
    private double[] startOffset;
    private double[] endOffset;

    //FastSCOA
    protected List<TreeSet<Integer>> queryAccessedPos;

    @Override
    public void setQueryAccessedPos(List<TreeSet<Integer>> queryAccessedPos)
    {
        this.queryAccessedPos = queryAccessedPos;
    }

    @Override
    public boolean isMultiThreaded()
    {
        return false;
    }

    public void setup()
    {
        String strCoolingRate = ConfigFactory.Instance().getProperty("refine.cooling_rate");
        String strInitTemp = ConfigFactory.Instance().getProperty("refine.init.temperature");
        this.setColumnOrder(this.getSchema());
        this.setSchema(new ArrayList<>(this.getSchema()));
        if (strCoolingRate != null)
        {
            this.coolingRate = Double.parseDouble(strCoolingRate);
        }
        if (strInitTemp != null)
        {
            this.temperature = Double.parseDouble(strInitTemp);
        }
        this.startOffset = new double[this.getColumnOrder().size()];
        this.endOffset = new double[this.getColumnOrder().size()];
        this.updateOffsets();
    }

    public void cleanup()
    {
        LogFactory.Instance().getLog().info("[FastRefine] Total iterations: " + this.iterations);
    }

    private void updateOffsets()
    {
        for (int i = 0; i < this.getColumnOrder().size(); i++)
        {
            this.startOffset[i] = (i == 0 ? 0 : this.startOffset[i - 1] + this.getColumnOrder().get(i - 1).getSize());
            this.endOffset[i] = (i == 0 ? this.getColumnOrder().get(0).getSize() :
                    this.endOffset[i - 1] + this.getColumnOrder().get(i).getSize());
        }
    }

    @Override
    public double getCurrentWorkloadSeekCost()
    {
        double workloadSeekCost = 0;
        this.updateOffsets();
        List<Column> columnOrder = this.getColumnOrder();
        for (Query query : this.getWorkload())
        {
            workloadSeekCost += query.getWeight() * getQuerySeekCost(columnOrder, query);
        }
        return workloadSeekCost;
    }

    @Override
    protected double getQuerySeekCost(List<Column> columnOrder, Query query)
    {
        int lastPos = -1;
        int j = this.getWorkload().indexOf(query);
        SeekCostFunction sc = this.getSeekCostFunction();
        double seekCost = 0;
        for (int pos : this.queryAccessedPos.get(j))
        {
            if (lastPos != -1)
            {
                seekCost += sc.calculate(this.startOffset[pos] - this.endOffset[lastPos]);
            }
            lastPos = pos;
        }
        return seekCost;
    }

    private double probability(double e, double e1, double temperature)
    {
        if (e1 < e)
        {
            return 1;
        } else
        {
            return Math.exp((e - e1) / temperature);
        }
    }

    @Override
    public void runAlgorithm()
    {
        long startSeconds = System.currentTimeMillis() / 1000;
        this.currentEnergy = getCurrentWorkloadSeekCost();

        LogFactory.Instance().getLog().info("[FastRefine] Refining... Initial workload seek cost: " + this.currentEnergy);
        LogFactory.Instance().getLog().info("[FastRefine] Computation budget: " + this.getComputationBudget());

        for (long currentSeconds = System.currentTimeMillis() / 1000;
             (currentSeconds - startSeconds) < this.getComputationBudget();
             currentSeconds = System.currentTimeMillis() / 1000, ++this.iterations)
        {
            //generate two random indices
            int i = rand.nextInt(this.getColumnOrder().size());
            int j = i;
            while (j == i)
            {
                j = rand.nextInt(this.getColumnOrder().size());
            }
            rand.setSeed(System.nanoTime());

            double neighbourEnergy = getNeighbourSeekCost(i, j);

            //try to accept it
            double temperature = this.temperature;
            if (this.probability(currentEnergy, neighbourEnergy, temperature) > Math.random())
            {
                currentEnergy = neighbourEnergy;
                updateColumnOrder(i, j);
            }
        }
        LogFactory.Instance().getLog().info("[FastRefine] Final workload seek cost: " + currentEnergy);
    }

    /**
     *
     * @param x position 1
     * @param y position 2
     */
    private void updateColumnOrder(int x, int y)
    {
        // swap
        Column column = this.getColumnOrder().get(x);
        this.getColumnOrder().set(x, this.getColumnOrder().get(y));
        this.getColumnOrder().set(y, column);

        //
        for (int i = 0; i < this.getWorkload().size(); i ++)
        {
            TreeSet<Integer> curSet = this.queryAccessedPos.get(i);
            int tot = 0;
            if (curSet.contains(x)) tot ++;
            if (curSet.contains(y)) tot ++;
            if (tot != 1) continue;

            if (curSet.contains(x))
            {
                curSet.remove(x); curSet.add(y);
            }
            else
            {
                curSet.remove(y); curSet.add(x);
            }
        }
    }

    private int getPrev(TreeSet<Integer> accessedPos, int x)
    {
        SortedSet head = accessedPos.headSet(x);
        if (head.isEmpty())
        {
            return -1;
        }
        return (int) head.last();
    }

    private int getSucc(TreeSet<Integer> accessedPos, int x)
    {
        SortedSet tail = accessedPos.tailSet(x);
        if (tail.isEmpty())
        {
            return -1;
        }
        return (int) tail.first();
    }

    private double getNeighbourSeekCost(int x, int y)
    {
        int columnNum = this.getColumnOrder().size();
        int queryNum = this.getWorkload().size();
        SeekCostFunction sc = this.getSeekCostFunction();
        double[] so = new double[columnNum];
        double[] eo = new double[columnNum];

        for (int i = 0; i < columnNum; i ++)
        {
            so[i] = (i == 0 ? 0 : so[i - 1] + this.getColumnOrder().get(i - 1).getSize());
            eo[i] = (i == 0 ? this.getColumnOrder().get(0).getSize() :
                    eo[i - 1] + this.getColumnOrder().get(i).getSize());
        }

        double deltaCost = 0.0;
        for (int i = 0; i < queryNum; i ++)
        {
            double delta = 0;
            TreeSet<Integer> curSet = this.queryAccessedPos.get(i);
            if (! curSet.contains(x) && ! curSet.contains(y))
            {
                int prevX = getPrev(curSet, x); int succX = getSucc(curSet, x);
                int prevY = getPrev(curSet, y); int succY = getSucc(curSet, y);
                if (succX != succY)
                {
                    if (prevX >= 0 && succX >= 0)
                    {
                        delta -= sc.calculate(so[succX] - eo[prevX]);
                        delta += sc.calculate(so[succX] - eo[prevX]
                                - this.getColumnOrder().get(x).getSize()
                                + this.getColumnOrder().get(y).getSize());
                    }
                    if (prevY >= 0 && succY >= 0)
                    {
                        delta -= sc.calculate(so[succY] - eo[prevY]);
                        delta += sc.calculate(so[succY] - eo[prevY]
                                - this.getColumnOrder().get(y).getSize()
                                + this.getColumnOrder().get(x).getSize());
                    }
                }
            }
            else if (curSet.contains(x) && curSet.contains(y))
            {
                //The easiest case, do nothing!!!!
            }
            else
            {
                if (curSet.contains(y))
                {
                    //do swap
                    int t; t = x; x = y; y = t;
                }
                curSet.remove(x);
                int prevX = getPrev(curSet, x); int succX = getSucc(curSet, x);
                int prevY = getPrev(curSet, y); int succY = getSucc(curSet, y);
                if (succX == succY) //special case
                {
                    if (prevX >= 0)
                    {
                        delta -= sc.calculate(so[x] - eo[prevX]);
                    }
                    if (succX >= 0)
                    {
                        delta -= sc.calculate(so[succX] - eo[x]);
                    }
                    //add
                    if (x < y)
                    {
                        if (succX >= 0)
                        {
                            delta += sc.calculate(so[succX] - eo[y]);
                        }
                        if (prevX >= 0)
                        {
                            delta += sc.calculate(so[y] - eo[prevX]
                                    - this.getColumnOrder().get(x).getSize()
                                    + this.getColumnOrder().get(y).getSize());
                        }
                    }
                    else
                    {
                        if (prevX >= 0)
                        {
                            delta += sc.calculate(so[y] - eo[prevX]);
                        }
                        if (succX >= 0)
                        {
                            delta += sc.calculate(so[succX] - eo[y]
                                    - this.getColumnOrder().get(x).getSize()
                                    + this.getColumnOrder().get(y).getSize());
                        }
                    }
                }
                else
                {
                    //minus
                    if (prevX >= 0)
                    {
                        delta -= sc.calculate(so[x] - eo[prevX]);
                    }
                    if (succX >= 0)
                    {
                        delta -= sc.calculate(so[succX] - eo[x]);
                    }
                    if (prevY >= 0 && succY >= 0)
                    {
                        delta -= sc.calculate(so[succY] - eo[prevY]);
                    }
                    //add
                    if (prevY >= 0)
                    {
                        delta += sc.calculate(so[y] - eo[prevY]);
                    }
                    if (succY >= 0)
                    {
                        delta += sc.calculate(so[succY] - eo[y]);
                    }
                    if (prevX >= 0 && succX >= 0)
                    {
                        delta += sc.calculate(so[succX] - eo[prevX]
                                - this.getColumnOrder().get(x).getSize()
                                + this.getColumnOrder().get(y).getSize());
                    }
                }
                curSet.add(x);
            }
            deltaCost += delta * this.getWorkload().get(i).getWeight();
        }
        return currentEnergy + deltaCost;
    }
}
