package cn.edu.ruc.iir.rainbow.layout.algorithm;

import cn.edu.ruc.iir.rainbow.common.exception.NotMultiThreadedException;
import cn.edu.ruc.iir.rainbow.common.util.LogFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class ExecutorContainer
{
    private ExecutorService executor = null;
    private Algorithm algo = null;

    public ExecutorContainer(Algorithm algo, int threadCount) throws NotMultiThreadedException
    {
        this.executor = Executors.newCachedThreadPool();
        this.algo = algo;
        algo.setup();
        if (algo.isMultiThreaded())
        {
            for (int i = 0; i < threadCount; ++i)
            {
                this.executor.execute(new AlgorithmExecutor(algo));
            }
        }
        else
        {
            if (threadCount == 1)
            {
                this.executor.execute(new AlgorithmExecutor(algo));
            }
            else
            {
                throw new NotMultiThreadedException("Algorithm " + algo.getClass().getName() + " is not multi-threaded.");
            }
        }
    }

    public void waitForCompletion() throws InterruptedException
    {
        this.executor.shutdown();
        long computationBudget = this.algo.getComputationBudget();
        if (computationBudget < 0)
        {
            LogFactory.Instance().getLog().debug("Computation budget {" + computationBudget + "} is less than zero, force set to zero.");
            computationBudget = 0;
        }
        while (this.executor.awaitTermination(computationBudget+1, TimeUnit.SECONDS) == false)
        {
            LogFactory.Instance().getLog().debug("Computation budget {" + computationBudget +
                    "} is not enough. We are not giving up. Another {" + computationBudget + "} is given.");
        }
        this.executor.shutdownNow();
        this.algo.cleanup();
    }
}
