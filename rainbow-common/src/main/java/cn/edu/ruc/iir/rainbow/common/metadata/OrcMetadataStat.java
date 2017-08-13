package cn.edu.ruc.iir.rainbow.common.metadata;

import java.util.List;

public class OrcMetadataStat implements MetadataStat
{
    /**
     * get the number of rows
     *
     * @return
     */
    @Override
    public long getRowCount()
    {
        return 0;
    }

    /**
     * get the average column chunk size of all the row groups
     *
     * @return
     */
    @Override
    public double[] getAvgColumnChunkSize()
    {
        return new double[0];
    }

    /**
     * get the standard deviation of the column chunk sizes.
     *
     * @param avgSize
     * @return
     */
    @Override
    public double[] getColumnChunkSizeStdDev(double[] avgSize)
    {
        return new double[0];
    }

    /**
     * get the field (column) names.
     *
     * @return
     */
    @Override
    public List<String> getFieldNames()
    {
        return null;
    }

    /**
     * get the number of blocks (row groups).
     *
     * @return
     */
    @Override
    public int getBlockCount()
    {
        return 0;
    }

    /**
     * get the number of files.
     *
     * @return
     */
    @Override
    public int getFileCount()
    {
        return 0;
    }

    /**
     * get the average compressed size of the rows in the parquet files.
     *
     * @return
     */
    @Override
    public double getRowSize()
    {
        return 0;
    }

    /**
     * get the total compressed size of all the parquet files.
     *
     * @return
     */
    @Override
    public double getTotalCompressedSize()
    {
        return 0;
    }

    /**
     * get the total uncompressed size of the parquet files.
     *
     * @return
     */
    @Override
    public double getTotalSize()
    {
        return 0;
    }
}
