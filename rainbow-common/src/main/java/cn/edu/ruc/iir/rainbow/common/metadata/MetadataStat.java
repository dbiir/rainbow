package cn.edu.ruc.iir.rainbow.common.metadata;

import java.util.List;

public interface MetadataStat
{
    /**
     * get the number of rows
     * @return
     */
    public long getRowCount ();

    /**
     * get the average column chunk size of all the row groups
     * @return
     */
    public double[] getAvgColumnChunkSize ();

    /**
     * get the standard deviation of the column chunk sizes.
     * @param avgSize
     * @return
     */
    public double[] getColumnChunkSizeStdDev (double[] avgSize);

    /**
     * get the field (column) names.
     * @return
     */
    public List<String> getFieldNames ();

    /**
     * get the number of blocks (row groups).
     * @return
     */
    public int getBlockCount ();

    /**
     * get the number of files.
     * @return
     */
    public int getFileCount ();

    /**
     * get the average compressed size of the rows in the parquet files.
     * @return
     */
    public double getRowSize ();

    /**
     * get the total compressed size of all the parquet files.
     * @return
     */
    public double getTotalCompressedSize ();

    /**
     * get the total uncompressed size of the parquet files.
     * @return
     */
    public double getTotalSize ();
}
