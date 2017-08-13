package cn.edu.ruc.iir.rainbow.common;

import cn.edu.ruc.iir.rainbow.common.exception.MetaDataException;
import cn.edu.ruc.iir.rainbow.common.metadata.ParquetMetadataStat;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;

/**
 *
 */
public class TestMetadata
{
    @Test
    void testGetMetadata () throws IOException, MetaDataException
    {
        ParquetMetadataStat stat = new ParquetMetadataStat("10.172.96.77", 9000, "/lineitem_grouped_test_parq");
        double[] avgs = stat.getAvgColumnChunkSize();
        double[] devs = stat.getColumnChunkSizeStdDev(avgs);
        List<String> names = stat.getFieldNames();
        System.out.println(stat.getFileCount() + ", " + stat.getBlockCount());

        for (int i = 0; i < avgs.length; ++i)
        {
            System.out.println(names.get(i) + "\t" + avgs[i]);
        }
        System.out.println(stat.getTotalSize());
        System.out.println(stat.getTotalCompressedSize());
    }
}
