package cn.edu.ruc.iir.rainbow.common;

import cn.edu.ruc.iir.rainbow.common.exception.MetaDataException;
import cn.edu.ruc.iir.rainbow.common.metadata.ParquetFileMetadata;
import cn.edu.ruc.iir.rainbow.common.metadata.ParquetMetadataStat;
import cn.edu.ruc.iir.rainbow.common.util.OutputFactory;
import org.junit.jupiter.api.Test;
import parquet.hadoop.metadata.BlockMetaData;

import java.io.BufferedWriter;
import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Created by hank on 2015/1/31.
 */
public class TestParquetMetadataStat
{
    @Test
    public void testGetStat () throws IOException, MetaDataException
    {
        //ParquetMetadataStat stat = new ParquetMetadataStat("10.172.96.77", 9000, "/tmp/hive-root/hive_2015-02-04_10-57-36_131_1404874572956637570-1/_tmp.-ext-10002");
        ParquetMetadataStat stat = new ParquetMetadataStat("222.29.197.231", 9000, "/parq_43");
        double[] columnSize = stat.getAvgColumnChunkSize();
        List<String> names = stat.getFieldNames();
        int i = 0;
        double total = 0;
        for (double size: columnSize)
        {
            //System.out.println(names.get(i) + "\t" + size);
            total += size;
            i++;
        }
        System.out.println(total/1024/1024 + "\t" + stat.getBlockCount() + "\t" + stat.getFileCount());

        for (BlockMetaData bm : stat.getBlocks())
        {
            System.out.println(bm.getCompressedSize() + ", " + bm.getTotalByteSize() + ", " + bm.getRowCount());
        }

        List<ParquetFileMetadata> metaDatas = stat.getFileMetaData();
        System.out.println(metaDatas.get(0).getFileMetaData().getCreatedBy());
        Map<String, String> keyValueMetaData = metaDatas.get(0).getFileMetaData().getKeyValueMetaData();
        for (String key : keyValueMetaData.keySet())
        {
            System.out.println(key + "=" + keyValueMetaData.get(key));
        }

        BufferedWriter writer = OutputFactory.Instance().getWriter("cord-generator/resources/column_size.txt");
        for (int j = 0; j < names.size(); ++j)
        {
            writer.write(names.get(j) + "\t" + columnSize[j] + "\n");
        }
        writer.close();
    }
}
