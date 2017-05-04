package cn.edu.ruc.iir.rainbow.common.metadata;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import parquet.hadoop.ParquetFileReader;
import parquet.hadoop.metadata.BlockMetaData;
import parquet.hadoop.metadata.ParquetMetadata;

import java.io.IOException;
import java.util.List;

import static parquet.format.converter.ParquetMetadataConverter.NO_FILTER;

public class MetaData
{
    private ParquetMetadata metaData = null;

    public MetaData(Configuration conf, Path hdfsFilePath) throws IOException
    {
        this.metaData = ParquetFileReader.readFooter(conf, hdfsFilePath, NO_FILTER);
    }

    public parquet.hadoop.metadata.FileMetaData getFileMetaData ()
    {
        return this.metaData.getFileMetaData();
    }

    public List<BlockMetaData> getBlocks ()
    {
        return this.metaData.getBlocks();
    }
}
