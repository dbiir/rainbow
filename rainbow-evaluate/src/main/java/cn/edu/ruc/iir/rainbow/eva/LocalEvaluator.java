package cn.edu.ruc.iir.rainbow.eva;

import cn.edu.ruc.iir.rainbow.eva.domain.Column;
import cn.edu.ruc.iir.rainbow.eva.metrics.LocalMetrics;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import parquet.column.ColumnDescriptor;
import parquet.column.page.PageReadStore;
import parquet.hadoop.ParquetFileReader;
import parquet.hadoop.metadata.ParquetMetadata;
import parquet.schema.MessageType;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

import static parquet.format.converter.ParquetMetadataConverter.NO_FILTER;

/**
 * Created by hank on 2015/2/5.
 */
public class LocalEvaluator
{
    public static FileStatus[] getFileStatuses (String path, Configuration conf) throws IOException
    {
        FileSystem fileSystem = FileSystem.get(URI.create(path), conf);
        return fileSystem.listStatus(new Path(path));
    }

    public static ParquetMetadata[] getMetadatas (FileStatus[] fileStatuses, Configuration conf) throws IOException
    {
        ParquetMetadata[] res = new ParquetMetadata[fileStatuses.length];
        for (int i = 0; i < fileStatuses.length; ++i)
        {
            res[i] = ParquetFileReader.readFooter(conf, fileStatuses[i].getPath(), NO_FILTER);
        }
        return res;
    }

    public static LocalMetrics execute (FileStatus[] fileStatuses, ParquetMetadata[] metadatas, String[] columnNames, Configuration conf) throws IOException
    {
        boolean printColumns = true;
        List<ParquetFileReader> readers = new ArrayList<ParquetFileReader>();
        List<Column> columns = new ArrayList<Column>();
        for (int i = 0; i < fileStatuses.length; ++i)
        {
            FileStatus status = fileStatuses[i];
            ParquetMetadata metadata = metadatas[i];

            MessageType schema = metadata.getFileMetaData().getSchema();

            List<ColumnDescriptor> columnDescriptors = new ArrayList<ColumnDescriptor>();

            for (String columnName : columnNames)
            {
                int fieldIndex = schema.getFieldIndex(columnName.toLowerCase());
                ColumnDescriptor descriptor = schema.getColumns().get(fieldIndex);

                columnDescriptors.add(descriptor);

                if (printColumns)
                {
                    Column column = new Column();
                    column.setIndex(fieldIndex);
                    column.setName(schema.getFieldName(column.getIndex()));
                    column.setDescriptor(descriptor);
                    columns.add(column);
                }
            }
            printColumns = false;

            readers.add(new ParquetFileReader(conf, status.getPath(), metadata.getBlocks(), columnDescriptors));
        }

        long time  = System.currentTimeMillis();
        long rowCount = 0;
        long rowGroupCount = 0;
        long readerCount = readers.size();
        for (ParquetFileReader reader : readers)
        {
            PageReadStore pageReadStore;
            while ((pageReadStore = reader.readNextRowGroup()) != null)
            {
                rowGroupCount ++;
                rowCount += pageReadStore.getRowCount();
            }
            reader.close();
        }
        LocalMetrics metrics = new LocalMetrics(columns, readerCount, rowGroupCount, rowCount, System.currentTimeMillis()-time);
        return metrics;
    }
}
