package cn.edu.ruc.iir.rainbow.common;

import com.google.protobuf.Descriptors;
import org.junit.Test;

import java.io.IOException;

public class TestOrcReader
{
    @Test
    public void test () throws IOException, Descriptors.DescriptorValidationException
    {
        /*
        Configuration conf = new Configuration();
        FileSystem fileSystem = FileSystem.get(URI.create("hdfs://192.168.124.15:9000"), conf);
        Path hdfsDirPath = new Path("/msra/orc_test");
        System.out.println(fileSystem.isFile(hdfsDirPath));
        FileStatus[] fileStatuses = fileSystem.listStatus(hdfsDirPath);
        System.out.println(fileStatuses.length);
        for (FileStatus status : fileStatuses)
        {
            System.out.println(status.getPath() + ", " + status.getLen());
        }

        Reader reader = OrcFile.createReader(fileStatuses[0].getPath(),
                OrcFile.readerOptions(conf));
        List<String> columnNames = new ArrayList<>();
        columnNames.add("_col346");
        System.out.println(reader.getRawDataSizeOfColumns(columnNames));
        System.out.println(reader.getFileTail().getFooter().getTypes(0).getFieldNames(0));
        System.out.println(reader.getWriterVersion());
        */
    }
}
