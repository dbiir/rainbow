package cn.edu.ruc.iir.rainbow.common;

import cn.edu.ruc.iir.rainbow.common.exception.MetadataException;
import cn.edu.ruc.iir.rainbow.common.metadata.OrcMetadataStat;
import com.google.protobuf.Descriptors;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.orc.OrcFile;
import org.apache.orc.OrcProto;
import org.apache.orc.Reader;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

public class TestOrcMetadata
{
    @Test
    public void test () throws IOException, Descriptors.DescriptorValidationException
    {
        Configuration conf = new Configuration();
        System.setProperty("hadoop.home.dir", "/");
        FileSystem fileSystem = FileSystem.get(URI.create("hdfs://presto00:9000"), conf);
        Path hdfsDirPath = new Path("/rainbow2/orc_new_compress");
        System.out.println(fileSystem.isFile(hdfsDirPath));
        FileStatus[] fileStatuses = fileSystem.listStatus(hdfsDirPath);
        System.out.println(fileStatuses.length);
        for (FileStatus status : fileStatuses)
        {
            status.getPath();
            System.out.println(status.getPath() + ", " + status.getLen());
        }

        Reader reader = OrcFile.createReader(fileStatuses[0].getPath(),
                OrcFile.readerOptions(conf));
        List<String> columnNames = new ArrayList<>();
        columnNames.add("samplepercent");
        System.out.println(reader.getRawDataSizeOfColumns(columnNames));
        System.out.println(reader.getFileTail().getFooter().getTypes(0).getFieldNames(0));
        System.out.println(reader.getTypes().get(0).getSerializedSize());

        List<Reader> readers = new ArrayList<>();
        for (FileStatus fileStatus : fileStatuses)
        {
            Reader reader1 = OrcFile.createReader(fileStatus.getPath(),
                    OrcFile.readerOptions(conf));
            readers.add(reader1);
            System.out.println("content size: " + reader1.getContentLength() + ", raw size: "
            + reader1.getRawDataSize());
        }

        for (String columnName : reader.getSchema().getFieldNames())
        {
            System.out.println(columnName);
        }
    }

    @Test
    public void testReadColumnChunkSize () throws IOException, Descriptors.DescriptorValidationException
    {
        Configuration conf = new Configuration();
        System.setProperty("hadoop.home.dir", "/");
        FileSystem fileSystem = FileSystem.get(URI.create("hdfs://presto00:9000"), conf);
        Path hdfsDirPath = new Path("/rainbow2/orc");
        FileStatus[] fileStatuses = fileSystem.listStatus(hdfsDirPath);

        Reader reader = OrcFile.createReader(fileStatuses[0].getPath(),
                OrcFile.readerOptions(conf));
        FSDataInputStream inputStream = fileSystem.open(fileStatuses[0].getPath());
        long offset = reader.getStripes().get(0).getOffset();
        long dataLength = reader.getStripes().get(0).getDataLength();
        long indexLength = reader.getStripes().get(0).getIndexLength();
        long footerLength = reader.getStripes().get(0).getFooterLength();
        long stripeLength = reader.getStripes().get(0).getLength();

        assert (stripeLength == (footerLength + dataLength + indexLength));
        System.out.println(reader.getFileTail().getPostscript().getCompression().name());
        System.out.println(offset + ", " + indexLength + ", " + dataLength + ", " +
                footerLength + ", " + stripeLength + ", " +
                reader.getStripes().get(1).getOffset() + ", " + reader.getStripes().size());
        byte[] buffer = new byte[(int)footerLength];
        //inputStream.seek();
        inputStream.readFully(offset + indexLength + dataLength, buffer);

        inputStream.close();

        OrcProto.StripeFooter footer = OrcProto.StripeFooter.parseFrom(buffer);
        List<OrcProto.Stream> streams = footer.getStreamsList();
        List<OrcProto.Stream> dataStreams = new ArrayList<>();
        long size = 0;
        for (int cid : reader.getFileTail().getFooter().getTypes(0).getSubtypesList())
        {
            System.out.println("cid: " + cid);
        }

        for (OrcProto.Stream stream : streams)
        {
            //if (stream.getKind().name().equals("DATA"))
            {
                dataStreams.add(stream);
                //System.out.println(stream.getColumn());
                size += stream.getLength();
            }
        }
        System.out.println(dataStreams.size() + ", " + size);
    }

    @Test
    public void testOrcMetadataStat () throws IOException, MetadataException
    {
        System.setProperty("hadoop.home.dir", "/");
        OrcMetadataStat stat = new OrcMetadataStat("presto00", 9000, "/rainbow2/orc");
        System.out.println(stat.getFieldNames());
        double[] sizes = stat.getAvgColumnChunkSize();
        double totalSize = 0;
        for (double size : sizes)
        {
            totalSize += size;
            System.out.println(size);
        }
        System.out.println(totalSize + ", " + stat.getRowGroupCount());
    }
}
