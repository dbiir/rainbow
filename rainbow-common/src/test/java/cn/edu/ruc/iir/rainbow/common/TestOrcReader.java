package cn.edu.ruc.iir.rainbow.common;

import com.google.protobuf.Descriptors;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.orc.OrcFile;
import org.apache.orc.Reader;
import org.junit.Test;

import java.io.IOException;

public class TestOrcReader
{
    @Test
    public void test () throws IOException, Descriptors.DescriptorValidationException
    {
        Configuration conf = new Configuration();
        Reader reader = OrcFile.createReader(new Path("my-file.orc"),
                OrcFile.readerOptions(conf));
        reader.getSchema().getFieldNames();
        Reader.Options options = new Reader.Options(conf);
        reader.rows(new Reader.Options());

    }
}
