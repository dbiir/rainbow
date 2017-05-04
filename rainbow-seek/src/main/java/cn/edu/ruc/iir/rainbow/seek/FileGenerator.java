package cn.edu.ruc.iir.rainbow.seek;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by hank on 2015/2/9.
 */
public class FileGenerator
{
    private static FileGenerator instance = null;

    public static FileGenerator Instance ()
    {
        if (instance == null)
        {
            instance = new FileGenerator();
        }
        return instance;
    }

    private FileGenerator() {}

    /**
     * Generate a batch of files on HDFS
     * @param blockSize
     * @param blockCount
     * @param path
     * @return
     * @throws IOException
     */
    public List<FileStatus> generateHDFSFile (final long blockSize, final long blockCount, String path) throws IOException
    {
        Configuration conf = new Configuration();
        //conf.setLong("dfs.block.size", blockSize);
        conf.setBoolean("dfs.support.append", true);
        FileSystem fs = FileSystem.get(URI.create(path), conf);
        fs.mkdirs(new Path(path));
        List<FileStatus> statuses = new ArrayList<FileStatus>();
        if (path.charAt(path.length()-1) != '/' && path.charAt(path.length()-1) != '\\')
        {
            path += "/";
        }

        // 10 MB buffer
        byte[] buffer = new byte[16* 1024 * 1024];
        buffer[0] = 1;
        buffer[1] = 2;
        buffer[2] = 3;

        // number of buffers to write
        long n = blockSize / 1024 / 1024 / 16;

        for (int i = 0; i < blockCount; ++i)
        {
            Path filePath = new Path(path + i);
            FSDataOutputStream out = fs.create(filePath, false, 64 * 1024 * 1024, (short) 1, blockSize);

            for (int j = 0; j < n; ++j)
            {
                out.write(buffer);
            }
            out.flush();
            out.close();
            statuses.add(fs.getFileStatus(filePath));
        }
        return statuses;
    }

    /**
     *
     * @param fileSize
     * @param path
     * @return
     * @throws IOException
     */
    public File generateLocalFile (long fileSize, String path) throws IOException
    {
        BufferedOutputStream out = new BufferedOutputStream(new FileOutputStream(path), 32 * 1024 * 1024);
        byte[] buffer = new byte[100*1024];
        buffer[0] = 1;
        buffer[1] = 2;
        buffer[2] = 3;
        long n = fileSize/ 1024 / 100;
        for (int i = 0; i < n; ++i)
        {
            out.write(buffer);
        }
        out.flush();
        out.close();
        return new File(path);
    }
}
