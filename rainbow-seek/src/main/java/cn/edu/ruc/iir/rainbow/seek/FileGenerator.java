package cn.edu.ruc.iir.rainbow.seek;

import cn.edu.ruc.iir.rainbow.common.cmd.ProgressListener;
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
     * @param blockSize in bytes
     * @param blockCount
     * @param path
     * @return
     * @throws IOException
     */
    public List<FileStatus> generateHDFSFile (final long blockSize, final long blockCount,
                                              String path, ProgressListener progressListener) throws IOException
    {
        Configuration conf = new Configuration();

        conf.setBoolean("dfs.support.append", true);
        FileSystem fs = FileSystem.get(URI.create(path), conf);
        fs.mkdirs(new Path(path));
        List<FileStatus> statuses = new ArrayList<FileStatus>();
        if (path.charAt(path.length()-1) != '/' && path.charAt(path.length()-1) != '\\')
        {
            path += "/";
        }

        // 1 MB buffer
        final int bufferSize = 1 * 1024 * 1024;
        byte[] buffer = new byte[bufferSize];
        buffer[0] = 1;
        buffer[1] = 2;
        buffer[2] = 3;

        // number of buffers to write for each block
        long n = blockSize / bufferSize;

        for (int i = 0; i < blockCount; ++i)
        {
            // one block per file
            Path filePath = new Path(path + i);
            FSDataOutputStream out = fs.create(filePath, false, bufferSize, (short) 1, n * bufferSize);

            for (int j = 0; j < n; ++j)
            {
                out.write(buffer);
            }
            out.flush();
            out.close();
            statuses.add(fs.getFileStatus(filePath));
            progressListener.setPercentage(1.0 * i / blockCount);
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
    public File generateLocalFile (long fileSize, String path, ProgressListener progressListener) throws IOException
    {
        // 1 MB buffer
        final int bufferSize = 1 * 1024 * 1024;
        BufferedOutputStream out = new BufferedOutputStream(new FileOutputStream(path), bufferSize);
        byte[] buffer = new byte[bufferSize];
        buffer[0] = 1;
        buffer[1] = 2;
        buffer[2] = 3;
        long writenLength = 0;
        double writenPercentage = 0.0;
        long n = fileSize / bufferSize;
        for (int i = 0; i < n; ++i)
        {
            out.write(buffer);
            out.flush();
            writenLength += bufferSize;
            if (writenLength * 100 / fileSize > writenPercentage)
            {
                writenPercentage = 100.0 * writenLength / fileSize;
                progressListener.setPercentage(writenPercentage / 100.0);
            }
        }
        out.close();
        return new File(path);
    }
}
