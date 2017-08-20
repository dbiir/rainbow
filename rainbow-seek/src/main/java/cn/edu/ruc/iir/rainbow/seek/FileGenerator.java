package cn.edu.ruc.iir.rainbow.seek;

import cn.edu.ruc.iir.rainbow.common.cmd.ProgressListener;
import cn.edu.ruc.iir.rainbow.common.util.ConfigFactory;
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
     * @param dataPath
     * @return
     * @throws IOException
     */
    public List<FileStatus> generateHDFSFile (final long blockSize, final long blockCount,
                                              String dataPath, ProgressListener progressListener) throws IOException
    {
        Configuration conf = new Configuration();

        conf.setBoolean("dfs.support.append", true);
        FileSystem fs = FileSystem.get(URI.create("hdfs://" +
                ConfigFactory.Instance().getProperty("namenode") + dataPath), conf);

        Path path = new Path(dataPath);
        if (fs.exists(path) || !fs.isDirectory(path))
        {
            throw new IOException("data path exists or is not a directory");
        }

        fs.mkdirs(new Path(dataPath));
        List<FileStatus> statuses = new ArrayList<FileStatus>();
        if (dataPath.charAt(dataPath.length()-1) != '/' && dataPath.charAt(dataPath.length()-1) != '\\')
        {
            dataPath += "/";
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
            Path filePath = new Path(dataPath + i);
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
     * @param blockSize
     * @param blockCount
     * @param dataPath
     * @param progressListener
     * @return
     * @throws IOException
     */
    public List<File> generateLocalFile (final long blockSize, final long blockCount,
                                   String dataPath, ProgressListener progressListener) throws IOException
    {
        File dir = new File(dataPath);
        if (!dir.isDirectory() || dir.exists())
        {
            throw new IOException("data path exists or is not a directory");
        }

        dir.mkdir();

        List<File> files = new ArrayList<>();
        if (dataPath.charAt(dataPath.length()-1) != '/' && dataPath.charAt(dataPath.length()-1) != '\\')
        {
            dataPath += "/";
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
            File file = new File(dataPath + i);
            BufferedOutputStream out = new BufferedOutputStream(new FileOutputStream(file), bufferSize);

            for (int j = 0; j < n; ++j)
            {
                out.write(buffer);
            }
            out.flush();
            out.close();
            files.add(file);
            progressListener.setPercentage(1.0 * i / blockCount);
        }
        return files;
    }
}
