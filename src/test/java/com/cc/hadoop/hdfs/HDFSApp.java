package com.cc.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Progressable;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * User: zhanghaichao
 * Date: 2018-09-10
 * Time: 下午4:26
 * Description:
 */
public class HDFSApp {
    FileSystem fileSystem = null;
    Configuration configuration = null;
    public static final String HDFS_PATH = "hdfs://localhost:9000";

    /**
     * 创建hdfs目录
     *
     * @throws IOException
     */
    @Test
    public void mkdir() throws IOException {
        fileSystem.mkdirs(new Path("/hdfs/api"));
    }

    /**
     * 创建文件
     *
     * @throws IOException
     */
    @Test
    public void create() throws IOException {
        FSDataOutputStream output = fileSystem.create(new Path("/hdfs/api/test.txt"));
        output.write("hello hadoop api".getBytes());
        output.flush();
        output.close();
    }

    /**
     * 查看HDFS内容
     *
     * @throws IOException
     */
    @Test
    public void cat() throws IOException {
        FSDataInputStream inputStream = fileSystem.open(new Path("/fz.txt"));
        IOUtils.copyBytes(inputStream, System.out, 1024);
        inputStream.close();
    }

    /**
     * 重命名HDFS文件
     *
     * @throws IOException
     */
    @Test
    public void rename() throws IOException {
        Path oldPath = new Path("/hdfs/api/test.txt");
        Path newPath = new Path("/hdfs/api/newtest.txt");
        fileSystem.rename(oldPath, newPath);
    }

    /**
     * 上传文件到HDFS
     *
     * @throws IOException
     */
    @Test
    public void copyFromLocalFile() throws IOException {
        Path localPath = new Path("/Users/zhanghaichao/hdu/1.cpp");
        Path hdfsPath = new Path("/hdfs/api/1.cpp");
        fileSystem.copyFromLocalFile(localPath, hdfsPath);
    }

    /**
     * 上传文件到HDFS,带进度条
     *
     * @throws IOException
     */
    @Test
    public void copyFromLocalFileWithProgress() throws IOException {
        Path hdfsPath = new Path("/hdfs/api/hadoop-2.8.4.tar.gz");

        InputStream inputStream = new BufferedInputStream(new FileInputStream(
                new File("/Users/zhanghaichao/Downloads/hadoop-2.8.4.tar.gz")));

        FSDataOutputStream outputStream = fileSystem.create(hdfsPath, new Progressable() {
            public void progress() {
                System.out.println(".");
            }
        });
        IOUtils.copyBytes(inputStream, outputStream, 4096);

    }

    /**
     * 下载HDFS文件
     *
     * @throws IOException
     */
    @Test
    public void copyToLocalFile() throws IOException {
        Path localPath = new Path("/Users/zhanghaichao/1.cpp");
        Path hdfsPath = new Path("/hdfs/api/1.cpp");
        fileSystem.copyToLocalFile(hdfsPath, localPath);
    }

    /**
     * 查看HDFS目录下的所有文件
     *
     * @throws IOException
     */
    @Test
    public void listFiles() throws IOException {
        Path hdfsPath = new Path("/");
        FileStatus[] fileStatuses = fileSystem.listStatus(hdfsPath);
        for (FileStatus fileStatus : fileStatuses) {
            String isDir  = fileStatus.isDirectory() ? " is directory," : " is file,";
            short replication = fileStatus.getReplication(); //hdfs shell 上传，副本系数使用配置文件中配置的，java api上传使用默认的（3）
            System.out.println(fileStatus.getPath().toString() + isDir
                    + "  replication is " + replication + "  length is :" + fileStatus.getLen());
        }
    }

    /**
     * 删除HDFS文件
     *
     * @throws IOException
     */
    @Test
    public void delete() throws IOException {
        Path hdfsPath = new Path("/fz");
        fileSystem.delete(hdfsPath,false); //默认递归删除，加参数控制
    }

    @Before
    public void setUp() throws URISyntaxException, IOException {
        configuration = new Configuration();
        fileSystem = FileSystem.get(new URI(HDFS_PATH), configuration);
        System.out.println("\n hdfs app setup\n");
    }

    @After
    public void tearDown() {
        configuration = null;
        fileSystem = null;

        System.out.println("\nhdfs app tear down\n");

    }
}
