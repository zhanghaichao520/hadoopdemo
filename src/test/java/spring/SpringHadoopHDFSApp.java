package spring;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import java.io.IOException;
import java.net.URISyntaxException;

/**
 * User: zhanghaichao
 * Date: 2018-10-17
 * Time: 上午10:24
 * Description:Spring Hadoop访问HDFS文件系统
 */
public class SpringHadoopHDFSApp {
    private ApplicationContext ctx;
    private FileSystem fileSystem;


    /**
     * 创建hdfs目录
     *
     * @throws IOException
     */
    @Test
    public void mkdir() throws IOException {
        fileSystem.mkdirs(new Path("/springhdfs"));
    }

    /**
     * 删除HDFS文件
     *
     * @throws IOException
     */
    @Test
    public void delete() throws IOException {
        Path hdfsPath = new Path("/springhdfs");
        fileSystem.delete(hdfsPath,true); //默认递归删除，加参数控制
    }

    @Before
    public void setUp() throws URISyntaxException, IOException {
        ctx = new ClassPathXmlApplicationContext("beans.xml");
        fileSystem = (FileSystem) ctx.getBean("fileSystem");
        System.out.println("\n hdfs app setup\n");
    }

    @After
    public void tearDown() {
        ctx = null;
        fileSystem = null;

        System.out.println("\nhdfs app tear down\n");

    }
}
