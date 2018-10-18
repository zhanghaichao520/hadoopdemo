package spring;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.data.hadoop.fs.FsShell;

import java.net.URI;

/**
 * User: zhanghaichao
 * Date: 2018-10-18
 * Time: 下午4:33
 * Description:
 */
@SpringBootApplication
public class SpringBootHDFSApp implements CommandLineRunner {

    FsShell fsShell;
    FileSystem fileSystem = null;
    Configuration configuration = null;
    public static final String HDFS_PATH = "hdfs://localhost:9000";

    public void run(String... strings) throws Exception {
        configuration = new Configuration();
        fileSystem = FileSystem.get(new URI(HDFS_PATH), configuration);
        fsShell = new FsShell(configuration,fileSystem);
        for (FileStatus fileStatus : fsShell.lsr("/")) {
            System.out.println(fileStatus.getPath());
        }

    }

    public static void main(String[] args){
        SpringApplication.run(SpringBootHDFSApp.class,args);
    }
}
