package HDFSClient;

import com.google.inject.internal.util.$SourceProvider;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.FileInputStream;
import java.io.IOException;
import java.net.URI;

public class Client {

    private FileSystem fs;

    @Before
    public void before() throws IOException, InterruptedException {
        fs = FileSystem.get(URI.create("hdfs://192.168.56.100:9000"), new Configuration(), "long");
        System.out.println("Before!");
    }

    @Test
    public void put() throws IOException, InterruptedException {
        FileSystem fileSystem = FileSystem.get(URI.create("hdfs://192.168.56.100:9000"), new Configuration(), "long");

        fileSystem.copyFromLocalFile(new Path("D:/report_data2.txt"), new Path("/"));

        fileSystem.close();
    }

    @Test
    public void rename() throws IOException, InterruptedException {
        FileSystem fileSystem = FileSystem.get(URI.create("hdfs://192.168.56.100:9000"), new Configuration(), "long");

        fileSystem.rename(new Path("/report_data2.txt"), new Path("/test.txt"));

        fileSystem.close();
    }

    @Test
    public void du() throws IOException {
        FSDataOutputStream append = fs.append(new Path("/test.txt"), 512);
        FileInputStream open = new FileInputStream("D:/test.txt");

        IOUtils.copyBytes(open, append, 1024, true);
    }

    @Test
    public void listFiles() throws IOException {
        RemoteIterator<LocatedFileStatus> files = fs.listFiles(new Path("/"), true);

        while (files.hasNext()) {
            LocatedFileStatus file = files.next();

            System.out.println(file.getPath());

            System.out.println("块信息");
            BlockLocation[] blockLocations = file.getBlockLocations();
            for (BlockLocation blockLocation:blockLocations){
                String[] hosts = blockLocation.getHosts();
                System.out.println("位置：");
                for (String host:hosts) {
                    System.out.println(host + " ");
                }
            }
        }
    }

    @Test
    public void ls() throws IOException {
        FileStatus[] fileStatuses = fs.listStatus(new Path("/"));

        for (FileStatus fileStatus:fileStatuses){
            if (fileStatus.isFile()) {
                System.out.println("这是一个文件");
                System.out.println(fileStatus.getPath());
                System.out.println(fileStatus.getLen());
            }else {
                System.out.println("这是一个文件夹");
                System.out.println(fileStatus.getPath());
            }
        }
    }

    @After
    public void after() throws IOException {
        fs.close();
        System.out.println("After!");
    }
}
