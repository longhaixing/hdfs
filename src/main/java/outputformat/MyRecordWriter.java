package outputformat;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;

public class MyRecordWriter extends RecordWriter<LongWritable, Text> {

    // 首先进行本地文件开流
    private FileOutputStream atguigu;
    private FileOutputStream other;

    // Hadoop文件流
//    private FSDataOutputStream atguigu;
//    private FSDataOutputStream other;

    // 需要手动初始化
    public void initialize() throws FileNotFoundException {
        atguigu = new FileOutputStream("d:\\atguigu.log");
        other = new FileOutputStream("d:\\other.log");
    }

    // 可以通过job获取设置输出信息
//    public void initialize1(TaskAttemptContext job) throws IOException {
//        String outdir = job.getConfiguration().get(FileOutputFormat.OUTDIR);
//        // 获取路径后开流
//        FileSystem fileSystem = FileSystem.get(job.getConfiguration());
//        atguigu = fileSystem.create(new Path(outdir + "/atguigu.log"));
//        other = fileSystem.create(new Path(outdir + "/other.log"));
//    }

    @Override
    public void write(LongWritable longWritable, Text text) throws IOException, InterruptedException {
        String out = text.toString() + "\n";
        if (out.contains("atguigu")) {
            atguigu.write(out.getBytes());
        }else {
            other.write(out.getBytes());
        }
    }

    @Override
    public void close(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        IOUtils.closeStream(atguigu);
        IOUtils.closeStream(other);
    }
}
