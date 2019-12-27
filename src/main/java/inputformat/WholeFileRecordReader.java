package inputformat;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

/**
 * 处理一个文件为一个KV值
 */
public class WholeFileRecordReader extends RecordReader<Text, BytesWritable> {

    private boolean notRead = true;

    private Text key = new Text();
    private BytesWritable value = new BytesWritable();

    private FSDataInputStream inputStream;
    private FileSplit fs;

    /**
     * 初始化，被调用时使用
     *
     * @param inputSplit
     * @param taskAttemptContext
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        FileSplit fs = (FileSplit) inputSplit;
        // 通过切片获取文件路径
        Path path = fs.getPath();
        // 获取文件系统
        FileSystem fileSystem = path.getFileSystem(taskAttemptContext.getConfiguration());
        // 开流
        inputStream = fileSystem.open(path);
    }

    /**
     * 读取下一组KV值
     *
     * @return 读到为true,读完了false
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        if (notRead){

            // key
            key.set(fs.getPath().toString());
            // value 文件长度
            byte[] buf = new byte[(int) fs.getLength()];
            inputStream.read(buf);
            value.set(buf, 0, buf.length);

            notRead = false;
            return true;
        }else {
            return false;
        }
    }

    @Override
    public Text getCurrentKey() throws IOException, InterruptedException {
        return key;
    }


    @Override
    public BytesWritable getCurrentValue() throws IOException, InterruptedException {
        return value;
    }

    /**
     * 当前读取进度
     *
     * @return 进度
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public float getProgress() throws IOException, InterruptedException {
        return notRead ? 0 : 1;
    }

    @Override
    public void close() throws IOException {
        IOUtils.closeStream(inputStream);
    }
}
