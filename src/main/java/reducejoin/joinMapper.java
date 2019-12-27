package reducejoin;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

public class joinMapper extends Mapper<LongWritable, Text, orderBean, NullWritable> {

    private orderBean OrderBean = new orderBean();

    private String filename;

    /**
     * Map执行前先执行setup，一个文件对应一个maptask,所以在这里读取一次文件
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        FileSplit fs = (FileSplit) context.getInputSplit();
        filename = fs.getPath().getName();
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields = value.toString().split("\t");
        if (filename.equals("order.txt")) {
            OrderBean.setId(fields[0]);
            OrderBean.setPid(fields[1]);
            OrderBean.setAmount(Integer.parseInt(fields[2]));
            OrderBean.setPname("");     // 不用的时候要覆盖
        }else {
            OrderBean.setPid(fields[0]);
            OrderBean.setPname(fields[1]);
            OrderBean.setId("");
            OrderBean.setAmount(0);
        }
        context.write(OrderBean, NullWritable.get());
    }
}
