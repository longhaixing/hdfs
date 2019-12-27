package reducejoin;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class joinDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Job job = Job.getInstance(new Configuration());

        job.setJarByClass(joinDriver.class);

        job.setMapperClass(joinMapper.class);
        job.setReducerClass(joinReducer.class);

        job.setMapOutputKeyClass(orderBean.class);
        job.setMapOutputValueClass(NullWritable.class);

        job.setOutputKeyClass(orderBean.class);
        job.setOutputValueClass(NullWritable.class);

        job.setGroupingComparatorClass(joinComparator.class);

        FileInputFormat.setInputPaths(job, new Path("d:\\111"));
        FileOutputFormat.setOutputPath(job, new Path("d:\\111"));

        boolean b = job.waitForCompletion(true);
        System.exit(b ? 0 : 1);
    }
}
