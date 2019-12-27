package reducejoin;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

public class joinReducer extends Reducer<orderBean, NullWritable, orderBean, NullWritable> {

    @Override
    protected void reduce(orderBean key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
        Iterator<NullWritable> iterator = values.iterator();
        iterator.next();
        String pname = key.getPname();
        while (iterator.hasNext()) {
            iterator.next();
            key.setPname(pname);
            context.write(key, NullWritable.get());
        }
    }
}
