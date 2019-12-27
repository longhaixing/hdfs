package flow;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class flowReducer extends Reducer<Text, flowBean, Text, flowBean> {

    private flowBean sumFlow = new flowBean();

    @Override
    protected void reduce(Text key, Iterable<flowBean> values, Context context) throws IOException, InterruptedException {
        long sumUpFlow = 0;
        long sumDownFlow = 0;
        for (flowBean value : values) {
            sumUpFlow += value.getUpFlow();
            sumDownFlow += value.getDownFlow();
        }
        sumFlow.set(sumUpFlow, sumDownFlow);
        context.write(key, sumFlow);
    }
}
