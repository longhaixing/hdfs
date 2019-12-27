package reducejoin;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class joinComparator extends WritableComparator {

    protected joinComparator() {
        super(orderBean.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        orderBean oa = (orderBean) a;
        orderBean ob = (orderBean) b;
        return oa.getPid().compareTo(ob.getPid());
    }
}
