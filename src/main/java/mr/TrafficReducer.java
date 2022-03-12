package mr; /**
 * Copyright @ 2022科大讯飞。 All rights reserved.
 *
 * @author: gdliu3
 * @Date: 2022/3/12 20:04
 */

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.awt.*;
import java.io.IOException;

/**
 * Copyright @ 2022科大讯飞。 All rights reserved.
 * @Title TrafficReducer
 * @Project mr-demo
 * @Description TODO
 * @author gdliu3
 * @date 2022/3/12 20:04
 */
public class TrafficReducer extends Reducer<Text,PhoneTraffic,Text,PhoneTraffic> {

	@Override
	public void reduce(Text key,Iterable<PhoneTraffic> values,Context context) throws IOException, InterruptedException {
			int totalUp = 0;
			int totalDown = 0;
			int sumTraffic = 0;

			for (PhoneTraffic phoneTraffic: values){
				totalUp+=phoneTraffic.getUp();
				totalDown+=phoneTraffic.getDown();
				sumTraffic+=phoneTraffic.getSum();
			}

			context.write(key,new PhoneTraffic(totalUp,totalDown,sumTraffic));

	}
}
