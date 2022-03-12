package mr; /**
 * Copyright @ 2022科大讯飞。 All rights reserved.
 *
 * @author: gdliu3
 * @Date: 2022/3/12 19:57
 */

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Copyright @ 2022科大讯飞。 All rights reserved.
 * @Title TrafficMapper
 * @Project mr-demo
 * @Description TODO
 * @author gdliu3
 * @date 2022/3/12 19:57
 */
public class TrafficMapper extends Mapper<Object, Text,Text,PhoneTraffic> {

	public void map(Object key,Text value,Context context) throws IOException, InterruptedException {
		String[] lines = value.toString().split("\t");
		if (lines.length<10){
			return;
		}

		String phone = lines[1];

		try {
			long up = Long.parseLong(lines[8]);
			long down = Long.parseLong(lines[9]);
			context.write(new Text(phone), new PhoneTraffic(up,down,up+down));
		}catch (NumberFormatException e){
			System.err.println("parseLong failed" + e.getMessage());
		}

	}

}
