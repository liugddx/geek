package mr; /**
 * Copyright @ 2022科大讯飞。 All rights reserved.
 *
 * @author: gdliu3
 * @Date: 2022/3/12 20:17
 */

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;
import java.util.Arrays;

/**
 * Copyright @ 2022科大讯飞。 All rights reserved.
 * @Title PhoneTrafficMain
 * @Project mr-demo
 * @Description TODO
 * @author gdliu3
 * @date 2022/3/12 20:17
 */
public class PhoneTrafficMain {

	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf,args).getRemainingArgs();
		if (otherArgs.length<2){
			System.err.println("Usage: PhoneTrafficMain <in> <out>");
			System.exit(2);
		}

		System.out.println("otherArgs: " + Arrays.toString(otherArgs));

		Job job = Job.getInstance(conf,"PhoneTrafficMain");
		job.setJarByClass(PhoneTrafficMain.class);
		job.setMapperClass(TrafficMapper.class);
		job.setCombinerClass(TrafficReducer.class);
		job.setReducerClass(TrafficReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(PhoneTraffic.class);

		job.setNumReduceTasks(1);

		FileInputFormat.addInputPath(job,new Path(otherArgs[otherArgs.length-2]));
		FileOutputFormat.setOutputPath(job,new Path(otherArgs[otherArgs.length-1]));
		System.exit(job.waitForCompletion(true)?0:1);


	}
}
