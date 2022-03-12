package mr; /**
 * Copyright @ 2022科大讯飞。 All rights reserved.
 *
 * @author: gdliu3
 * @Date: 2022/3/12 19:57
 */

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Copyright @ 2022科大讯飞。 All rights reserved.
 * @Title PhoneTraffic
 * @Project mr-demo
 * @Description TODO
 * @author gdliu3
 * @date 2022/3/12 19:57
 */

@Data
@AllArgsConstructor
@NoArgsConstructor
public class PhoneTraffic implements Writable {

	private long up;

	private long down;

	private long sum;

	@Override
	public void write(DataOutput dataOutput) throws IOException {
		dataOutput.writeLong(up);
		dataOutput.writeLong(down);
		dataOutput.writeLong(sum);

	}

	@Override
	public void readFields(DataInput dataInput) throws IOException {
		this.up = dataInput.readLong();
		this.down = dataInput.readLong();
		this.sum = dataInput.readLong();
	}
}
