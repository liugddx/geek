/**
 * Copyright @ 2022科大讯飞。 All rights reserved.
 *
 * @author: gdliu3
 * @Date: 2022/4/9 15:11
 */
package org.example.stock;

import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * Copyright @ 2022科大讯飞。 All rights reserved.
 * @Title StockPriceTimeAssigner
 * @Project geek
 * @Description TODO
 * @author gdliu3
 * @date 2022/4/9 15:11
 */
public class StockPriceTimeAssigner extends BoundedOutOfOrdernessTimestampExtractor<StockPrice> {

	public StockPriceTimeAssigner(Time maxOutOfOrderness) {
		super(maxOutOfOrderness);
	}

	@Override
	public long extractTimestamp(StockPrice stockPrice) {
		return stockPrice.ts;
	}
}
