/**
 * Copyright @ 2022科大讯飞。 All rights reserved.
 *
 * @author: gdliu3
 * @Date: 2022/4/9 14:32
 */
package org.example.stock;

import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * Copyright @ 2022科大讯飞。 All rights reserved.
 * @Title StockJob
 * @Project geek
 * @Description TODO
 * @author gdliu3
 * @date 2022/4/9 14:32
 */
public class StockJob {

	public static void main(String[] args) throws Exception {

		// 设置执行环境
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

		// 每5秒生成一个Watermark
		env.getConfig().setAutoWatermarkInterval(5000L);

		// 股票价格数据流
		DataStream<StockPrice> stock = env
				// 该数据流由StockPriceSource类随机生成
				.addSource(new StockSource())
				.name("stock")
				// 设置 Timestamp 和 Watermark
				.assignTimestampsAndWatermarks(new StockPriceTimeAssigner(Time.seconds(5)));

		stock.keyBy(stockPrice -> stockPrice.symbol)
				// 设置5秒的时间窗口
				.window(TumblingEventTimeWindows.of(Time.seconds(5)))
				// 取5秒内某一只股票的最大值
				.max("price");

		stock.print();

		env.execute("Compute max stock price");
	}
}
