/**
 * Copyright @ 2022科大讯飞。 All rights reserved.
 *
 * @author: gdliu3
 * @Date: 2022/4/9 12:43
 */
package org.example.stock;

import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.util.*;

/**
 * Copyright @ 2022科大讯飞。 All rights reserved.
 * @Title StockSource
 * @Project geek
 * @Description TODO
 * @author gdliu3
 * @date 2022/4/9 12:43
 */
public class StockSource extends RichSourceFunction<StockPrice> {


	private boolean isRunning = true;

	private Random rand = new Random();

	private List<Double> priceList = new ArrayList<>(Arrays.asList(100.0d, 200.0d, 300.0d, 400.0d, 500.0d));

	private Integer stockId = 0;

	private Double curPrice = 0.0d;




	@Override
	public void run(SourceContext<StockPrice> sourceContext) throws Exception {
		while (isRunning){
			stockId = rand.nextInt(priceList.size());
			curPrice = priceList.get(stockId) + rand.nextGaussian() * 0.05;

			priceList.add(stockId, curPrice);
			long curTime = Calendar.getInstance().getTimeInMillis();

			sourceContext.collect(new StockPrice("symbol_" + stockId.toString(), curTime, curPrice));

			Thread.sleep(rand.nextInt(10));
		}

	}

	@Override
	public void cancel() {

			isRunning = false;
	}
}
