/**
 * Copyright @ 2022科大讯飞。 All rights reserved.
 *
 * @author: gdliu3
 * @Date: 2022/4/9 12:43
 */
package org.example.stock;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Copyright @ 2022科大讯飞。 All rights reserved.
 * @Title StockPrice
 * @Project geek
 * @Description TODO
 * @author gdliu3
 * @date 2022/4/9 12:43
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class StockPrice {

	public String symbol;
	public long ts;
	public double price;

}
