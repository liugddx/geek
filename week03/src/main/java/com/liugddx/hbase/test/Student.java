/**
 * Copyright @ 2022科大讯飞。 All rights reserved.
 *
 * @author: gdliu3
 * @Date: 2022/3/19 18:51
 */
package com.liugddx.hbase.test;

import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * Copyright @ 2022科大讯飞。 All rights reserved.
 * @Title Student
 * @Project geek
 * @Description TODO
 * @author gdliu3
 * @date 2022/3/19 18:51
 */
@Data
@AllArgsConstructor
public class Student {

	private String name;

	private String studentId;

	private String clazz;

	private String understanding;

	private String programming;

}
