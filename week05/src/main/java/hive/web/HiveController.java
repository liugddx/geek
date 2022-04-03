/**
 * Copyright @ 2022科大讯飞。 All rights reserved.
 *
 * @author: gdliu3
 * @Date: 2022/4/3 20:33
 */
package hive.web;

import lombok.extern.slf4j.Slf4j;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import javax.sql.DataSource;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

/**
 * Copyright @ 2022科大讯飞。 All rights reserved.
 * @Title HiveController
 * @Project geek
 * @Description TODO
 * @author gdliu3
 * @date 2022/4/3 20:33
 */
@RestController
@RequestMapping("/hive")
@Slf4j
public class HiveController {

	public static final Logger logger = LoggerFactory.getLogger(HiveController.class);

	@Autowired
	@Qualifier("hiveDruidDataSource")
	private  DataSource jdbcDataSource;

	@Autowired
	@Qualifier("hiveDruidDataSource")
	private DataSource druidDataSource;

	@Autowired
	@Qualifier("hiveDruidTemplate")
	private JdbcTemplate jdbcTemplate;

	@RequestMapping("/table/show")
	public List<String> showtables() {
		List<String> list = new ArrayList<String>();
		Statement statement = null;
		try {
			statement = druidDataSource.getConnection().createStatement();
			String sql = "show tables";
			logger.info("Running: " + sql);
			ResultSet res = statement.executeQuery(sql);
			while (res.next()) {
				list.add(res.getString(1));
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return list;
	}



}
