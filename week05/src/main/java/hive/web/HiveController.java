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
import org.springframework.web.bind.annotation.*;

import javax.sql.DataSource;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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

	@GetMapping("/topic1/{movieId}")
	public List<Map> topic1(@PathVariable("movieId") String movieId) {
		List<Map> list = new ArrayList<>();
		PreparedStatement statement = null;
		try {
			String sql = String.format("select a.age , AVG(b.rate) as avgRate from t_user a join t_rating b on  a.userid=b.userid  where b.movieid=%s group by age;",movieId);
			statement = druidDataSource.getConnection().prepareStatement(sql);
			logger.info("Running: " + sql);
			ResultSet res = statement.executeQuery(sql);
			while (res.next()) {
				Map<String,String> map = new HashMap<>();
				map.put("age",res.getString(1));
				map.put("avgrate",res.getString(2));
				list.add(map);
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return list;
	}

		@GetMapping("/topic2/")
		public List<Map> topic2(@RequestParam("numOfRate") Integer numOfRate, @RequestParam("numOfMovie") Integer numOfMovie) {
		List<Map> list = new ArrayList<>();
		PreparedStatement statement = null;
		try {
			String sql = String.format("select m.moviename as moviename,avg(r.rate) as avgRate,count(*) total  from t_rating r join t_user u on u.userid=r.userid join t_movie m on r.movieid=m.movieid where u.sex='M' group by u.sex,m.moviename having total>%d order by avgRate desc limit %d",numOfRate,numOfMovie);
			statement = druidDataSource.getConnection().prepareStatement(sql);
			logger.info("Running: " + sql);
			ResultSet res = statement.executeQuery(sql);
			while (res.next()) {
				Map<String,String> map = new HashMap<>();
				map.put("moviename",res.getString(1));
				map.put("avgrate",res.getString(2));
				map.put("total",res.getString(3));
				list.add(map);
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return list;
	}

	@GetMapping("/topic3/")
	public List<Map> topic3(@RequestParam("sex") String sex, @RequestParam("numOfMovie") Integer numOfMovie) {
		List<Map> list = new ArrayList<>();
		PreparedStatement statement = null;
		try {
			String sql = String.format("select m.moviename as moviename , avg(r.rate) as avgRate " +
					"from t_rating r join t_movie m on r.movieid = m.movieid join" +
					"( select r.movieid from t_rating r join (select u.userid,count(*) " +
					"as num from t_user u join t_rating r on u.userid = r.userid " +
					"where u.sex='%s' group by u.userid order by num desc limit 1) u " +
					"on r.userid = u.userid order by r.rate desc,movieid limit %d) mm " +
					"on r.movieid = mm.movieid group by m.moviename;",sex,numOfMovie);
			statement = druidDataSource.getConnection().prepareStatement(sql);
			logger.info("Running: " + sql);
			ResultSet res = statement.executeQuery(sql);
			while (res.next()) {
				Map<String,String> map = new HashMap<>();
				map.put("moviename",res.getString(1));
				map.put("avgrate",res.getString(2));
				list.add(map);
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return list;
	}


}
