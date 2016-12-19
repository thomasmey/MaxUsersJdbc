package de.m3y3r;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.h2.jdbcx.JdbcDataSource;
import org.hamcrest.Matchers;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class MaxParallelConstraintTest {

	private JdbcDataSource ds;

	@Before
	public void setup() throws SQLException {
		JdbcDataSource ds = new JdbcDataSource();
		ds.setUrl("jdbc:h2:mem:db1;DB_CLOSE_DELAY=-1");
		try(Connection c = ds.getConnection()) {
			c.createStatement().execute("create table users (id integer not null primary key, name varchar(32) )");
		}
		this.ds = ds;
	}

	@Test
	public void testMaxConstraint() throws SQLException, InterruptedException {
		final int maxInserts = 10;
		final int maxNoUsers = 5;

		ExecutorService exec = Executors.newFixedThreadPool(maxInserts);
		final CountDownLatch cdl = new CountDownLatch(maxInserts);

		final Random r = new Random();
		Runnable code = () -> {
			try(Connection con = ds.getConnection()) {
				int id = r.nextInt();
				con.setAutoCommit(false);
//				con.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);

				PreparedStatement psIns = con.prepareStatement("insert into users (id, name) values (?, ?)");
				PreparedStatement psCheck = con.prepareStatement("select count(*) from users");

				psIns.setInt(1, id);
				psIns.setString(2, "name-of-user-" + id);
				psIns.execute();

				ResultSet rs = psCheck.executeQuery();
				if(rs.next()) {
					int noUsers = rs.getInt(1);
					System.out.println("thread=" + Thread.currentThread() + " noUsers="+noUsers);
					if(noUsers > maxNoUsers) {
						System.out.println("max users reached rollback");
						con.rollback();
						return;
					}
				}
				con.commit();
			} catch (SQLException e) {
				e.printStackTrace();
			} finally {
				cdl.countDown();
			}
		};
		for(int i=0,n=maxInserts; i < n; i++) exec.execute(code);
		cdl.await();
		exec.shutdown();

		int noUsers = 0;
		try(Connection con = ds.getConnection()) {
			PreparedStatement psCheck = con.prepareStatement("select count(*) from users");
			ResultSet rs = psCheck.executeQuery();
			if(rs.next()) noUsers = rs.getInt(1);
		}
		Assert.assertThat(noUsers, Matchers.lessThanOrEqualTo(maxNoUsers));
	}
}
