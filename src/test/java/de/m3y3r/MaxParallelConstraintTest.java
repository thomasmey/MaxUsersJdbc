package de.m3y3r;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import javax.sql.DataSource;

import org.apache.commons.dbcp2.BasicDataSource;
import org.hamcrest.Matchers;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class MaxParallelConstraintTest {

	private DataSource ds;

	@Before
	public void setup() throws SQLException {
		BasicDataSource ds = new BasicDataSource();
		ds.setDriver(new org.h2.Driver());
		ds.setUrl("jdbc:h2:mem:db1");
//		ds.setUrl("jdbc:h2:file:~/db1");
//		ds.setDefaultTransactionIsolation(Connection.TRANSACTION_READ_UNCOMMITTED);
//		ds.setDefaultAutoCommit(false);
//		ds.setMinIdle(10);
		try(Connection c = ds.getConnection()) {
			c.createStatement().execute("create table users (id integer not null primary key, name varchar(32) )");
//			c.createStatement().execute("insert into users (id, name) values (1, 'tom')");
			c.createStatement().execute("truncate table users");
			c.commit();
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
//				System.out.println("isoSup="+ con.getMetaData().supportsTransactionIsolationLevel(Connection.TRANSACTION_READ_UNCOMMITTED));

				con.setAutoCommit(false);
				con.setTransactionIsolation(Connection.TRANSACTION_READ_UNCOMMITTED);
//				con.commit();

				PreparedStatement psIns = con.prepareStatement("insert into users (id, name) values (?, ?)");
				PreparedStatement psCheck = con.prepareStatement("select count(*) from users");

				int id = r.nextInt();
				psIns.setInt(1, id);
				psIns.setString(2, "name-of-user-" + id);
				int cnt = psIns.executeUpdate();
//				System.out.println("INS " + System.nanoTime() + '-' + Thread.currentThread() + " cnt=" + cnt);
//				psIns.close();

//				con.setTransactionIsolation(Connection.TRANSACTION_READ_UNCOMMITTED);
				ResultSet rs = psCheck.executeQuery();
				if(rs.next()) {
					int noUsers = rs.getInt(1);
//					System.out.println("CHK " + System.nanoTime() + '-' + Thread.currentThread() + " noUsers="+noUsers);
					if(noUsers > maxNoUsers) {
						System.out.println("max users " + noUsers + " reached rollback");
						con.rollback();
						return;
					}
				}
				rs.close();
//				psCheck.close();

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
