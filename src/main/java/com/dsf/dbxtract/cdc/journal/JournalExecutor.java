/**
 * Copyright 2016 Fabio De Santi
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.dsf.dbxtract.cdc.journal;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.zookeeper.KeeperException.NoNodeException;

import com.dsf.dbxtract.cdc.Data;
import com.dsf.dbxtract.cdc.Source;
import com.dsf.utils.sql.NamedParameterStatement;

/**
 * This class is specialized on journal-based change-data-capture.
 * 
 * @author fabio de santi
 *
 */
public class JournalExecutor implements Runnable {

	private static final Logger logger = LogManager.getLogger(JournalExecutor.class.getName());

	private static final String BASEPREFIX = "/dbxtract/cdc/";

	private static Map<Source, BasicDataSource> dataSources = new HashMap<Source, BasicDataSource>();
	private String zookeeper;
	private JournalHandler handler;
	private Source source;
	private String agentName;
	private String prefix;
	private List<String> journalColumns = null;

	/**
	 * 
	 * @param zookeeper
	 *            connection string to ZooKeeper server
	 * @param handler
	 *            {@link JournalHandler}
	 * @param source
	 *            {@link Source}
	 */
	public JournalExecutor(String agentName, String zookeeper, JournalHandler handler, Source source) {
		logger.info(agentName + " :: Creating executor for " + handler + " and " + source);
		this.agentName = agentName;
		this.zookeeper = zookeeper;
		this.handler = handler;
		this.source = source;
		BasicDataSource ds = dataSources.get(source);
		if (ds == null) {
			logger.info(agentName + " :: setting up a connection pool for " + source.toString());
			ds = new BasicDataSource();
			ds.setDriverClassName(source.getDriver());
			ds.setUsername(source.getUser());
			ds.setPassword(source.getPassword());
			ds.setUrl(source.getConnection());
			dataSources.put(source, ds);
		}
	}

	private Connection getConnection() throws SQLException {
		return dataSources.get(source).getConnection();
	}

	private String getPrefix() {
		if (prefix == null) {
			prefix = new StringBuilder(BASEPREFIX).append(source.getName()).append('/')
					.append(handler.getJournalTable()).append('/').toString();
		}
		return prefix;
	}

	private Long getLastWindowId(CuratorFramework client) throws Exception {

		try {
			String s = new String(client.getData().forPath(getPrefix() + "lastWindowId"));
			return s == null ? 0L : Long.parseLong(s);

		} catch (NoNodeException nne) {
			return 0L;
		}
	}

	/**
	 * Gets reference data from journal table.
	 * 
	 * @param client
	 * @param conn
	 * @return a Map list with column names and values
	 * @throws SQLException
	 */
	private List<Map<String, Object>> getJournalKeys(CuratorFramework client, Connection conn)
			throws SQLException, Exception {

		List<Map<String, Object>> result = new ArrayList<Map<String, Object>>();
		PreparedStatement ps = null;
		ResultSet rs = null;
		try {
			// Obtem os dados do journal
			logger.debug(agentName + " :: getting journalized data");
			String baseQuery = "select * from " + handler.getJournalTable();
			if (JournalStrategy.DELETE.equals(handler.getStrategy())) {
				ps = conn.prepareStatement(baseQuery);
			} else {
				Long lastWindowId = getLastWindowId(client);
				ps = conn.prepareStatement(baseQuery + " where window_id > ?");
				ps.setLong(1, lastWindowId);
			}
			ps.setFetchSize(handler.getBatchSize());
			ps.setMaxRows(handler.getBatchSize());
			rs = ps.executeQuery();
			while (rs.next()) {
				if (journalColumns == null) {
					journalColumns = new ArrayList<String>();
					for (int i = 0; i < rs.getMetaData().getColumnCount(); i++) {
						journalColumns.add(rs.getMetaData().getColumnLabel(i + 1).toLowerCase());
					}
				}
				Map<String, Object> map = new HashMap<String, Object>();
				for (String col : journalColumns) {
					map.put(col, rs.getObject(col));
				}
				result.add(map);
			}
			rs.close();
			ps.close();

		} finally {
			try {
				if (rs != null)
					rs.close();
				if (ps != null)
					ps.close();
			} catch (Exception e) {
				logger.error("Failed to get journal new keys", e);
			}
		}
		return result;
	}

	/**
	 * Retrieves updated data and publishes somewhere.
	 * 
	 * @param conn
	 * @param rows
	 * @throws SQLException
	 * @throws IOException
	 * @throws Exception
	 */
	private void selectAndPublish(Connection conn, List<Map<String, Object>> rows)
			throws SQLException, IOException, Exception {

		if (rows.isEmpty()) {
			logger.debug(agentName + " :: nothing to load");
			return;
		}
		logger.debug(agentName + " :: getting data");
		String query = handler.getTargetQuery();
		NamedParameterStatement ps = null;
		ResultSet rs = null;
		try {
			Data data = null;
			ps = new NamedParameterStatement(conn, query);
			for (Map<String, Object> keys : rows) {
				for (String k : keys.keySet()) {
					if (query.contains(":" + k)) {
						ps.setObject(k, keys.get(k));
					}
				}
				if (rs != null)
					rs.close();
				rs = ps.executeQuery();
				if (data == null) {
					int cols = rs.getMetaData().getColumnCount();
					String[] colNames = new String[cols];
					for (int i = 0; i < cols; i++) {
						colNames[i] = rs.getMetaData().getColumnLabel(i + 1);
					}
					data = new Data(colNames);
				}
				while (rs.next()) {
					data.append(rs);
				}
			}
			handler.publish(data);

		} finally {
			if (rs != null)
				rs.close();
			if (ps != null)
				ps.close();
		}
	}

	/**
	 * Removes imported references from journal table.
	 * 
	 * @param conn
	 * @param rows
	 * @throws SQLException
	 */
	private void deleteFromJournal(Connection conn, List<Map<String, Object>> rows) throws SQLException {

		if (rows.isEmpty()) {
			logger.debug(agentName + " :: nothing to clean");
			return;
		}
		logger.debug("cleaning journal " + handler.getJournalTable());
		StringBuilder sb = new StringBuilder("delete from " + handler.getJournalTable() + " where ");
		for (int i = 0; i < journalColumns.size(); i++) {
			sb.append(i > 0 ? " and " : "").append(journalColumns.get(i)).append("=?");
		}
		PreparedStatement ps = null;
		try {
			ps = conn.prepareStatement(sb.toString());
			for (Map<String, Object> keys : rows) {
				for (int i = 0; i < journalColumns.size(); i++) {
					ps.setObject(i + 1, keys.get(journalColumns.get(i)));
				}
				ps.addBatch();
			}
			ps.executeBatch();
			logger.info(agentName + " :: " + rows.size() + " rows removed (" + handler.getJournalTable() + ")");

		} finally {
			if (ps != null)
				ps.close();
		}
	}

	/**
	 * Memorizes the last imported window_id from journal table
	 * 
	 * @param rows
	 */
	private void markLastLoaded(CuratorFramework client, List<Map<String, Object>> rows) throws Exception {

		if (rows == null || rows.isEmpty())
			return;

		Long lastWindowId = 0L;
		for (Map<String, Object> row : rows) {
			Number windowId = (Number) row.get("window_id");
			if (windowId.longValue() > lastWindowId.longValue()) {
				lastWindowId = windowId.longValue();
			}
		}

		String k = getPrefix() + "lastWindowId";
		String s = lastWindowId.toString();
		try {
			if (client.checkExists().forPath(k) == null)
				client.create().forPath(k);

			client.setData().forPath(k, s.getBytes());
		} catch (Exception e) {
			logger.error("failed to update last window_id", e);
		}
	}

	/**
	 * Gets from journal table any update, executes the query to retrieve data,
	 * publishes to somewhere and removes imported data from journal.
	 *
	 * This routine is controlled by ZooKeeper that ensures only one node will
	 * try to import a specific table's data at time.
	 * 
	 */
	@Override
	public void run() {

		logger.debug(agentName + " :: connecting to " + zookeeper);
		RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);
		CuratorFramework client = CuratorFrameworkFactory.newClient(zookeeper, retryPolicy);
		client.start();

		// Uses the distributed lock recipe of ZooKeeper to avoid concurrency
		Connection conn = null;
		String lockPath = getPrefix() + "lock";
		InterProcessMutex lock = new InterProcessMutex(client, lockPath);
		logger.debug(agentName + " :: waiting lock from " + zookeeper + lockPath);
		try {
			if (lock.acquire(5, TimeUnit.SECONDS)) {
				try {
					logger.debug(agentName + " :: get database connection");
					conn = getConnection();

					// Get journal data
					List<Map<String, Object>> rows = getJournalKeys(client, conn);

					// Retrieve changed data and publish it
					selectAndPublish(conn, rows);

					if (JournalStrategy.DELETE.equals(handler.getStrategy())) {
						// Remove from journal imported & published data
						deleteFromJournal(conn, rows);

					} else {
						// Update last loaded window_id
						markLastLoaded(client, rows);
					}

				} finally {
					if (conn != null) {
						try {
							conn.close();
						} catch (Exception e) {
						}
					}
					logger.debug(agentName + " :: lock release");
					lock.release();
					client.close();
				}
			}

		} catch (Exception e) {
			logger.error(agentName + " :: failure", e);
		}
	}
}
