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
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException.NoNodeException;

import com.dsf.dbxtract.cdc.App;
import com.dsf.dbxtract.cdc.ConfigurationException;
import com.dsf.dbxtract.cdc.Data;
import com.dsf.dbxtract.cdc.PublishException;
import com.dsf.dbxtract.cdc.Source;
import com.dsf.dbxtract.cdc.mon.Statistics;
import com.dsf.utils.sql.DBUtils;
import com.dsf.utils.sql.NamedParameterStatement;

/**
 * This class is specialized on journal-based change-data-capture.
 * 
 * @author fabio de santi
 *
 */
public class JournalExecutor implements Runnable {

	private static final Logger logger = LogManager.getLogger(JournalExecutor.class.getName());

	private Map<Source, BasicDataSource> dataSources = new HashMap<>();
	private Statistics statistics = null;
	private String zookeeper;
	private JournalHandler handler;
	private Source source;
	private String agentName;
	private String prefix;
	private String logPrefix;
	private List<String> journalColumns = null;

	/**
	 * @param agentName
	 *            cdc agent's assigned name
	 * @param zookeeper
	 *            connection string to ZooKeeper server
	 * @param handler
	 *            {@link JournalHandler}
	 * @param source
	 *            {@link Source}
	 */
	public JournalExecutor(String agentName, String zookeeper, JournalHandler handler, Source source) {
		logPrefix = agentName + " :: ";
		if (logger.isDebugEnabled())
			logger.debug(logPrefix + "Creating executor for " + handler + " and " + source);
		this.agentName = agentName;
		this.zookeeper = zookeeper;
		this.handler = handler;
		this.source = source;
		BasicDataSource ds = dataSources.get(source);
		if (ds == null) {
			if (logger.isDebugEnabled())
				logger.debug(agentName + " :: setting up a connection pool for " + source.toString());
			ds = new BasicDataSource();
			ds.setDriverClassName(source.getDriver());
			ds.setUsername(source.getUser());
			ds.setPassword(source.getPassword());
			ds.setUrl(source.getConnection());
			dataSources.put(source, ds);
		}

		if (statistics == null)
			statistics = new Statistics();
	}

	private Connection getConnection() throws SQLException {
		return dataSources.get(source).getConnection();
	}

	private String getPrefix() {
		if (prefix == null) {
			prefix = new StringBuilder(App.BASEPREFIX).append('/').append(source.getName()).append('/')
					.append(handler.getJournalTable()).toString();
		}
		return prefix;
	}

	private Long getLastWindowId(CuratorFramework client) throws ConfigurationException {

		try {
			byte[] b = client.getData().forPath(getPrefix() + "/lastWindowId");
			if (b == null)
				return 0L;

			return Long.parseLong(new String(b));

		} catch (NoNodeException nne) {
			logger.warn(nne);
			return 0L;

		} catch (Exception e) {
			throw new ConfigurationException("Failed to access zk entry", e);
		}
	}

	/**
	 * Gets reference data from journal table.
	 * 
	 * @param client
	 * @param conn
	 * @return a Map list with column names and values
	 * @throws SQLException
	 * @throws ConfigurationException
	 */
	private List<Map<String, Object>> getJournalKeys(CuratorFramework client, Connection conn)
			throws SQLException, ConfigurationException {

		List<Map<String, Object>> result = new ArrayList<>();
		PreparedStatement ps = null;
		ResultSet rs = null;
		try {
			// Obtem os dados do journal
			if (logger.isDebugEnabled())
				logger.debug(logPrefix + "getting journalized data");
			StringBuilder baseQuery = new StringBuilder("select * from ").append(handler.getJournalTable());
			if (JournalStrategy.WINDOW.equals(handler.getStrategy())) {
				Long lastWindowId = getLastWindowId(client);
				baseQuery.append(" where window_id > ?");
				ps = conn.prepareStatement(baseQuery.toString());
				ps.setLong(1, lastWindowId);
			} else {
				ps = conn.prepareStatement(baseQuery.toString());
			}
			ps.setFetchSize(handler.getBatchSize());
			ps.setMaxRows(handler.getBatchSize());
			rs = ps.executeQuery();
			copyResultsetToMap(rs, result);

		} finally {
			DBUtils.close(rs);
			DBUtils.close(ps);
		}
		return result;
	}

	private void copyResultsetToMap(ResultSet rs, List<Map<String, Object>> result) throws SQLException {

		if (rs == null)
			throw new SQLException("result is null");
		if (result == null)
			throw new NullPointerException("result map is null");

		while (rs.next()) {
			if (journalColumns == null) {
				journalColumns = new ArrayList<>();
				for (int i = 0; i < rs.getMetaData().getColumnCount(); i++) {
					journalColumns.add(rs.getMetaData().getColumnLabel(i + 1).toLowerCase());
				}
			}
			Map<String, Object> map = new HashMap<>();
			for (String col : journalColumns) {
				map.put(col, rs.getObject(col));
			}
			result.add(map);
		}
	}

	/**
	 * Retrieves updated data and publishes somewhere.
	 * 
	 * @param conn
	 * @param rows
	 * @throws SQLException
	 * @throws IOException
	 * @throws PublishException
	 * @throws Exception
	 */
	private void selectAndPublish(Connection conn, List<Map<String, Object>> rows)
			throws SQLException, IOException, PublishException {

		if (rows.isEmpty()) {
			if (logger.isDebugEnabled())
				logger.debug(logPrefix + "nothing to load");
			return;
		}
		
		if (logger.isDebugEnabled())
			logger.debug(logPrefix + "getting data");
		
		String query = handler.getTargetQuery();
		NamedParameterStatement ps = null;
		ResultSet rs = null;
		try {
			Data data = null;
			ps = new NamedParameterStatement(conn, query);
			for (Map<String, Object> keys : rows) {
				fillParameters(keys, query, ps);
				DBUtils.close(rs);

				rs = ps.executeQuery();
				if (data == null) {
					String[] colNames = getColumnNamesFromResultSet(rs);
					data = new Data(colNames);
				}
				while (rs.next()) {
					data.append(rs);
				}
			}
			handler.publish(data);

		} finally {
			DBUtils.close(rs);
			DBUtils.close(ps);
		}
	}

	/**
	 * Retrieve column names from a resultset
	 * 
	 * @param rs
	 *            query result set
	 * @return array of column names
	 * @throws SQLException
	 */
	private String[] getColumnNamesFromResultSet(ResultSet rs) throws SQLException {

		int cols = rs.getMetaData().getColumnCount();
		String[] colNames = new String[cols];
		for (int i = 0; i < cols; i++) {
			colNames[i] = rs.getMetaData().getColumnLabel(i + 1);
		}
		return colNames;
	}

	/**
	 * Fill named parameter in a query from a Map<ColumnName and Value>.
	 * 
	 * @param map
	 *            parameters map
	 * @param query
	 *            query statement
	 * @param ps
	 *            a named parameter statement
	 * @throws SQLException
	 */
	private void fillParameters(Map<String, Object> map, String query, NamedParameterStatement ps) throws SQLException {

		for (Map.Entry<String, Object> e : map.entrySet()) {
			if (query.contains(":" + e.getKey())) {
				ps.setObject(e.getKey(), e.getValue());
			}
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
			if (logger.isDebugEnabled())
				logger.debug(logPrefix + "nothing to clean");
			return;
		}
		
		if (logger.isDebugEnabled())
			logger.debug(logPrefix + "cleaning journal " + handler.getJournalTable());
		
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
			logger.info(logPrefix + rows.size() + " rows removed (" + handler.getJournalTable() + ")");

		} finally {
			DBUtils.close(ps);
		}
	}

	/**
	 * Memorizes the last imported window_id from journal table
	 * 
	 * @param rows
	 */
	private void markLastLoaded(CuratorFramework client, List<Map<String, Object>> rows) {

		if (rows == null || rows.isEmpty())
			return;

		Long lastWindowId = 0L;
		for (Map<String, Object> row : rows) {
			Number windowId = (Number) row.get("window_id");
			if (windowId.longValue() > lastWindowId.longValue()) {
				lastWindowId = windowId.longValue();
			}
		}

		String k = getPrefix() + "/lastWindowId";
		String s = lastWindowId.toString();
		try {
			if (client.checkExists().forPath(k) == null)
				client.create().withMode(CreateMode.PERSISTENT).forPath(k);

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

		if (logger.isDebugEnabled())
			logger.debug(agentName + " :: connecting to " + zookeeper);
		RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);
		CuratorFramework client = CuratorFrameworkFactory.newClient(zookeeper, retryPolicy);
		client.start();

		// Uses the distributed lock recipe of ZooKeeper to avoid concurrency
		Connection conn = null;
		String lockPath = getPrefix() + "/lock";
		InterProcessMutex lock = new InterProcessMutex(client, lockPath);
		if (logger.isTraceEnabled())
			logger.trace(logPrefix + "waiting lock from " + zookeeper + lockPath);
		boolean lockAcquired = false;
		try {
			if (lock.acquire(5, TimeUnit.SECONDS)) {
				lockAcquired = true;

				if (logger.isTraceEnabled())
					logger.trace(logPrefix + "get database connection");
				
				conn = getConnection();

				// Get journal data
				List<Map<String, Object>> rows = getJournalKeys(client, conn);

				// Retrieve changed data and publish it
				selectAndPublish(conn, rows);

				if (JournalStrategy.WINDOW.equals(handler.getStrategy())) {
					// Update last loaded window_id
					markLastLoaded(client, rows);

				} else {
					// Remove from journal imported & published data
					deleteFromJournal(conn, rows);
				}

				statistics.update(client, handler.getClass().getName(), rows.size());

			}

		} catch (Exception e) {
			logger.error(agentName + " :: failure", e);

		} finally {
			DBUtils.close(conn);
			if (lockAcquired) {
				if (logger.isDebugEnabled())
					logger.debug(agentName + " :: lock release");
				try {
					lock.release();
				} catch (Exception e) {
					logger.warn(logPrefix + "failed to release zk lock for ", e);
				}
			}
			client.close();
		}
	}
}
