package com.dsf.dbxtract.cdc.mon;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlTransient;
import javax.xml.bind.annotation.XmlType;

import org.apache.curator.framework.CuratorFramework;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.codehaus.jackson.map.ObjectMapper;

import com.dsf.dbxtract.cdc.App;

/**
 * Session store for basic Handler's statistics.
 * 
 * @author fabio de santi
 * @version 0.2
 */
public class Statistics {

	private static final Logger logger = LogManager.getLogger(Statistics.class.getName());
	protected static final String ZOOPATH = App.BASEPREFIX + "/statistics";

	private ObjectMapper mapper;

	public Statistics() {
		this.mapper = new ObjectMapper();
	}

	/**
	 * Updates statistics for a given handler.
	 * 
	 * @param client
	 *            zookeeper's connection
	 * @param handler
	 *            handler's name
	 * @param rows
	 *            number of rows captured. If zero, only lastSeek is updated,
	 *            otherwise updates also lastRead.
	 */
	public void update(CuratorFramework client, String handler, int rows) {

		String path = ZOOPATH + "/" + handler;
		try {
			StatEntry entry = get(client, handler);
			entry.increment(rows);
			byte[] b = mapper.writeValueAsBytes(entry);
			if (client.checkExists().forPath(path) == null)
				client.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT).forPath(path);
			client.setData().forPath(path, b);

		} catch (Exception e) {
			logger.error("Failed to save " + path, e);
			return;
		}
	}

	/**
	 * 
	 * @param handler
	 *            handler's name
	 * @return {@link StatEntry} object with basic statistics for the given
	 *         handler
	 */
	public StatEntry get(CuratorFramework client, String handler) {
		String path = ZOOPATH + "/" + handler;
		try {
			if (client.checkExists().forPath(path) == null)
				client.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT).forPath(path);

			byte[] d = client.getData().forPath(path);
			StatEntry entry = mapper.readValue(d, StatEntry.class);
			if (entry == null)
				entry = new StatEntry(handler);
			return entry;

		} catch (Exception e) {
			logger.error("Failed to obtain " + path, e);
			return new StatEntry(handler);
		}
	}

	/**
	 * Basic handler's statistics.
	 */
	@XmlRootElement
	@XmlAccessorType(XmlAccessType.PUBLIC_MEMBER)
	@XmlType(propOrder = { "name", "lastSeek", "lastRead", "readCount" })
	public static class StatEntry {

		@XmlTransient
		private static final DateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ");

		@XmlElement
		private String name;
		@XmlElement
		private Date lastSeek;
		@XmlElement
		private Date lastRead;
		@XmlElement
		private long readCount = 0L;

		public StatEntry() {
		}

		public StatEntry(String handler) {
			this.name = handler;
		}

		public String getName() {
			return name;
		}

		public String getLastRead() {
			return lastRead == null ? null : sdf.format(lastRead);
		}

		public String getLastSeek() {
			return lastSeek == null ? null : sdf.format(lastSeek);
		}

		public long getReadCount() {
			return readCount;
		}

		protected void increment(int rows) {
			this.lastSeek = new Date();
			this.readCount += rows;
			if (rows > 0)
				this.lastRead = new Date();
		}

		public StatEntry clone() {
			StatEntry e = new StatEntry(name);
			e.lastRead = this.lastRead;
			e.lastSeek = this.lastSeek;
			e.readCount = this.readCount;
			return e;
		}
	}
}
