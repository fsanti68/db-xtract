package com.dsf.dbxtract.cdc.mon;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlTransient;
import javax.xml.bind.annotation.XmlType;

/**
 * Session store for basic Handler's statistics.
 * 
 * @author fabio de santi
 * @version 0.1
 */
@XmlRootElement
@XmlType(propOrder = { "handlers" })
public class Statistics {

	private static ReentrantReadWriteLock rrwl = new ReentrantReadWriteLock();
	private static Map<String, StatEntry> map = new HashMap<String, StatEntry>();

	/**
	 * 
	 * @param handler
	 *            handler's name
	 * @param rows
	 *            number of rows captured
	 */
	public void notifyRead(String handler, int rows) {

		WriteLock lock = rrwl.writeLock();
		lock.lock();
		try {
			StatEntry entry = map.get(handler);
			if (entry == null)
				entry = new StatEntry(handler);
			entry.increment(rows);
			map.put(handler, entry);

		} finally {
			lock.unlock();
		}
	}

	/**
	 * Signal that a capture was attempt.
	 * 
	 * @param handler
	 *            handler's name
	 */
	public void touch(String handler) {

		WriteLock lock = rrwl.writeLock();
		lock.lock();
		try {
			StatEntry entry = map.get(handler);
			if (entry == null) {
				entry = new StatEntry(handler);
			}
			entry.touch();
			map.put(handler, entry);

		} finally {
			lock.unlock();
		}
	}

	/**
	 * 
	 * @param handler
	 *            handler's name
	 * @return {@link StatEntry} object with basic statistics for the given
	 *         handler
	 */
	public StatEntry get(String handler) {
		ReadLock lock = rrwl.readLock();
		lock.lock();
		try {
			return map.get(handler).clone();
		} finally {
			lock.unlock();
		}
	}

	public Collection<Statistics.StatEntry> getHandlers() {
		ReadLock lock = rrwl.readLock();
		lock.lock();
		try {
			Map<String, StatEntry> _m = new HashMap<String, StatEntry>();
			_m.putAll(map);
			return _m.values();
		} finally {
			lock.unlock();
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
			this.readCount += rows;
			this.lastRead = new Date();
		}

		protected void touch() {
			this.lastSeek = new Date();
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
