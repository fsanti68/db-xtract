package com.dsf.dbxtract.cdc.mon;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;

public class Statistics {

	private static ReentrantReadWriteLock rrwl = new ReentrantReadWriteLock();
	private static Map<String, StatEntry> map = new HashMap<String, StatEntry>();

	public void notifyRead(String handler, int rows) {

		WriteLock lock = rrwl.writeLock();
		lock.lock();
		try {
			StatEntry entry = map.get(handler);
			if (entry == null) {
				entry = new StatEntry(handler);
			}
			entry.increment(rows);
			map.put(handler, entry);

		} finally {
			lock.unlock();
		}
	}
	
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

	public Set<String> keySet() {
		ReadLock lock = rrwl.readLock();
		lock.lock();
		try {
			return map.keySet();
		} finally {
			lock.unlock();
		}
	}

	public StatEntry get(String key) {
		ReadLock lock = rrwl.readLock();
		lock.lock();
		try {
			return map.get(key);
		} finally {
			lock.unlock();
		}
	}

	public static class StatEntry {

		private String handler;
		private Date lastSeek;
		private Date lastRead;
		private long readCount = 0L;
		
		public StatEntry(String handler) {
			this.handler = handler;
		}

		public String getHandler() {
			return handler;
		}

		public Date getLastRead() {
			return lastRead;
		}
		
		public Date getLastSeek() {
			return lastSeek;
		}

		public long getReadCount() {
			return readCount;
		}

		public void increment(int rows) {
			this.readCount += rows;
			this.lastRead = new Date();
		}
		
		public void touch() {
			this.lastSeek = new Date();
		}
	}
}
