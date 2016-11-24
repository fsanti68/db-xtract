package com.dsf.dbxtract.cdc.journal;

/**
 * There are two ways for control what has been already captured:
 * <ul>
 * <li>DELETE = imported data is removed from journal table</li>
 * <li>WINDOW = DB-Xtract keeps record of last window id, without changing
 * journal data</li>
 * </ul>
 * 
 * @author fabio de santi
 *
 */
public enum JournalStrategy {
	DELETE(1), WINDOW(0);

	private int strategy;

	private JournalStrategy(int id) {
		this.strategy = id;
	}

	/**
	 * Retrieve a strategy based on its identifier
	 * 
	 * @param id
	 *            object's identifier (enum key).
	 * @return
	 */
	public JournalStrategy getById(int id) {
		for (JournalStrategy js : values()) {
			if (js.strategy == id)
				return js;
		}
		return null;
	}
}
