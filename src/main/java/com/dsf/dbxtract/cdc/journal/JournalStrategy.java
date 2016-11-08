package com.dsf.dbxtract.cdc.journal;

/**
 * 
 * @author fabio de santi
 *
 */
public enum JournalStrategy {
	DELETE(0), WINDOW(1);

	private int strategy;

	private JournalStrategy(int id) {
		this.strategy = id;
	}

	public JournalStrategy getById(int id) {
		for (JournalStrategy js : values()) {
			if (js.strategy == id)
				return js;
		}
		return null;
	}
}
