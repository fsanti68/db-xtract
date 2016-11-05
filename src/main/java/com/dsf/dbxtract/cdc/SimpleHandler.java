package com.dsf.dbxtract.cdc;

/**
 * A simple handler
 * 
 * @author fabio de santi
 * @version 0.1
 */
public class SimpleHandler implements Handler {

	private String journal;
	private String query;
	private Publisher publisher;

	public SimpleHandler(String journalTable, String query, Publisher publisher) {
		this.journal = journalTable;
		this.query = query;
		this.publisher = publisher;
	}

	public String getJournalTable() {
		return journal;
	}

	public int getBatchSize() {
		return 200;
	}

	public String getTargetQuery() {
		return query;
	}

	public void publish(Data data) throws Exception {
		publisher.publish(data);
	}
}
