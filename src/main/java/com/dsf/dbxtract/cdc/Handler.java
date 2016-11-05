package com.dsf.dbxtract.cdc;

/**
 * Interface to be implemented by capture executors.
 * 
 * @author fabio de santi
 * @version 0.1
 */
public interface Handler {

	/**
	 * 
	 * @return journal table's name
	 */
	public String getJournalTable();

	/**
	 * <p>The query to retrieve changed data can references columns from journal table as named parameters:</p>
	 * <code>select * from mytable where pk = :pk</code> 
	 * 
	 * @return data retrieval query
	 */
	public String getTargetQuery();

	/**
	 * Publishes captured data (like writing a file or publishing to a kafka queue).
	 */
	public void publish(Data data) throws Exception;
}
