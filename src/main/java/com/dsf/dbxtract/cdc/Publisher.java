package com.dsf.dbxtract.cdc;

/**
 * 
 * @author fabio de santi
 *
 */
public interface Publisher {

	/**
	 * 
	 * @param data
	 */
	public void publish(Data data);
}
