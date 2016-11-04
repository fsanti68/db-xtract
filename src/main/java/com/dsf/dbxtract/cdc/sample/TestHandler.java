package com.dsf.dbxtract.cdc.sample;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import com.dsf.dbxtract.cdc.Handler;

/**
 * Exemplo de classe que controla uma tabela para carga de dados.
 * 
 * @author fabio
 *
 */
public class TestHandler implements Handler {

	private static Logger logger = LogManager.getLogger(TestHandler.class.getName());

	public String getJournalTable() {
		return "J$TEST";
	}

	/**
	 * Os parâmetros devem ser referenciados da mesma forma que nas NamedQueries
	 * do JPA.
	 */
	public String getTargetQuery() {
		return "SELECT * FROM TEST WHERE KEY1 = :key1 AND KEY2 = :key2";
	}

	public void publish(String json) throws Exception {
		logger.info("Data to Publish:\n" + json);
		logger.debug(json);
		// TODO: enviar os dados para alguem...
	}
}
