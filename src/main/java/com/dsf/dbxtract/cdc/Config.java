package com.dsf.dbxtract.cdc;

import java.io.File;
import java.io.FileInputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * 
 * @author fabio de santi
 * @version 0.1
 */
public class Config {

	private Properties props;
	private List<Source> sources = null;

	public Config(String source) throws Exception {

		Properties props = new Properties();
		props.load(new FileInputStream(new File(source)));
		this.props = props;
	}

	/**
	 * A lista das fontes de dados é mantida no arquivo de configuração, sob o
	 * item "sources". Várias fontes de dados podem ser cadastradas, separadas
	 * por vírgula.
	 * 
	 * @return lista das fontes de dados
	 */
	public List<Source> getDataSources() {

		if (sources == null) {
			sources = new ArrayList<Source>();
			String[] a = props.getProperty("sources").split(",");
			for (String s : a) {
				String cat = s.trim();
				sources.add(new Source(cat, props.getProperty("source." + cat + ".connection"),
						props.getProperty("source." + cat + ".driver"), props.getProperty("source." + cat + ".user"),
						props.getProperty("source." + cat + ".password"),
						props.getProperty("source." + cat + ".handlers")));
			}
		}
		return sources;
	}

	/**
	 * 
	 * @return endereco de conexao do ZooKeeper (por ex. "localhost:2181")
	 */
	public String getZooKeeper() {
		return props.getProperty("zookeeper");
	}
	
	/**
	 * 
	 * @return intervalo, em milisegundos, entre os ciclos de captura
	 */
	public String getInterval() {
		return props.getProperty("interval");
	}
}
