# DB-Xtract

Welcome to the DB-Xtract!

It is an attempt to create a scalable, fault-tolerant and flexible change data capture (<i>CDC</i>) tool. A this very first moment, it is providing journal-based <i>CDC</i>, but I want to add other mechanisms for more performance and lower resources consumption.

DB-Xtract can be distributed as concurrent <i>CDC</i> agents, providing scalability, availability and performance. It relies on Apache's ZooKeeper to coordinate data captures in a safe and still fast way.

## Getting Started

For your first try, you need:
<ul>
<li>a JVM</li>
<li>ZooKeeper (instructions at https://zookeeper.apache.org/)</li>
<li>your preferred database (PostgreSQL, Oracle, MariaDB and so on). Out-of-the-box example is using MySQL, but you don't need to stick to this.</li>
</ul>

### Installing

Download the project with:
	
	git clone https://github.com/fsanti68/db-xtract
	
	mvn compile
	

You can create in your database two tables, that will act as source table and journal table:

	create table test (
		key1	int not null,
		key2 int not null,
		data int,
		primary key (key1, key2)
	); 
	
	create table j$test (
		window_id int auto_increment primary key,
		key1 int not null,
		key2 int not null
	);

Change the tests configuration to your own environment (<code>src/test/java/com/dsf/dbxtract/cdc/config-apptest.properties</code>):

	log4j.appender.A1=org.apache.log4j.ConsoleAppender
	log4j.appender.A1.layout=org.apache.log4j.PatternLayout
	log4j.appender.A1.layout.ConversionPattern=%-4r [%t] %-5p %c %x - %m%n
	
	zookeeper=localhost:2181
	thread.pool.size=3
	interval=5000
	
	sources=test
	source.test.connection=jdbc:mysql://localhost:3306/mytestdb
	source.test.driver=org.gjt.mm.mysql.Driver
	source.test.user=root
	source.test.password=mysql
	
	## same handler repeated many time to simulate some concurrency
	source.test.handlers=com.dsf.dbxtract.cdc.sample.TestHandler,com.dsf.dbxtract.cdc.sample.TestHandler

## Running the tests

All tests are provided as JUnit test cases. Before starting tests, be sure that mysql (or your preferred database) and ZooKeeper are ready, running and configured in app's property file (<code>src/test/java/com/dsf/dbxtract/cdc/config-apptest.properties</code>).

	mvn test
	
Or... you can start database and/or ZooKeeper before the application and enjoy it's fault-tolerant behaviour.


## Built with Maven

* [Maven](https://maven.apache.org/) - Dependency Management

## Contributing

Please read [CONTRIBUTING.md](CONTRIBUTING.md) for details on our code of conduct, and the process for submitting pull requests to us.

## Authors

* **Fabio De Santi** - *Initial work* - [fsanti68](https://github.com/fsanti68)

See also the list of [contributors](https://github.com/your/project/contributors) who participated in this project.

## License

This project is licensed under the Apache License, Version 2.0 - see the [LICENSE.md](LICENSE.md) file for details

## Acknowledgments

* A special thanks to Flavio ([xboard](https://github.com/xboard)) that always share his bright wisdom.
* Thanks to Adam Crume ([adamcrume](https://github.com/adamcrume)) that created a really useful NamedParameterStatement class.
