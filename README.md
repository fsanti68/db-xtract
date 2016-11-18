# DB-Xtract

Welcome to the DB-Xtract!

It is an attempt to create a scalable, fault-tolerant and flexible change data capture (<i>CDC</i>) tool. At this very first moment, it is providing journal-based <i>CDC</i>, but I want to add other mechanisms for more performance and lower resources consumption.

DB-Xtract can be distributed as concurrent <i>CDC</i> agents, providing scalability, availability and fault-tolerance. It relies on Apache's ZooKeeper to coordinate concurrent data captures in a safe and still performatic way.

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
	
**Note**: The <code>master</code> branch may be in an unstable or even broken state during development. Please use [releases](https://github.com/fsanti68/db-xtract/releases) instead of the <code>master</code> branch in order to get stable binaries.

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

Create your own configuration file:

	log4j.appender.A1=org.apache.log4j.ConsoleAppender
	log4j.appender.A1.layout=org.apache.log4j.PatternLayout
	log4j.appender.A1.layout.ConversionPattern=%-4r [%t] %-5p %c %x - %m%n
	
	zookeeper=localhost:2181
	thread.pool.size=3

Configure remaining parameters (that are shared among all DB-Xtract nodes):

1) create a new datasource (essencially a database connection):

	$ java -jar dbxtract.jar --config myconfig.properties --source-add test jdbc:mysql://localhost:3306/mytestdb org.gjt.mm.mysql.Driver root mysql
	
2) assign a new CDC handler for this datasource:

	$ java -jar dbxtract.jar --config myconfig.properties --handler-add test  com.dsf.dbxtract.cdc.sample.TestHandler
	
3) check your new configuration:

	$ java -jar dbxtract.jar --config myconfig.properties --list
	
4) start the agent:

	$ java -jar dbxtract.jar --config myconfig.properties --start


## Running the tests

All tests are provided as JUnit test cases. Before starting tests, be sure that mysql (or your preferred database) and ZooKeeper are ready, running and configured in app's property file (<code>src/test/java/com/dsf/dbxtract/cdc/config-app-journal.properties</code>).

	mvn test
	
Or... you can start the application before database and/or ZooKeeper and enjoy it's fault-tolerant behaviour.


## Built with Maven

* [Maven](https://maven.apache.org/) - Dependency Management

## Contributing

Please read [CONTRIBUTING.md](CONTRIBUTING.md) for details on our code of conduct, and the process for submitting pull requests to us.

## Authors

* **Fabio De Santi** - *Initial work* - [fsanti68](https://github.com/fsanti68)

See also the list of [contributors](https://github.com/fsanti68/db-xtract/contributors) who participated in this project.

## License

This project is licensed under the Apache License, Version 2.0 - see the [LICENSE.md](LICENSE.md) file for details

## Acknowledgments

* A special thanks to Flavio ([xboard](https://github.com/xboard)) that always share his bright wisdom.
* Thanks to Adam Crume ([adamcrume](https://github.com/adamcrume)) that created a really useful *NamedParameterStatement* class.
