# DB-Xtract

Welcome to the DB-Xtract!

It is an attempt to create a scalable, fault-tolerant and flexible change data capture (_CDC_) tool. At this very first moment, it is providing journal-based _CDC_, but I want to add other mechanisms for more performance and lower resources consumption.

DB-Xtract can be distributed as concurrent _CDC_ agents, taking advantage of Apache's ZooKeeper coordination capabilities.

## Getting Started

For your first try, you need:
<ul>
<li>a JVM</li>
<li>[Apache ZooKeeper](https://zookeeper.apache.org/)</li>
<li>your preferred database (PostgreSQL, Oracle, MariaDB and so on). Out-of-the-box example is using MySQL, but you don't need to stick to this.</li>
</ul>

### Installing

Create your local copy of project:
	
	git clone https://github.com/fsanti68/db-xtract
	
	mvn compile

**Note**: The <code>master</code> branch may be in an unstable or even broken state during development. Please use [releases](https://github.com/fsanti68/db-xtract/releases) instead of the <code>master</code> branch in order to get stable binaries.

You can create a new database with two tables -- a source table and journal table:

	create database dbxtest;
	
	use dbxtest;
	
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

Create your own configuration file <code>myconfig.properties</code>:

	log4j.appender.A1=org.apache.log4j.ConsoleAppender
	log4j.appender.A1.layout=org.apache.log4j.PatternLayout
	log4j.appender.A1.layout.ConversionPattern=%-4r [%t] %-5p %c %x - %m%n
	
	zookeeper=localhost:2181
	thread.pool.size=3
	
	interval=5000
	
	sources=test
	
	source.test.connection=jdbc:mysql://localhost:3306/dbxtest?useSSL=false
	source.test.driver=org.gjt.mm.mysql.Driver
	source.test.user=root
	source.test.password=mysql
	source.test.handlers=com.dsf.dbxtract.cdc.sample.TestHandler

Check your configuration...

	$ java -jar dbxtract.jar --config myconfig.properties --list
	
... and start the agent:

	$ java -jar dbxtract.jar --config myconfig.properties --start


## Running the tests

All tests are provided as TestNG test cases. Before starting tests, be sure that mysql (or your preferred database) and ZooKeeper are ready, running and configured in app's property files (<code>src/test/java/com/dsf/dbxtract/cdc/config-app-journal-*.properties</code>).

	mvn test
	
Or... you can start the application before database and/or ZooKeeper and enjoy it's fault-tolerant behaviour.


## Built with Maven

* [Maven](https://maven.apache.org/) - Dependency Management

## Monitoring with JMX

The application can be monitored using any JMX-compliant tool. A simple example is jconsole, that is part of regular JDK distribution. To start the application with JMX support, just call:

	$ export JMXPARAMS="-Dcom.sun.management.jmxremote -Dcom.sun.management.jmxremote.authenticate=false -Dcom.sun.management.jmxremote.ssl=false -Dcom.sun.management.jmxremote.port=5000"
	$ java $JMXPARAMS -jar dbxtract.jar --config myconfig.properties --start
	
	$ jconsole
	
Once opened the jconsole window, you can select the corresponding process id (PID) or select a remote connection with <code>service:jmx:rmi:///jndi/rmi://localhost:5000/jmxrmi</code>.

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
