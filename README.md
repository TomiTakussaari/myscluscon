myscluscon 
==========
[![Build Status](https://travis-ci.org/TomiTakussaari/myscluscon.svg)](https://travis-ci.org/TomiTakussaari/myscluscon)[![codecov.io](http://codecov.io/github/TomiTakussaari/myscluscon/coverage.svg?branch=master)](http://codecov.io/github/TomiTakussaari/myscluscon?branch=master)

JDBC Driver for always connecting to valid server in your (Mysql) Galera or read-only slave cluster

##  What
- Define servers in your cluster in jdbc url
- Always connect to valid server in your cluster.
    - Either Galera node that returns WSREP_READY=ON
    - Or read-only slave in normal mysql master-slave replication that is valid (replication is running)
- Connections to servers that do not pass validity check, return "false" for isValid(timeout) call.
    - Your favorite connection pool will notice this, and can then replace faulty connections with proper one, pointing to working server. 
- Standardish JDBC API.
- Java8

### Supported schemas
- jdbc:myscluscon:mysql:read_cluster - for connecting to read only cluster
- jdbc:myscluscon:galera:cluster - for connecting to Galera cluster
      
## Why
- There did not seem to be existing solution for this kind of functionality
- You can probably do the same using MysqlProxy or HAproxy (and maybe others), but that means you need more servers and (probably) results in more complex overall architecture. 
- It might make sense to do this on driver level if all/most of the applications using your cluster use JDBC.
- I've never written a JDBC driver, so wanted to try it out.. :)      


## Usage example with standard JDBC

    
    //Connection to mysql read cluster consisting of serverOne, serverTwo or serverThree, which ever is valid or some if all are valid
    Connection connection = DriverManager.getConnection("jdbc:myscluscon:mysql:read_cluster://serverOne,serverTwo,serverThree":2134/database", "username", "password");

    //Connection to galera cluster consisting of serverOne, serverTwo or serverThree, which ever is valid or some if all are valid
    Connection connection = DriverManager.getConnection("jdbc:myscluscon:galera:cluster://serverOne,serverTwo,serverThree", "username", "password");

All queryparameters are passed on untouched


## Usage example with [HikariCP](https://github.com/brettwooldridge/HikariCP) connection pool 
            
    Class.forName("com.mysql.jdbc.Driver"); //Load your favorite Mysql driver that understands JDBC urls which start with jdbc:mysql
             
    HikariConfig hikariConfig = new HikariConfig();
    hikariConfig.setJdbcUrl(jdbc:myscluscon:mysql:read_cluster://serverOne,serverTwo,ServerThree:2134/database);
    hikariConfig.setUsername("username");
    hikariConfig.setPassword("password");
    
    DataSource dataSource = new HikariDataSource(hikarConfig);
    
    //Connection to serverOne, serverTwo or serverThree, which ever is valid or some if all are valid 
    dataSource.getConnectioon(); 
            
