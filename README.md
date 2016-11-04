myscluscon 
==========
[![Build Status](https://travis-ci.org/TomiTakussaari/myscluscon.svg)](https://travis-ci.org/TomiTakussaari/myscluscon)[![Coverage Status](https://coveralls.io/repos/github/TomiTakussaari/myscluscon/badge.svg?branch=master)](https://coveralls.io/github/TomiTakussaari/myscluscon?branch=master)

JDBC Driver for always connecting to valid server in your Galera or MariaDb/Mysql read-only slave cluster

##  What
- Define servers in your cluster in jdbc url
- Classifies each server with status (DEAD, STOPPED, BEHIND, or OK) and then chooses best one (least useful status can be configured)
- Always connect to valid server in your cluster.
    - Either Galera node that returns WSREP_READY=ON
    - Or read-only slave in normal mysql master-slave replication that is valid (replication is running)
- Connections to servers that do not pass validity check, return "false" for isValid(timeout) call.
    - Do note that Connections created via myscluscon do not switch to another server when server is no longer valid, you need to do it manually
    - However, most connection pools notice this, and can then replace faulty connections with proper one automatically before giving connections to your code
- Standardish JDBC API
- Java8, no other runtime dependencies (but requires jdbc driver supporting jdbc:mysql scheme)

### Supported schemas
- jdbc:myscluscon:mysql:read_cluster - for connecting to read only cluster
- jdbc:myscluscon:galera:cluster - for connecting to Galera cluster
      
## Why
- There did not seem to be existing solution for this kind of functionality
- You can probably do the same using MysqlProxy or HAproxy (and maybe others), or via application level "DbManager" but that means you need more servers and code and (most likely) results in more complex overall architecture.
- It might make sense to do this on driver level if all/most of the applications using your cluster use JDBC.
- I've never written a JDBC driver, so wanted to try it out.. :)      


# Maven 

    <dependency>
        <groupId>com.github.tomitakussaari</groupId>
        <artifactId>myscluscon-driver</artifactId>
        <version>3.0.0</version>
    </dependency>

## Usage example with standard JDBC

    
    //Connection to any valid server in mysql read cluster consisting of serverOne, serverTwo or serverThree.
    Connection connection = DriverManager.getConnection("jdbc:myscluscon:mysql:read_cluster://serverOne,serverTwo,serverThree:2134/database", "username", "password");

    //Connection to any valid server in galera cluster consisting of serverOne, serverTwo or serverThree
    Connection connection = DriverManager.getConnection("jdbc:myscluscon:galera:cluster://serverOne,serverTwo,serverThree", "username", "password");

All queryparameters are passed untouched to jdbc:mysql driver

## Database privileges

In addition to privileges needed by your business logic, myscluscon reed-cluster mode needs ```REPLICATION CLIENT``` privilege, as it needs to run ```SHOW SLAVE STATUS``` query to check slave status.

In Galera mode, myscluscon needs to just issue ```SHOW STATUS``` query, which does require any extra privileges.

### Configuration

Following queryparameters can be used to configure myscluscon:

    - maxSlaveLag=<max amount of seconds slave can be behind master to be considered OK>
       - If over this value, slave will be considered BEHIND. 
       - Not supported for Galera
       - Default 2
       - example: "jdbc:myscluscon:mysql:read_cluster://serverOne,serverTwo,serverThree:2134/database?maxSlaveLag=3
    
    - connectionStatus=<status of connection that is considered usable, one of DEAD, STOPPED, BEHIND, or OK>
       - Default STOPPED
       - BEHIND not supported for Galera
       - example: "jdbc:myscluscon:mysql:read_cluster://serverOne,serverTwo,serverThree:2134/database?connectionStatus=BEHIND


## Usage example with [HikariCP](https://github.com/brettwooldridge/HikariCP) connection pool 
            
    Class.forName("com.mysql.jdbc.Driver"); //Load your favorite Mysql driver that understands JDBC urls which start with jdbc:mysql
             
    HikariConfig hikariConfig = new HikariConfig();
    hikariConfig.setJdbcUrl("jdbc:myscluscon:mysql:read_cluster://serverOne,serverTwo,ServerThree:2134/database");
    hikariConfig.setUsername("username");
    hikariConfig.setPassword("password");
    
    DataSource dataSource = new HikariDataSource(hikarConfig);
    
    //Connection to serverOne, serverTwo or serverThree, which ever is valid or some if all are valid 
    dataSource.getConnectioon(); 
            
## Developing
Uses [lombok](https://projectlombok.org/index.html) to avoid some boilerplate code, so you probably want to use IDE plugin to support that.