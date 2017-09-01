Spark
-----
>The purpose of the project is to test Spark features.

Environment
-----------
>The following environment variables should be in your .bash_profile.

- export JAVA_HOME="/Library/Java/JavaVirtualMachines/jdk1.8.0_144.jdk/Contents/Home"
- export SPARK_HOME="/Users/myself/workspace/apache/spark"
- export SPARK_LAUNCHER="$SPARK_HOME/launcher/target"
- export PATH=${JAVA_HOME}/bin:${SPARK_HOME}/bin:${SPARK_HOME}/sbin:/usr/local/bin:/usr/local/sbin:$PATH

Configuration
-------------
1. log4j.properties.template -> rename to log4j.properites, located in SPARK_HOME/libexec

Test
----
1. sbt test

Run
---
1. sbt run

Output
------
1. ./target/output/test
2. ./target/output/main