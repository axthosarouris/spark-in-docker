# Spark in docker.
**Run spark in docker for testing purposes and for easier configuration.**

Running Spark inside a Docker container allows easy configuration of a Spark application.

##User guide

1. To run the image you need to install Docker and docker-compose

2. Create a folder (e.g. `spark-conf`) where you will set your configuration files.

3. Add in the `spark-conf` folder a log4j.properties or log4j.xml file.

3. Add a (possibly empty) file named `sparksubmbit.conf` that contains all the `--conf`
  sent to the spark-submit command. You do not need to add an option fot the log4j file. 
  It will be inlcluded automatically.
  
4. (Optional) Add a file `spark-driver-java-options.conf` for sending configuration options
   to the spark-driver. You do not need to add a configuration option for the log4j file. 
 
5. In the docker-compose file set a volume so that  there is a folder `/libs` in 
   the docker container 
   containing the jars that are necessary for the Spark application to run, 
   
6. In the docker-compose file set a volume so that  there is a folder `/conf` in 
   the docker container 
   containing the configuration files mentioned above,
