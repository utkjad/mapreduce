# Customize these paths for your environment.
# -----------------------------------------------------------
hadoop.root=hadoop
local.input=input
jar.name=target/com.hw1aws-0.0.1-SNAPSHOT.jar 
job.name=com.hw1aws.WordCount
local.output=output

# Compiles code and builds jar.
jar:
	mvn install
# Removes local output directory.
clean-local-output:
	mvn clean
	rm -rf ${local.output}

# Runs standalone
# Make sure Hadoop  is set up (in /etc/hadoop files) for standalone operation (not pseudo-cluster).
# https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-common/SingleCluster.html#Standalone_Operation
local:	clean-local-output jar
	${hadoop.root} jar ${jar.name} ${job.name} ${local.input} ${local.output}
