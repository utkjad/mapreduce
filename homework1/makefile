#Customize these paths for your environment.
# -----------------------------------------------------------
jar.name=target/homework1-0.0.1-SNAPSHOT.jar

# Compiles code and builds jar.
jar:
	mvn install
# Removes local output directory.
clean:
	mvn clean

# Runs standalone
local:  clean jar
	java -Xmx6g -jar ${jar.name} ${job.name} ${input}

