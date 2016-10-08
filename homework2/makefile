# Customize these paths for your environment.
# -----------------------------------------------------------
hadoop.root=hadoop
jar.name=target/homework2-0.0.1-SNAPSHOT.jar
job.incombiner.type=homework2.InMapperCombiner
job.nocombiner.type=homework2.Homework2
job.combiner.type=homework2.Combiner
job.secondarysort=homework2.SecondarySort

# Compiles code and builds jar.
jar:
	mvn install

# Removes local output directory.
clean-local-output:
	mvn clean
	rm -rf ${output}

# for no combiner
nocombiner:
	${hadoop.root} jar ${jar.name} ${job.nocombiner.type} ${input} ${output}

# for combiner
combiner:
	${hadoop.root} jar ${jar.name} ${job.combiner.type} ${input} ${output}

# for in combiner
inmappercombiner:
	${hadoop.root} jar ${jar.name} ${job.incombiner.type} ${input} ${output}

secondarysort:
	${hadoop.root} jar ${jar.name} ${job.secondarysort} ${input} ${output}

local:	clean-local-output jar
