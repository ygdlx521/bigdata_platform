#!/bin/bash
$JAVA_HOME/bin/javac -cp $(hadoop classpath) -d . CustomMultiOutputFormat.java
$JAVA_HOME/bin/jar cvf multiOutput.jar com/custom/CustomMultiOutputFormat.class


