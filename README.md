  # Apache Spark Scala Job Template
  
  What is it?
  -----------
  
  spark-scala-template is used to speedup development process
  by introducing a template to write and run a Spark job with commonly used 
  methods and flows (common enough for me to not ever write it from scratch again).
  I also uses it to teach fresh meat from university about Spark.
  
  Things you will need to run this
  --------------------------------
  
  Before running the project you will need to make sure you have these 
  things installed (which I used to write this template):

  * Scala 2.11.8 (http://www.scala-lang.org/)
  * SBT 0.13.12 (http://www.scala-sbt.org/)
  * Apache Spark 2.0.1 (https://spark.apache.org/)
  * Scala IDE (http://scala-ide.org/)
  
  Configuration Files
  -------------------

  You will find the following configuration files here:
  
  * /project/build.properties
  * /project/Dependencies.scala
  * /project/plugins.sbt
  * /build.sbt
  
  Running on Scala-IDE for Eclipse
  --------------------------------
  
  * in the command line navigate to project directory and run "sbt clean eclipse"
  * import project into Scala IDE
  * run DataPreparationTemplate.scala

  Licensing
  ---------

  Please see the file called LICENSE.