name := "bigclam"
 
version := "1.0"
 
scalaVersion := "2.11.8"

 
libraryDependencies ++= Seq(
  "org.slf4j" % "slf4j-log4j12" % "1.7.7",
  "org.apache.spark" % "spark-core_2.11" % "2.4.5" % "provided",
  "org.apache.spark" % "spark-sql_2.11" % "2.4.5" % "provided",
  "org.apache.spark" % "spark-streaming_2.11" % "2.4.5" % "provided",
  "org.apache.spark" % "spark-graphx_2.11" % "2.4.5" % "provided",
  "org.apache.spark" % "spark-mllib_2.11" % "2.4.5" % "provided",
  "com.hankcs" % "hanlp" % "portable-1.7.2",
  "org.scalaj" %% "scalaj-http" % "2.3.0",
  "com.google.code.gson" % "gson" % "2.3.1",
  "org.slf4j" % "slf4j-api" % "1.7.7" % "provided",
  "commons-codec" % "commons-codec" % "1.15",
  "javax.mail" % "mail" % "1.4.6",
  "com.metamx" % "java-util" % "0.27.9",
  "mysql" % "mysql-connector-java" % "5.1.38",
  "com.github.scopt" %% "scopt" % "3.3.0"
)
mergeStrategy in assembly := {
  case PathList(ps @ _*) if ps.last endsWith ".xml" => MergeStrategy.first
  case PathList(ps @ _*) if ps.last endsWith ".properties" => MergeStrategy.first
  case PathList(ps @ _*) if ps.last endsWith ".class" => MergeStrategy.first
  case PathList(ps @ _*) if ps.last endsWith ".html" => MergeStrategy.first
  case PathList(ps @ _*) if ps.last endsWith ".Named" => MergeStrategy.first
  case PathList(ps @ _*) if ps.last endsWith ".java" => MergeStrategy.first
  case PathList(ps @ _*) if ps.last endsWith ".types" => MergeStrategy.first
  case x =>
    val oldStrategy = (mergeStrategy in assembly).value
    oldStrategy(x)
}
 
 
javacOptions ++= Seq("-encoding", "UTF-8")
 
javacOptions ++= Seq("-source", "1.8","-target","1.8")
 
