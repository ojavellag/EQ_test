name := "processLogs"

version := "1.0"

organization := "com.ojag"

scalaVersion:= "2.11.11"
val sparkVersion = "2.3.0"
val vegasVersion = "0.3.11"

libraryDependencies ++= Seq(
	"org.apache.spark" %% "spark-core" %  sparkVersion,
	"org.apache.spark" %% "spark-sql"  %  sparkVersion,
	"org.vegas-viz"    %% "vegas"      %  vegasVersion,
	"org.vegas-viz"    %% "vegas-spark" % vegasVersion
)


