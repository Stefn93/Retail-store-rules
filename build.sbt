name := "Retail store rules"

version := "0.1"

scalaVersion := "2.11.8"

libraryDependencies += "org.apache.spark" % "spark-core_2.11" % "2.1.0"
libraryDependencies += "org.apache.spark" % "spark-sql_2.11" % "2.1.0"
libraryDependencies += "org.apache.spark" % "spark-mllib_2.11" % "2.1.0"

artifactName := { (sv: ScalaVersion, module: ModuleID, artifact: Artifact) => "Retail-store-rules.jar" }