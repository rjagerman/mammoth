name := "Mammoth"

version := "0.1"

scalaVersion := "2.11.5"


// Spark, Hadoop, Mahout

libraryDependencies += "org.apache.spark" % "spark-core_2.11" % "1.3.0"

//libraryDependencies += "org.eclipse.jetty.orbit" % "javax.servlet" % "3.0.0.v201112011016" % "container,test,compile" artifacts Artifact("javax.servlet", "jar", "jar")

// WARC file parser

libraryDependencies += "org.jwat" % "jwat-warc" % "1.0.1"

// Apache Commons IO

libraryDependencies += "commons-io" % "commons-io" % "2.4"


// Boilerplate:

libraryDependencies += "de.l3s.boilerpipe" % "boilerpipe" % "1.2.0"

libraryDependencies += "net.sourceforge.nekohtml" % "nekohtml" % "1.9.21"

libraryDependencies += "xerces" % "xercesImpl" % "2.11.0"


// Resolvers

resolvers += "Akka Repository" at "http://repo.akka.io/releases/"

resolvers += "boilerpipe-m2-repo" at "http://boilerpipe.googlecode.com/svn/repo/"


// Unit tests

libraryDependencies += "org.scalatest" % "scalatest_2.11" % "2.2.4" % "test"

