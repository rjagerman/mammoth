name := "Mammoth"

version := "0.1"

scalaVersion := "2.10.5"


// Spark, Hadoop, Mahout

libraryDependencies += "org.apache.spark" % "spark-core_2.10" % "1.3.0"

libraryDependencies += "org.apache.spark" % "spark-mllib_2.10" % "1.3.0"


// WARC file parser

libraryDependencies += "org.jwat" % "jwat-warc" % "1.0.1"


// WARC utils

resolvers += "SURFSARA repository" at "http://beehub.nl/surfsara-repo/releases"

libraryDependencies += "SURFsara" % "warcutils" % "1.2"


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

libraryDependencies += "org.scalatest" % "scalatest_2.10" % "2.2.4" % "test"

