name := "Mammoth"

version := "0.1"

scalaVersion := "2.10.6"


// Glint

libraryDependencies += "ch.ethz.inf.da" %% "glint" % "0.1-SNAPSHOT"


// Glint LDA

libraryDependencies += "ch.ethz.inf.da" %% "glintlda" % "0.1-SNAPSHOT"


// Spire (generic fast numerics)

libraryDependencies += "org.spire-math" %% "spire" % "0.7.4"

// Spark

libraryDependencies += "org.apache.spark" % "spark-core_2.10" % "1.5.1" % "provided"

libraryDependencies += "org.apache.spark" % "spark-mllib_2.10" % "1.5.1" % "provided"


// Breeze native BLAS support

libraryDependencies += "org.scalanlp" %% "breeze" % "0.11.2"

libraryDependencies += "org.scalanlp" %% "breeze-natives" % "0.11.2"


// Retry

resolvers += "softprops-maven" at "http://dl.bintray.com/content/softprops/maven"

libraryDependencies += "me.lessis" %% "retry" % "0.2.0"


// WARC file parser

libraryDependencies += "org.jwat" % "jwat-warc" % "1.0.1"


// WARC utils

resolvers += "SURFSARA repository" at "http://beehub.nl/surfsara-repo/releases"

libraryDependencies += "SURFsara" % "warcutils" % "1.2"


// Apache Commons IO

libraryDependencies += "commons-io" % "commons-io" % "2.4"


// JSoup

libraryDependencies += "org.jsoup" % "jsoup" % "1.8.3"


// Boilerplate:

libraryDependencies += "de.l3s.boilerpipe" % "boilerpipe" % "1.2.0"

libraryDependencies += "net.sourceforge.nekohtml" % "nekohtml" % "1.9.21"

libraryDependencies += "xerces" % "xercesImpl" % "2.11.0"


// Scopt

libraryDependencies += "com.github.scopt" % "scopt_2.10" % "3.3.0"


// Unit tests

libraryDependencies += "org.scalatest" % "scalatest_2.10" % "2.2.4" % "test"


// Resolvers

resolvers += "Akka Repository" at "http://repo.akka.io/releases/"

resolvers += "spray repo" at "http://repo.spray.io"

resolvers += "boilerpipe-m2-repo" at "http://boilerpipe.googlecode.com/svn/repo/"

resolvers += Resolver.sonatypeRepo("public")

resolvers += Resolver.sonatypeRepo("snapshots")


// Helper for assembly
assemblyMergeStrategy in assembly := {
  case PathList("org", "cyberneko", "html", _ *) => MergeStrategy.first
  case x =>
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(x)
}
