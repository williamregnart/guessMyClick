name := "guessMyClick"

version := "0.1"

scalaVersion := "2.12.10"

libraryDependencies := Seq(
	// https://mvnrepository.com/artifact/org.apache.spark/spark-core
	"org.apache.spark" %% "spark-core" % "2.4.4",
	// https://mvnrepository.com/artifact/org.apache.spark/spark-sql
	"org.apache.spark" %% "spark-sql" % "2.4.4",
	// https://mvnrepository.com/artifact/org.apache.spark/spark-mllib
	"org.apache.spark" %% "spark-mllib" % "2.4.4"
)

//assemblyMergeStrategy in assembly := {
//	case PathList("org","aopalliance", xs @ _*) => MergeStrategy.last
//	case PathList("javax", "inject", xs @ _*) => MergeStrategy.last
//	case PathList("javax", "servlet", xs @ _*) => MergeStrategy.last
//	case PathList("javax", "activation", xs @ _*) => MergeStrategy.last
//	case PathList("org", "apache", xs @ _*) => MergeStrategy.last
//	case PathList("com", "google", xs @ _*) => MergeStrategy.last
//	case PathList("com", "esotericsoftware", xs @ _*) => MergeStrategy.last
//	case PathList("com", "codahale", xs @ _*) => MergeStrategy.last
//	case PathList("com", "yammer", xs @ _*) => MergeStrategy.last
//	case "about.html" => MergeStrategy.rename
//	case "META-INF/ECLIPSEF.RSA" => MergeStrategy.last
//	case "META-INF/mailcap" => MergeStrategy.last
//	case "META-INF/mimetypes.default" => MergeStrategy.last
//	case "plugin.properties" => MergeStrategy.last
//	case "log4j.properties" => MergeStrategy.last
//	case "overview.html" => MergeStrategy.last  // Added this for 2.1.0 I think
//	case x =>
//		val oldStrategy = (assemblyMergeStrategy in assembly).value
//		oldStrategy(x)
//}

/* including scala bloats your assembly jar unnecessarily, and may interfere with
   spark runtime */
assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)
assemblyJarName in assembly := "guessmyclick.jar"