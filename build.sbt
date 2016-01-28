organization := "com.github.nilsga"

name := "akka-persistence-elasticsearch"

version := "1.0.3"

scalaVersion := "2.11.7"

val akkaVersion = "2.4.1"

licenses += ("MIT", url("http://opensource.org/licenses/MIT"))

parallelExecution in Test := false

credentials += Credentials(
  "Artifactory Realm",
  "oss.jfrog.org",
  sys.env.getOrElse("OSS_JFROG_USER", ""),
  sys.env.getOrElse("OSS_JFROG_PASS", "")
)

publishTo := {
  val jfrog = "https://oss.jfrog.org/artifactory/"
  if (isSnapshot.value)
    Some("OJO Snapshots" at jfrog + "oss-snapshot-local")
  else
    Some("OJO Releases" at jfrog + "oss-release-local")
}

libraryDependencies ++= Seq(
  "org.elasticsearch"  % "elasticsearch"             % "1.7.1",
  "com.sksamuel.elastic4s" %% "elastic4s-streams" % "1.7.4",
  "com.typesafe.akka"      %% "akka-persistence"                  % akkaVersion,
  "com.typesafe.akka"      %% "akka-slf4j"                        % akkaVersion % "test",
  "com.typesafe.akka"      %% "akka-persistence-tck"              % akkaVersion  % "test",
  "ch.qos.logback"         % "logback-classic" % "1.1.3" % "test"
)
