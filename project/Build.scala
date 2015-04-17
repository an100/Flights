import sbt.Keys._
import sbt._

object ApplicationBuild extends Build {

  Resolver.sonatypeRepo("releases")

  import org.scalastyle.sbt.ScalastylePlugin.{Settings => scalastyleSettings}
  import scoverage.ScoverageSbtPlugin.{buildSettings => scoverageSettings}

  object Versions {
    val spark = "1.2.0"
  }

  val customScalacOptions = Seq(
    "-deprecation",
    "-encoding", "UTF-8",
    "-feature",
    "-language:existentials",
    "-language:higherKinds",
    "-language:implicitConversions",
    "-unchecked",
    "-Xfatal-warnings",
    "-Xfuture",
    "-Xlint",
    "-Yno-adapted-args",
    "-Ywarn-dead-code",
    "-Ywarn-numeric-widen")

  val customJavaInRuntimeOptions = Seq(
    "-Xmx512m"
  )

  val customJavaInTestOptions = Seq(
    "-Xmx512m"
  )

  val customResolvers = Seq(
    Classpaths.sbtPluginReleases,
    Classpaths.typesafeReleases,
    "Sonatype OSS Releases" at "http://oss.sonatype.org/content/repositories/releases/",
    "Scalaz Bintray Repo" at "http://dl.bintray.com/scalaz/releases"
  )

  val customLibraryDependencies = Seq(
    "com.github.nscala-time" %% "nscala-time" % "1.8.0",
    "org.apache.spark" %% "spark-core" % Versions.spark,
    "org.apache.spark" %% "spark-sql" % Versions.spark,
    "org.apache.hadoop" % "hadoop-client" % "2.4.0" % "compile,test" exclude("javax.servlet", "servlet-api"),
    "org.scalatest" %% "scalatest" % "2.2.4" % "test"
  )

  lazy val main = Project(
    id = "flights",
    base = file("."),
    settings = Seq(
      version := "1.0",
      scalaVersion := "2.10.4",
      javaOptions in Runtime ++= customJavaInRuntimeOptions,
      javaOptions in Test ++= customJavaInTestOptions,
      scalacOptions ++= customScalacOptions,
      resolvers ++= customResolvers,
      libraryDependencies ++= customLibraryDependencies,
      parallelExecution in Test := false,
      fork in Test := true
    ) ++ scalastyleSettings ++ scoverageSettings
  )

}