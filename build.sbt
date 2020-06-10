name := "DataEngineerChallenge"

version := "0.1"

scalaVersion := "2.11.12"

val sparkVersion = "2.4.5"

libraryDependencies ++= Seq(
  "com.github.pureconfig" %% "pureconfig" % "0.12.3",
  "com.github.scopt" %% "scopt" % "4.0.0-RC2",
  "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-streaming" % sparkVersion % "provided",
  "org.scalatest" %% "scalatest" % "3.0.1" % Test,
  "com.holdenkarau" %% "spark-testing-base" % "2.4.5_0.14.0" % Test
)

fork in Test := true
parallelExecution in Test := false
javaOptions ++= Seq("-Xms512M", "-Xmx2048M", "-XX:MaxPermSize=2048M", "-XX:+CMSClassUnloadingEnabled")

assemblyShadeRules in assembly := Seq(ShadeRule.rename("shapeless.**" -> "new_shapeless.@1").inAll)