
package org.scalaproject001.config

import com.typesafe.config.{Config, ConfigFactory}

//the purpose of this code is to retrieve values from the application.conf file

case class AppSettings(
  inputDetections: String,
  inputLocations: String,
  output: String,
  topX: Int
)

object AppConfig {
  private val config: Config = ConfigFactory.load()

  val settings: AppSettings = AppSettings(
    inputDetections = config.getString("app.inputDetections"),
    inputLocations  = config.getString("app.inputLocations"),
    output          = config.getString("app.output"),
    topX            = config.getInt("app.topX")
  )
}
