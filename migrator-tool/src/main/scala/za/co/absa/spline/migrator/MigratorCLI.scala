/*
 * Copyright 2017 ABSA Group Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package za.co.absa.spline.migrator

import java.io.File

import ch.qos.logback.classic.Level
import org.backuity.ansi.AnsiFormatter.FormattedHelper
import za.co.absa.spline.common.SplineBuildInfo

import scala.concurrent.duration._

object MigratorCLI extends App {

  new MigratorCLI(MigratorTool).exec(args)
}

class MigratorCLI(migratorTool: MigratorTool) {
  def exec(args: Array[String]): Unit = {
    val cliParser = new scopt.OptionParser[MigratorConfig]("migrator-tool") {
      head("Spline Migration Tool", SplineBuildInfo.Version)

      (opt[String]('s', "source")
        valueName "<url>"
        text "MongoDB connection URL containing Spline (ver < 0.4) data to be migrated"
        required()
        action ((url, conf) => conf.copy(mongoConnectionUrl = url)))

      (opt[String]('t', "target")
        valueName "<url>"
        text "Spline Producer REST endpoint URL - a destination for migrated data"
        required()
        action ((url, conf) => conf.copy(producerRESTEndpointUrl = url)))

      (opt[File]('e', "failrec")
        valueName "<file>"
        text
        """|A file where a list of failed lineage IDs will be written to.
           |Running migrator with the option '-r' will repeat attempt to migrate lineages from this file."""
          .stripMargin
        action ((file, conf) => conf.copy(failRecFileOut = Some(file))))

      (opt[File]('r', "retry-from")
        valueName "<file>"
        text "A failrec file (see option '-e') to retry from."
        action ((file, conf) => conf.copy(failRecFileIn = Some(file))))

      (opt[Int]('b', "batch-size")
        text s"Number of lineages per batch. (Default is ${MigratorConfig.empty.batchSize})"
        validate (x => if (x > 0) success else failure("<batch-size> should be a positive number"))
        action ((value, conf) => conf.copy(batchSize = value)))

      (opt[Int]('n', "batch-max")
        text s"Number of batches to process. Negative value means unbounded. (Default is ${MigratorConfig.empty.batchesMax})"
        action ((value, conf) => conf.copy(batchesMax = value)))

      (opt[Unit]('c', "continuous")
        text s"Watch the source database and migrate the incoming data on the fly"
        action ((_, conf) => conf.copy(continuousMode = true)))

      (opt[String]('l', "log-level")
        text s"Log level (`ALL`, `TRACE`, `DEBUG`, `INFO`, `WARN`, `ERROR`, `OFF`). Default is `ERROR`"
        action ((str, conf) => conf.copy(logLevel = Level.valueOf(str))))

      help("help").text("prints this usage text")
    }

    cliParser.parse(args, MigratorConfig.empty) match {
      case Some(config) =>
        val stats = migratorTool.migrate(config)
        Thread.sleep(1.second.toMillis) // give a chance to async logs to print out before the final message.
        println() // to make sure there is a new line before the final message.
        stats match {
          case Stats(succ, 0) if succ > 0 =>
            println(ansi"%green{SUCCESS}. All records migrated: ${stats.processed}")
            sys.exit(ExitStatus.OK)
          case Stats(0, fails) if fails > 0 =>
            println(ansi"%red{FAILURE}. All records failed: ${stats.processed}")
            sys.exit(ExitStatus.ERROR)
          case Stats(succ, fails) if succ > 0 && fails > 0 =>
            println(ansi"%yellow{WARNING}. Totally processed: ${stats.processed} (of which successfully migrated: $succ, failed: $fails)")
            sys.exit(ExitStatus.WARN)
          case Stats(0, 0) =>
            println(ansi"%yellow{WARNING}. No records found.")
            sys.exit(ExitStatus.WARN)
        }

      case None =>
        cliParser.terminate(Left(""))
    }
  }
}