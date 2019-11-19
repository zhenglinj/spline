/*
 * Copyright 2019 ABSA Group Limited
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

package za.co.absa.spline.producer.rest.controller

import java.util.UUID

import io.swagger.annotations.{Api, ApiOperation, ApiResponse, ApiResponses}
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.http.HttpStatus
import org.springframework.stereotype.Controller
import org.springframework.web.bind.annotation._
import za.co.absa.spline.producer.rest.model.{ExecutionEvent, ExecutionPlan}
import za.co.absa.spline.producer.service.repo.ExecutionProducerRepository

import scala.concurrent.{ExecutionContext, Future}

@Controller
@Api(tags = Array("execution"))
@RequestMapping(
  value = Array("/execution"),
  consumes = Array("application/json"),
  produces = Array("application/json"))
class ExecutionProducerController @Autowired()(
  val repo: ExecutionProducerRepository) {

  import ExecutionContext.Implicits.global

  @PostMapping(Array("/plan"))
  @ApiOperation(
    value = "Save Execution Plan",
    notes =
      """
        Saves an Execution Plan and returns its new UUID.
        In most cases the method returns the same UUID that was passed in the request. However in the future Spline versions the server could
        recognize duplicated execution plans, in which case the method will return UUID of already existing execution plan.
        In all the future interactions with the Spline API, the client must use this UUID instead of the original UUID to refer the given
        Execution Plan.

        RequestBody format :

        {
          // Id of the execution plan to create
          id: UUID
          // List of operations of the execution plan
          operations: Lists of operations
          {
                //Array of read operations of the execution plan
                reads: Array[ReadOperation]
                [
                  // Array of input DataSources for this operation
                  inputSources : Array[DataSource]
                  // Id of this operation
                  id : Int
                  // List of references to the dataTypes
                  schema: Array[String]
                  // Other parameters containing for instance the name of the operation
                  params: Map[String, Any]
                ]
                // Write operation of the execution plan
                write: WriteOperation{
                  // Id of the write operation
                  id: Int
                  // output DataSource uri
                  outputSource: String
                  // append mode - true if append, false if override
                  append: Boolean
                  // Array of the children operations id
                  childIds: Array[Int]
                  // List of references to the dataTypes
                  schema: Option[Any]
                  // Other parameters containing for instance the name of the operation
                  params: Map[String, Any]
                }
                other: Array[DataOperation]
                [
                  // Id of the Data operation
                  id: Int,
                  // Array of the children operations id
                  childIds: Array[Int],
                  // List of references to the dataTypes
                  schema: Option[Any] = None,
                  // Other parameters containing for instance the name of the operation
                  params: Map[String, Any]
                ]
          }
          // Information about a data framework in use (e.g. Spark, StreamSets etc)
          systemInfo: SystemInfo
          {
           name : String
           version : String
          }
          // Spline agent information
          agentInfo: Option[AgentInfo]
          {
            name: String
            version: String
          }
          // Map containing any other extra info like the name of the application
          extraInfo: Map[String, Any]
        }
      """)
  @ApiResponses(Array(
    new ApiResponse(code = 200, message = "Execution Plan is stored with the UUID returned in a response body")
  ))
  @ResponseBody
  def executionPlan(@RequestBody execPlan: ExecutionPlan): Future[UUID] = repo
    .insertExecutionPlan(execPlan)
    .map(_ => execPlan.id)

  @PostMapping(Array("/event"))
  @ApiOperation(
    value = "Save Execution Events",
    notes =
      """
        Saves a list of Execution Events.

        RequestBody format :

        Array[ExecutionEvent]
        [
          // Reference to the Executionplan Id that was triggered
          planId: UUID
          // Timestamp of the execution
          timestamp: Long
          // If an error occurred during the execution
          error: Option[Any]
          // Any other extra information related to the execution Event Like the application Id for instance
          extra: Map[String, Any]
        ]
      """)
  @ApiResponses(Array(
    new ApiResponse(code = 204, message = "All execution Events are successfully stored")
  ))
  @ResponseStatus(HttpStatus.NO_CONTENT)
  def executionEvent(@RequestBody execEvents: Array[ExecutionEvent]): Future[Unit] = {
    repo.insertExecutionEvents(execEvents)
  }

}
