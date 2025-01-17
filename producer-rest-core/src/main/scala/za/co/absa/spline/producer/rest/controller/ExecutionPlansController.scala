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
import org.springframework.web.bind.annotation._
import za.co.absa.spline.producer.model.ExecutionPlan
import za.co.absa.spline.producer.service.repo.ExecutionProducerRepository

import scala.concurrent.{ExecutionContext, Future}

@RestController
@Api(tags = Array("execution"))
class ExecutionPlansController @Autowired()(
  val repo: ExecutionProducerRepository) {

  import ExecutionContext.Implicits.global

  @PostMapping(Array("/execution-plans"))
  @ApiOperation(
    value = "Save Execution Plan",
    notes =
      """
        Saves an Execution Plan and returns its new UUID.

        In most cases the method returns the same UUID that was passed in the request.
        However in the future Spline versions the server could recognize duplicated
        execution plans, in which case the method will return UUID of already existing
        execution plan. In all the future interactions with the Spline API, the client
        must use this UUID instead of the original UUID to refer the given Execution Plan.

        Payload format:

        {
          // Global unique identifier of the execution plan
          id: <UUID>,

          operations: {

            // Write operation
            write: {
              // Write operation ID (a number, unique in the scope of the current execution plan)
              id: <number>,
              // Destination URI, where the data has been written to
              outputSource: <URI>,
              // Shows if the write operation appended or replaced the data in the target destination
              append: <boolean>,
              // Array of preceding operations IDs,
              // i.e. operations that serves as an input for the current operation
              childIds: [<number>],
              // [Optional] Object that describes the schema of the operation output
              schema: {...},
              // [Optional] Custom info about the operation
              params: {...}
            },

            // Array of read operations
            reads: [
              {
                // Operation ID (see above)
                id: <number>,
                // Source URIs, where the data has been read from
                inputSources: [<URI>],
                // [Optional] Object that describes the schema of the operation output
                schema: {...},
                // [Optional] Custom info about the operation
                params: {...}
              },
              ...
            ],

            // Other operations
            other: [
              {
                // Operation ID (see above)
                id: <number>,
                // Array of preceding operations IDs (see above)
                childIds: [<number>],
                // [Optional] Object that describes the schema of the operation output
                schema: {...},
                // [Optional] Custom info about the operation
                params: {...}
              },
              ...
            ]
          },

          // Information about the data framework in use (e.g. Spark, StreamSets etc)
          systemInfo: {
            name: <string>,
            version: <string>
          },

          // [Optional] Spline agent information
          agentInfo: {
            name: <string>,
            version: <string>
          },

          // [Optional] Any other extra info associated with the current execution plan
          extraInfo: {...}
        }
      """)
  @ApiResponses(Array(
    new ApiResponse(code = 201, message = "Execution Plan is stored with the UUID returned in a response body")
  ))
  @ResponseStatus(HttpStatus.CREATED)
  def executionPlan(@RequestBody execPlan: ExecutionPlan): Future[UUID] = repo
    .insertExecutionPlan(execPlan)
    .map(_ => execPlan.id)

}
