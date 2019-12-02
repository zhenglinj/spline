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

package za.co.absa.spline.example.batch;

import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import scala.reflect.ClassTag$;
import za.co.absa.spline.harvester.SparkLineageInitializer;

import java.util.List;

public class JavaBroadcastExampleJob {

    public static void main(String[] args) {
        SparkSession.Builder builder = SparkSession.builder();
        SparkSession session = builder.appName("java example app").master("local[*]").getOrCreate();
        // configure Spline to track lineage
        SparkLineageInitializer.enableLineageTracking(session);

        //TODO
        List<Row> num2String = session.read()
                .option("header", "true")
                .option("inferSchema", "true")
                .csv("examples/data/input/batch/map-broadcast.csv")
                .collectAsList();
        Broadcast<List<Row>> broadcast = session.sparkContext().broadcast(num2String, ClassTag$.MODULE$.apply(List.class));
        Dataset<Row> sourceDs = session.read()
                .option("header", "true")
                .option("inferSchema", "true")
                .csv("examples/data/input/batch/wikidata.csv")
                .as("source");
        Dataset<Row> filterDs = sourceDs.filter(sourceDs.col("total_response_size").$less(100));
        filterDs.show();
        filterDs.write()
                .mode(SaveMode.Overwrite)
                .csv("examples/data/output/batch/java-sample.csv");
    }
}

/*
ExecutionPlan(7b722756-e050-43a2-b1b3-50bc4210abe1,Operations(Vector(ReadOperation(ArrayBuffer(file:/C:/Projects/spline/examples/data/input/batch/wikidata.csv),3,Some(List(6bf06b2a-a37f-4668-81c6-517189055861, 63d8957f-050a-453e-8045-378c23299c51, 91b960c1-c5a5-40d0-975b-f5e9d9688045, 1783063d-0726-4984-85fb-0fb466156d06, c0e00421-a3cd-4c6e-8d27-10ddeb212145)),Map(name -> LogicalRelation, sourceType -> Some(CSV), inferschema -> true, header -> true))),WriteOperation(file:/C:/Projects/spline/examples/data/output/batch/java-sample.csv,false,0,List(1),Some(List(a44e01c3-5cd4-481f-a0f9-d85219bb1fb1, ffd887c7-5b9d-4d21-855d-ada4c2ad6b7d, 63bb92a9-4650-4365-80ff-0e8afc0215e3, 1beedc54-1bf9-42aa-8299-fc126547e311, b50e9251-e88e-4f33-b324-53ea9ac62cad)),Map(name -> SaveIntoDataSourceCommand, destinationType -> Some(csv), path -> examples/data/output/batch/java-sample.csv)),Vector(DataOperation(2,List(3),Some(List(a44e01c3-5cd4-481f-a0f9-d85219bb1fb1, ffd887c7-5b9d-4d21-855d-ada4c2ad6b7d, 63bb92a9-4650-4365-80ff-0e8afc0215e3, 1beedc54-1bf9-42aa-8299-fc126547e311, b50e9251-e88e-4f33-b324-53ea9ac62cad)),Map(name -> SubqueryAlias, alias -> Some(source))), DataOperation(1,List(2),Some(List(a44e01c3-5cd4-481f-a0f9-d85219bb1fb1, ffd887c7-5b9d-4d21-855d-ada4c2ad6b7d, 63bb92a9-4650-4365-80ff-0e8afc0215e3, 1beedc54-1bf9-42aa-8299-fc126547e311, b50e9251-e88e-4f33-b324-53ea9ac62cad)),Map(name -> Filter, condition -> Some(Binary(<,6af4d10c-fefd-4798-b65b-3702007c0ee2,List(AttrRef(b50e9251-e88e-4f33-b324-53ea9ac62cad), Literal(100,6c57b4ba-ab8d-4b11-ba75-ae467fc49b79)))))))),SystemInfo(spark,2.2.2),Some(AgentInfo(spline,0.4.0-SNAPSHOT)),Map(appName -> java example app, dataTypes -> Stream(Simple(1add07d8-996a-4e64-b455-767818887654,timestamp,true), ?), attributes -> Stream(Attribute(a44e01c3-5cd4-481f-a0f9-d85219bb1fb1,date,1add07d8-996a-4e64-b455-767818887654), ?)))
ExecutionEvent(7b722756-e050-43a2-b1b3-50bc4210abe1,1574252337470,None,Map(appId -> local-1574252331563, readMetrics -> Map(), writeMetrics -> Map()))
 */
