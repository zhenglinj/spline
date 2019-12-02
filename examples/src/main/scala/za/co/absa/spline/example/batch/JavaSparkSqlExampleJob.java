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

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import za.co.absa.spline.harvester.SparkLineageInitializer;

public class JavaSparkSqlExampleJob {

    public static void main(String[] args) {
        SparkSession.Builder builder = SparkSession.builder();
        SparkSession session = builder.appName("java example app").master("local[*]").getOrCreate();
        // configure Spline to track lineage
        SparkLineageInitializer.enableLineageTracking(session);
        Dataset<Row> sourceDs = session.read()
                .option("header", "true")
                .option("inferSchema", "true")
                .csv("examples/data/input/batch/wikidata.csv")
                .as("source");
        sourceDs.createOrReplaceTempView("wikidata");
        String sparkSql = "select * from wikidata where total_response_size < 100";
        Dataset<Row> filterDs = session.sql(sparkSql);
        filterDs.show();
        filterDs.write()
                .mode(SaveMode.Overwrite)
                .csv("examples/data/output/batch/java-sample-sql.csv");
    }
}

/*
ExecutionPlan(d0d2a988-5b40-4435-bbb1-70eb76154763,Operations(Vector(ReadOperation(ArrayBuffer(file:/C:/Projects/spline/examples/data/input/batch/wikidata.csv),5,Some(List(477b0ef7-1097-4dbf-8400-e1e69dbecc26, 7b54452f-7798-4d68-8bd2-23c2fe2590a9, cc5a00a0-eea7-47c2-a9ec-a5b02e8cdbdc, fefc5e5a-7aa1-49eb-ac19-9af735cf6df5, e0012c8d-7efa-49ff-9fc9-9f6f42e68beb)),Map(name -> LogicalRelation, sourceType -> Some(CSV), inferschema -> true, header -> true))),WriteOperation(file:/C:/Projects/spline/examples/data/output/batch/java-sample-sql.csv,false,0,List(1),Some(List(e0a91dc5-c086-418b-b6b7-fd3c61501b5c, b656da99-e63a-4a68-8e3e-02f7941b55bd, f25468ec-7756-43c0-a9ab-4512328ef931, bbdb270e-8a4f-4c1d-928b-ee1ce8df5943, fd6e89ae-403d-4f94-a0ff-69bbb3cd741d)),Map(name -> SaveIntoDataSourceCommand, destinationType -> Some(csv), path -> examples/data/output/batch/java-sample-sql.csv)),Vector(DataOperation(4,List(5),Some(List(228ca577-fbaa-4c8e-b8d8-18dd0b27574a, f160b310-00ff-4685-82e7-0b2294359b48, 162ea1a8-c663-436b-95d5-2379823ce739, 6be833e1-ff2e-4094-a26c-dff0feff81e4, 0e7d2b58-69e4-4296-9443-a7fd82039741)),Map(name -> SubqueryAlias, alias -> Some(source))), DataOperation(3,List(4),Some(List(e0a91dc5-c086-418b-b6b7-fd3c61501b5c, b656da99-e63a-4a68-8e3e-02f7941b55bd, f25468ec-7756-43c0-a9ab-4512328ef931, bbdb270e-8a4f-4c1d-928b-ee1ce8df5943, fd6e89ae-403d-4f94-a0ff-69bbb3cd741d)),Map(name -> SubqueryAlias, alias -> Some(wikidata))), DataOperation(2,List(3),Some(List(e0a91dc5-c086-418b-b6b7-fd3c61501b5c, b656da99-e63a-4a68-8e3e-02f7941b55bd, f25468ec-7756-43c0-a9ab-4512328ef931, bbdb270e-8a4f-4c1d-928b-ee1ce8df5943, fd6e89ae-403d-4f94-a0ff-69bbb3cd741d)),Map(name -> Filter, condition -> Some(Binary(<,87e296ad-b5da-47a2-99b0-b39efb61b9f0,List(AttrRef(fd6e89ae-403d-4f94-a0ff-69bbb3cd741d), Literal(100,f648a672-4720-4b49-9703-6ef9d3909e4e)))))), DataOperation(1,List(2),Some(List(e0a91dc5-c086-418b-b6b7-fd3c61501b5c, b656da99-e63a-4a68-8e3e-02f7941b55bd, f25468ec-7756-43c0-a9ab-4512328ef931, bbdb270e-8a4f-4c1d-928b-ee1ce8df5943, fd6e89ae-403d-4f94-a0ff-69bbb3cd741d)),Map(name -> Project, projectList -> Some(Vector(List(date, AttrRef(e0a91dc5-c086-418b-b6b7-fd3c61501b5c)), List(domain_code, AttrRef(b656da99-e63a-4a68-8e3e-02f7941b55bd)), List(page_title, AttrRef(f25468ec-7756-43c0-a9ab-4512328ef931)), List(count_views, AttrRef(bbdb270e-8a4f-4c1d-928b-ee1ce8df5943)), List(total_response_size, AttrRef(fd6e89ae-403d-4f94-a0ff-69bbb3cd741d)))))))),SystemInfo(spark,2.2.2),Some(AgentInfo(spline,0.4.0-SNAPSHOT)),Map(appName -> java example app, dataTypes -> Stream(Simple(8f686dc3-17dd-446b-98e5-5ca650297eb0,timestamp,true), ?), attributes -> Stream(Attribute(e0a91dc5-c086-418b-b6b7-fd3c61501b5c,date,8f686dc3-17dd-446b-98e5-5ca650297eb0), ?)))
ExecutionEvent(d0d2a988-5b40-4435-bbb1-70eb76154763,1574841521245,None,Map(appId -> local-1574841513765, readMetrics -> Map(), writeMetrics -> Map()))
 */
