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

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;
import scala.reflect.ClassTag$;
import za.co.absa.spline.harvester.SparkLineageInitializer;

import java.io.IOException;
import java.util.List;

public class JavaRddWithBroadcastExampleJob {
    public static void main(String[] args) throws IOException {
        SparkSession.Builder builder = SparkSession.builder();
        SparkSession session = builder.appName("java example app").master("local[*]").getOrCreate();
        // configure Spline to track lineage
        SparkLineageInitializer.enableLineageTracking(session);

        List<Row> rows = session.read()
                .option("header", "true")
                .option("inferSchema", "true")
                .csv("examples/data/input/batch/map-broadcast.csv")
                .as("source")
                .collectAsList();
        Broadcast<List<Row>> broadcast = session.sparkContext().broadcast(rows, ClassTag$.MODULE$.apply(List.class));
        JavaRDD<Row> sourceRdd = session.read()
                .option("header", "true")
                .option("inferSchema", "true")
                .csv("examples/data/input/batch/wikidata.csv")
                .as("source")
                .toJavaRDD();
        JavaRDD<Tuple2> filterRdd = sourceRdd.filter(row -> row.<Integer>getAs("total_response_size") < 100)
                .map(row -> {
                    String stringNum = broadcast.value().stream()
                            .filter(r -> r.<Integer>getAs("key").equals(row.getAs("total_response_size")))
                            .map(r -> (String) r.<String>get(1)).findFirst().orElse("");
                    return new Tuple2<>(row, stringNum);
                });
        HdfsHelper.deleteDirectoryRecursively("examples/data/output/batch/java-sample-rdd.csv");
        filterRdd.saveAsTextFile("examples/data/output/batch/java-sample-rdd.csv");
    }
}

