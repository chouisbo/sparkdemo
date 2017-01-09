

val links = sc.parallelize(Array(('A', Array('D')), ('B', Array('A')), ('C', Array('A', 'B')), ('D', Array('A', 'C'))), 2).map(x => (x._1, x._2)).partitionBy(new org.apache.spark.HashPartitioner(2)).cache()

var ranks = sc.parallelize(Array(('A', 1.0), ('B', 1.0), ('C', 1.0), ('D', 1.0)), 2)

val ITERATIONS = 100
for (i <- 1 to ITERATIONS) {
    val contribs = links.join(ranks, 2).flatMap {
        case (url, (links, rank)) => links.map(dest => (dest, rank/links.size))
    }
    ranks = contribs.reduceByKey(_ + _, 2).mapValues(0.15 + 0.85 * _)
}

ranks.foreach(kv => println(kv._1+" => "+kv._2))

ranks.take(4)

