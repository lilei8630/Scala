package Word2Vec

import org.apache.spark._
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.mllib.feature.{Word2Vec, Word2VecModel}

/**
  * Created by extends_die on 15/12/8.
  */
object trainNewsData {

  def main(args: Array[String]): Unit = {

    val conf_spark = new SparkConf().setAppName("trainNewsData")
                                    .setMaster("local[2]")
                                    .set("spark.serializer","org.apache.spark.serializer.KryoSerializer")
                                    .set("spark.kryoserializer.buffer.max","512")

    val sc = new SparkContext(conf_spark)

    val newsdata = sc.textFile("hdfs://IP:9000/user/bd/newsdata/*").map(line => line.split(" ").toSeq)

    val word2vec = new Word2Vec()

    word2vec.setMinCount(10)

    word2vec.setNumPartitions(10)

    word2vec.setNumIterations(6)

    word2vec.setVectorSize(200)

    val model = word2vec.fit(newsdata)

    // Save and load model
    model.save(sc, "hdfs://183.174.228.33:9000/user/bd/Word2Vec_patition10_iteration6_size200")

    val sameModel = Word2VecModel.load(sc, "hdfs://ip:9000/user/bd/Word2Vec_patition10_iteration6_size200")

    val synonyms = model.findSynonyms("", 40)

    for ((synonym, cosineSimilarity) <- synonyms) {

      println(s"$synonym $cosineSimilarity")

    }


//    val d = sqlContext.parquetFile("hdfs://localhost:9000/Word2Vec_news2007/data/part-r-00000-59d056d0-bc4b-43c6-85c8-e5a7f30fcae9.gz.parquet")
//    d.map{r => r.getString(0) + " " + r.getSeq(1).mkString(" ")}.saveAsTextFile("{output directory}/vectors")


  }
}
