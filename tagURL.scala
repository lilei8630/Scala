package Word2Vec

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.mllib.feature.Word2VecModel
import com.github.fommil.netlib.BLAS.{getInstance => blas}
import scala.collection.mutable.ArrayBuffer
import org.apache.log4j.{Level, Logger}
/**
  * Created by extends_die on 15/12/22.
  */
object tagURL {

  def sumArray(m: Array[Double], n: Array[Double]): Array[Double] = {
    for (i <- 0 until m.length) {
      m(i) += n(i)
    }
    return m
  }

  def divArray(m: Array[Double], divisor: Double): Array[Double] = {
    for (i <- 0 until m.length) {
      m(i) /= divisor
    }
    return m
  }

  def wordToVector(w: String, m: Word2VecModel): Vector = {
    try {
      // sameModel.transform("政治")
      return m.transform(w)
    } catch {
      case e: Exception => return Vectors.zeros(200)
    }
  }

  def findTopK(word:String,k:Int,word2VecModel: Word2VecModel):Unit={

    val synonyms = word2VecModel.findSynonyms(word, k)

    for ((synonym, cosineSimilarity) <- synonyms) {

      println(s"$synonym $cosineSimilarity")

    }
  }

//  def calSimilarity(word1:Array[Double],word2:Array[Double]):Double={
//    var dot:Double= 0
//    for(i<-0 to (word1.length-1)){
//      dot +=word1(i)*word2(i)
//    }
//    dot
//  }
//def getWord2VecMap_shell(sc: SparkContext, path: String): Map[String, Array[Float]] = {
//  val dataPath = path++"/data"
//  val sqlContext = new SQLContext(sc)
//  val dataFrame = sqlContext.read.parquet(dataPath)
//  val dataArray = dataFrame.select("word", "vector").collect()
//  val word2VecMap = dataArray.map(i => (i.getString(0), i.getSeq[Float](1).toArray)).toMap
//  word2VecMap
//}

  def getWord2VecMap(sc: SparkContext, path: String): Map[String, Array[Float]] = {
    //val dataPath = "hdfs://183.174.228.33:9000/user/bd/Word2Vec_patition10_iteration6_size200/data"
//    val dataPath ="hdfs://183.174.228.33:9000/user/bd/gogo/vector.skip.win2.100.float"
    val dataPath = path++"/data"
    val sqlContext = new SQLContext(sc)
    val dataFrame = sqlContext.read.parquet(dataPath)
    val dataArray = dataFrame.select("word", "vector").collect()
    val word2VecMap = dataArray.map(i => (i.getString(0), i.getSeq[Float](1).toArray)).toMap
    word2VecMap
  }

  def buildWordIndex(model: Map[String, Array[Float]]): Map[String, Int] = {
    model.keys.zipWithIndex.toMap
  }

  def buildWordVectors(model: Map[String, Array[Float]]): Array[Float] = {
    require(model.nonEmpty, "Word2VecMap should be non-empty")
    val (vectorSize, numWords) = (model.head._2.size, model.size)
    val wordList = model.keys.toArray
    val wordVectors = new Array[Float](vectorSize * numWords)
    var i = 0
    while (i < numWords) {
      Array.copy(model(wordList(i)), 0, wordVectors, i * vectorSize, vectorSize)
      i += 1
    }
    wordVectors
  }

  def cosineSimilarityByVec(v1: Array[Float], v2: Array[Float]): Double = {
    require(v1.length == v2.length, "Vectors should have the same length")
    val n = v1.length
    val norm1 = blas.snrm2(n, v1, 1)
    val norm2 = blas.snrm2(n, v2, 1)
    if (norm1 == 0 || norm2 == 0) return 0.0
    blas.sdot(n, v1, 1, v2, 1) / norm1 / norm2
  }

  /**
    * 下面这些方法与TagURL有关
    * */
  def transform(word: String,wordIndex: Map[String, Int], wordVectors: Array[Float],vectorSize:Int): Vector = {
    wordIndex.get(word) match {
      case Some(ind) =>
        val vec = wordVectors.slice(ind * vectorSize, ind * vectorSize + vectorSize)
        Vectors.dense(vec.map(_.toDouble))
      case None =>
        return Vectors.zeros(100)
    }
  }

  def cosineSimilarityByWord(word1:String, word2: String,wordIndex: Map[String, Int], wordVectors: Array[Float],vectorSize:Int): Double = {
    val v1  = transform(word1,wordIndex,wordVectors,vectorSize).toArray.map(_.toFloat)
    val v2  = transform(word2,wordIndex,wordVectors,vectorSize).toArray.map(_.toFloat)
    require(v1.length == v2.length, "Vectors should have the same length")
    val n = v1.length
    val norm1 = blas.snrm2(n, v1, 1)
    val norm2 = blas.snrm2(n, v2, 1)
    if (norm1 == 0 || norm2 == 0) return 0.0
    blas.sdot(n, v1, 1, v2, 1) / norm1 / norm2
  }

  def getSimilarityPerTag(sentence:String,tags:Array[String],wordIndex: Map[String, Int],wordVectors: Array[Float],vectorSize:Int): String={
//  val segBuffer = sentence.split("\\s+").toBuffer.filter(x=>x.trim.length>1)
    val segBuffer = sentence.split("\\s+").toBuffer
    var tagBuffer = tags.toBuffer
    //扫描分词结果直接出现在词典中的词
    var cooccurrence  = new ArrayBuffer[String](10)
    segBuffer.foreach(x=>{
      if(tagBuffer.contains(x)) {
        cooccurrence+=(x+" "+1.0)
        tagBuffer-=x
      }
    })
    val cooccurrence_arr = cooccurrence.toArray
    val seg_arr = segBuffer.toArray
    val tag_arr = tagBuffer.toArray
    val numTags = tag_arr.length
    val numWords = seg_arr.length
    val finalSimilarity = Array.fill[Float](numTags)(0)
    var tagIndex = 0
    var segIndex = 0
    if(seg_arr.length>0){
      var word:String = seg_arr(0)
      var tag:String = tag_arr(0)
      var count: Int = 0
      while (tagIndex < numTags) {
        tag = tag_arr(tagIndex)
        var similarity: Double = 0.0
        count = 0
        while (segIndex < numWords) {
          word = seg_arr(segIndex)
          val sim = cosineSimilarityByWord(tag, word, wordIndex, wordVectors, 100)
          if (sim != 0.0) {
            count += 1
          }
          similarity += sim
          //        println(tag+" "+word+" "+sim)
          segIndex += 1
        }
        if (count != 0) {
          finalSimilarity(tagIndex) = similarity.toFloat / count
        } else {
          finalSimilarity(tagIndex) = 0.0f
        }
        tagIndex += 1
        segIndex = 0
      }
    }
    //返回的结果数组,按照相似度从小到大排序,数据类型为Array[(String, Float)]
    val resArray = tag_arr.zip(finalSimilarity).toSeq.sortBy(-_._2).toArray.filter(x=>x._2>0.4f)
    var res1=""
    var res2=""
    var res3=""
    var res4=""
    var res5=""
    if(resArray.length ==1){
      res1 = resArray(0)._1+" "+resArray(0)._2
    }
    if(resArray.length ==2){
      res1 = resArray(0)._1+" "+resArray(0)._2
      res2 = resArray(1)._1+" "+resArray(1)._2
    }
    if(resArray.length ==3){
      res1 = resArray(0)._1+" "+resArray(0)._2
      res2 = resArray(1)._1+" "+resArray(1)._2
      res3 = resArray(2)._1+" "+resArray(2)._2
    }
    if(resArray.length ==4){
      res1 = resArray(0)._1+" "+resArray(0)._2
      res2 = resArray(1)._1+" "+resArray(1)._2
      res3 = resArray(2)._1+" "+resArray(2)._2
      res4 = resArray(3)._1+" "+resArray(3)._2
    }
    if(resArray.length >4){
      res1 = resArray(0)._1+" "+resArray(0)._2
      res2 = resArray(1)._1+" "+resArray(1)._2
      res3 = resArray(2)._1+" "+resArray(2)._2
      res4 = resArray(3)._1+" "+resArray(3)._2
      res5 = resArray(4)._1+" "+resArray(4)._2
    }
    if(cooccurrence_arr.length >0)
      cooccurrence_arr.mkString(" ")+" "+res1+" "+res2+" "+res3+" "+res4+" "+res5
    else
      res1+" "+res2+" "+res3+" "+res4+" "+res5
  }

  // wordVecNorms: Array of length numWords, each value being the Euclidean norm of the wordVector.
   def findWordVecNorms( wordVectors: Array[Float],numWords:Int,vectorSize: Int): Array[Double] = {
    val wordVecNorms = new Array[Double](numWords)
    var i = 0
    while (i < numWords) {
      val vec = wordVectors.slice(i * vectorSize, i * vectorSize + vectorSize)
      wordVecNorms(i) = blas.snrm2(vectorSize, vec, 1)
      i += 1
    }
    wordVecNorms
  }

  def findSynonymsbyVEC(vector: Vector, num: Int,wordVecNorms:Array[Double],wordIndex: Map[String, Int], wordVectors: Array[Float],vectorSize:Int,numWords:Int): Array[(String, Double)] = {
    // TODO: optimize top-k
    val fVector = vector.toArray.map(_.toFloat)
    val cosineVec = Array.fill[Float](numWords)(0)
    val alpha: Float = 1
    val beta: Float = 0
    val (wl, _) = wordIndex.toSeq.sortBy(_._2).unzip
    val wordList: Array[String] = wl.toArray
    blas.sgemv(
      "T", vectorSize, numWords, alpha, wordVectors, vectorSize, fVector, 1, beta, cosineVec, 1)
    // Need not divide with the norm of the given vector since it is constant.
    val cosVec = cosineVec.map(_.toDouble)
    var ind = 0
    while (ind < numWords) {
      cosVec(ind) /= wordVecNorms(ind)
      ind += 1
    }
    wordList.zip(cosVec)
      .toSeq
      .sortBy(- _._2)
      .take(num + 1)
      .tail
      .toArray
  }

  def findSynonyms(word: String, num: Int,wordVecNorms:Array[Double],wordIndex: Map[String, Int], wordVectors: Array[Float],vectorSize:Int,numWords:Int): Unit = {

    val vector  = transform(word,wordIndex,wordVectors,vectorSize)

    val synonyms = findSynonymsbyVEC(vector, num,wordVecNorms,wordIndex,wordVectors,vectorSize,numWords)

    for ((synonym, cosineSimilarity) <- synonyms) {

      println(s"$synonym $cosineSimilarity")

    }
  }

  /**
    * 加载gogo的word2vec文件
    * */
  case class MyWord2Vec(word: String, vector: Array[Float])
  def loadW2V(sc:SparkContext,path:String):(Int,Int,Map[String, Int],Array[Float])={
    val rawVectors = sc.textFile(path)
    //返回参数1:wordNum
    val wordNum = rawVectors.first.split("\\s+")(0).toInt
    //返回参数2:size
    val size = rawVectors.first.split("\\s+")(1).toInt

    //过滤掉第一行
    val word_vectors = rawVectors.filter(line=>{
      var flag = true
      val length = line.split("\\s+").length
      if(length==2) flag = false
      flag
    })
    //词向量以word->Array形式显示
    val word_vectors_in_array =  word_vectors.map(line=>{
      val vector = line.split("\\s+").toBuffer
      val word = vector.remove(0)
      val vec = vector.toArray.map(_.toFloat)
      (word,vec)
    })
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._
    val W2V = word_vectors_in_array.map(p => MyWord2Vec(p._1, p._2)).toDF()
    W2V.registerTempTable("W2V")
    val dataArray = W2V.select("word", "vector").collect()
    val word2VecMap = dataArray.map(i => (i.getString(0), i.getSeq[Float](1).toArray)).toMap
    //返回参数3:wordIndex
    val wordIndex:Map[String, Int] = word2VecMap.keys.zipWithIndex.toMap
    //返回参数4:wordVectors
    val (vectorSize, numWords) = (word2VecMap.head._2.size, word2VecMap.size)
    val wordList = word2VecMap.keys.toArray
    val wordVectors = new Array[Float](vectorSize * numWords)
    var i = 0
    while (i < numWords) {
      Array.copy(word2VecMap(wordList(i)), 0, wordVectors, i * vectorSize, vectorSize)
      i += 1
    }
    (wordNum,size,wordIndex,wordVectors)
  }

  def loadTag(sc:SparkContext,path:String):Array[String]={

    println("loadTag...")

    val tags = sc.textFile(path)

    val arr = new ArrayBuffer[String]()

    tags.collect().foreach(x=>{
      arr += x
    })
    arr.toArray
  }

  def loadStopWords(sc:SparkContext,path:String):Array[String]={
    println("loadStopWords...")
    val tags = sc.textFile(path)
    val arr = new ArrayBuffer[String]()
    tags.collect().foreach(x=>{
      arr += x
    })
    arr.toArray
  }

  def filterStopWords(sentence:String,stopWords:Array[String]): String ={
    var res = ""
    val segBuffer = sentence.split("\\s+").toBuffer
    val stopWordsBuffer = stopWords.toBuffer
    //用停用词典过滤分词结果
    var filtered  = new ArrayBuffer[String]()
    segBuffer.foreach(x=>{
      if(!stopWordsBuffer.contains(x)) {
        filtered+=x
      }
    })
    val filteredArray = filtered.toArray

    if (filteredArray.length>0){
      res = filteredArray.mkString(" ")
    }
    res
  }

  def getLogdata(sc:SparkContext,sql:String):RDD[Row]={
    val sqlContext = new HiveContext(sc)
    val df = sqlContext.sql(sql).rdd
    df
  }

  def getHost(url:String):String ={
    var res = ""
    if(url.length==0||url==null){
      res =""
    }else{
      var doubleslash:Int = url.indexOf("//")
      if(doubleslash == -1)
        doubleslash = 0
      else
        doubleslash +=2
      var end:Int = url.indexOf('/', doubleslash)
      if(end < 0)
        end = url.length
      val port:Int = url.indexOf(':', doubleslash)
      if(port>0 && port < end)
        end = port
      res = url.substring(doubleslash,end)
    }
    res
  }

  def getBaseDomain(url:String):String ={
    val host:String = getHost(url)
    var res:String = ""
    var startIndex:Int = 0
    var nextIndex:Int = host.indexOf(".")
    val lastIndex:Int = host.lastIndexOf(".")
    while(nextIndex < lastIndex){
      startIndex = nextIndex + 1
      nextIndex = host.indexOf('.',startIndex)
    }
    if(startIndex > 0)
      res = host.substring(startIndex)
    else
      res = host
    res
  }

  def getMostFreq(allQuery:String,stopWords:Array[String]):String ={

//    val sentence = "国务院 葛延风 的 医改 判断 在 宏观 问题 上 仍然 有 不少 分歧 所以 需要 通过 进一步 研究 凝聚 共识 比如 在 医疗 卫生 问题 方面 政府 和 市场 政府 和 社会 力量 到底 是 什么 关系 政府 在 管 和 办 的 问题 到底 怎么 协调 分歧 不少"

    var res = ""
    val sentence = filterStopWords(allQuery,stopWords)

    val word_num = scala.collection.mutable.Map[String,Int]()

    val elems = sentence.split("\\s+").foreach(x=>{

      val num:Int = word_num.getOrElse(x,0)

      word_num(x) = num+1

    })

    val sorted = word_num.toSeq.filter(_._1.length>1).sortBy(-_._2).map(x=>x._1+" "+x._2).toArray

    if(sorted.length==1){
      res = res + sorted(0)+" "
    }
    if(sorted.length==2){
      res =  res + sorted(0)+" "
      res =  res + sorted(1)
    }
    if(sorted.length>=3){
      res =  res + sorted(0)+" "
      res =  res + sorted(1)+" "
      res =  res + sorted(2)+" "
    }
    res
  }

  def main(args: Array[String]) {

//    val sql = "select query,click.url from custom.common_wap_pv lateral view explode(click_result) click_result as click where logdate >= '2015110101' and logdate <= '2016030101' and clean_state = 'OK' and page_type = 'RESULTPAGE'"

    val conf= new SparkConf().setAppName("tagURL")

    conf.set("spark.driver.maxResultSize", "6g")
    conf.set("spark.yarn.executor.memoryOverhead","1024")

    val sc = new SparkContext(conf)
//    val rootLogger = Logger.getRootLogger()
//    rootLogger.setLevel(Level.ERROR)

//    //默认60000
//    sc.hadoopConfiguration.set("dfs.client.socket-timeout", "180000")
//    //默认80000
//    sc.hadoopConfiguration.set("dfs.datanode.socket.write.timeout", "180000")

    val stopWords = loadStopWords(sc,args(0))

    val tagArr = loadTag(sc,args(1))

    val (wordNum,size,wordIndex,wordVectors) = loadW2V(sc,args(2))

    val domain_allQuery = sc.textFile(args(3),200).
                          filter(_.split("\\t").length==2).
                          map(x=>{(x.split("\\t")(0),x.split("\\t")(1))}).
                          reduceByKey((x,y) => x+" "+y)

//    println(wordNum)
//    println(size)
//    println(wordIndex.size)
//    println(wordVectors.size)
    println(domain_allQuery.count())
//    domain_allQuery.cache()

    val domain_tag = domain_allQuery.map(x=>{

      val domain = x._1

      val allQuery = x._2
      println(domain)
      println(allQuery)

      val mostFreq = getMostFreq(allQuery,stopWords)

      val mostSim = getSimilarityPerTag(allQuery,tagArr,wordIndex,wordVectors,100)

      domain+"\t|"+mostFreq+mostSim

    }).saveAsTextFile(args(4))

//    val query_url = getLogdata(sc,sql)
//    val count = query_url.filter(x=>{
//      var flag = true
//      if((x.get(0)==null||x.get(1)==null)){
//        flag  = false
//      }
//      flag
//    }).map(x=>getHost(x.get(1).toString.trim)+"$$$$$$"+getBaseDomain(x.get(1).toString.trim)+"\t"+x.get(0).toString).saveAsTextFile("/user/lilei/host_site_query_15days")

//    println(count)
//    val url = "http://www.soku.com/detail/show/XMTE1MTc0MA==?siteId=14"

//    println(getBaseDomain(url))

//    println(query_url.count())
//    val (wordNum,size,wordIndex,wordVectors) = loadW2V(sc,args(0))
//    val tagArr = loadTag(sc,args(1))
//    val stopWords = loadStopWords(sc,args(2))
//    sc.textFile(args(3)).map(x=>{
//      val elems = x.split("\\t")
//      val uid = elems(0)
//      val nick = elems(1)
//      val time = elems(2)
//      val seg = filterStopWords(elems(3),stopWords)
//      val res = getSimilarityPerTag(seg,tagArr,wordIndex,wordVectors,100)
//      uid+"\t|"+nick+"\t|"+time+"\t|"+res+"\t|"+seg
//    }).saveAsTextFile(args(4))

//      val seg = "中国 应 尊重 朝 方 要求 并 考虑 毛泽东 使馆 人员 安全 撤回 大使馆 人员 吧 朝鲜 政府 在 五 号 向 外国 使馆 表示 因为 朝鲜 半岛 局势 紧张 要求 他们 考虑 撤离 在 平壤 的 使馆 人员 韩 联社 引述 俄罗斯 外交部 官员 报道 朝鲜 外务省 除 了 另行 向 俄罗斯 和 中国 驻 平壤 大使馆 通报 外 还 举行 新闻 发布 会 集体 通知 其他 国家"
//    val host_seg = sc.textFile("hdfs://183.174.228.33:9000/user/bd/gogo/Host_Seg").map(x=>{
//      val host = x.split("\\t")(0)
//      val sentence = x.split("\\t")(1)
//      val res = getSimilarityPerTag(sentence,tags,wordIndex,wordVectors,100)
//      host+"\t|"+res+"\t|"+sentence
//    })

//    host_seg.saveAsTextFile("hdfs://183.174.228.33:9000/user/bd/gogo/Host_tag_seg")

//    println(word2VecMap.size)
//    val wordIndex: Map[String, Int] = buildWordIndex(word2VecMap)
//    val wordVectors: Array[Float] = buildWordVectors(word2VecMap)

//    val conf_spark = new SparkConf().setAppName("tagURL").setMaster("local[2]")
//    val sc = new SparkContext(conf_spark)
//
//    val word2VecMap: Map[String, Array[Float]] = getWord2VecMap(sc,"hdfs://183.174.228.33:9000/user/bd/Word2Vec_patition10_iteration6_size200")
//    val wordIndex: Map[String, Int] = buildWordIndex(word2VecMap)
//    val wordVectors: Array[Float] = buildWordVectors(word2VecMap)
//    val wordVecNorms : Array[Double] = findWordVecNorms(wordVectors,4185425,100)
//
//    findSynonyms("味全", 40,wordVecNorms,wordIndex, wordVectors,100,4185425)
//
//    val vector  = transform("味全",wordIndex,wordVectors,100)

//    val sentence ="张赫宣###我们###不该###这样###的###ｌｏｖｅ###ｂｉｒｄｓ###曲婉婷###ｌｏｖｅ###ｂｉｒｄｓ###曲婉婷###有何###不###可###手机###铃声###假日###的###海滩###纯音乐###ｒ      ａｇｅ###ｏｎ###男子###游泳###部###经典###老歌###５００###首###怀旧###忧心###曲###播放###ｌｏｌ###歌###２０１５###流行###歌手###千秋###家国###梦###ｍｐ３###卡农###ｍｐ３###下载###"
//    val setentence ="张赫宣###我们###不该###这样###的###love###birds###曲婉婷###love###birds###曲婉婷###有何###不###可###手机###铃声###假日###的###海滩###纯音乐###r      age###on###男子###游泳###部###经典###老歌###500###首###怀旧###忧心###曲###播放###lol###歌###2015###流行###歌手###千秋###家国###梦###mp3###卡农###mp3###下载###"
//    val sentence1 = BCConvert.qj2bj(sentence)
//    println(sentence1)
    //    val sentence1 = sentence.replaceAll("\\s+","").replaceAll("###"," ")
//    val elems = sentence1.split("\\s+").foreach(println)

//    val sentence = "平凉 宾馆 武威 紫 云 阁 酒店 临夏 美 格 美 居 假日 酒店"
//    sentence.split("\\s+")

//    getSimilarityPerTag(sentence,tags,wordIndex,wordVectors,100)

//    var tags = Array("科技","电影","旅游","情感")

    //0.781488
//    cosineSimilarityByWord("北京","伊尔库茨克",wordIndex,wordVectors,100)
//    cosineSimilarityByWord("玛旁雍错","西藏",wordIndex,wordVectors,100)
//    cosineSimilarityByWord("中国","香港",wordIndex,wordVectors,100)
//    cosineSimilarityByWord("李克强","习近平",wordIndex,wordVectors,100)
//    cosineSimilarityByWord("自由","反恐",wordIndex,wordVectors,100)
//    cosineSimilarityByWord("法国","巴黎",wordIndex,wordVectors,100)
//    cosineSimilarityByWord("美国","华盛顿",wordIndex,wordVectors,100)
    //    val model = Word2VecModel.load(sc, "hdfs://183.174.228.33:9000/user/bd/Word2Vec_patition10_iteration6_size200")
////    val sameModel = Word2VecModel.load(sc, "hdfs://183.174.228.33:9000/user/bd/Word2Vec_news2007")
//
//    val allKey_url = sc.textFile("hdfs://183.174.228.33:9000/user/bd/gogo/allKey_url/*")
//
//    var tags = Array("政治","色情","军事", "图片","购彩","NBA","中超","电影","电视","音乐","时尚","美容","情感","资讯","综艺","公开课","股票","基金","商业","科技","手机",
//    "数码","家电","电脑","房产","家居","二手房","旅游","健康","汽车","购车","选车","博客","海外","小说","游戏","彩票","论坛","摄影","佛学","教育","亲子","艺术","交友")
//

//    /**给weibo打标签的任务*/
//    var tags =Array("三中全会","军事","军队","美国","美帝","日本","朝鲜","香港","钓鱼岛","台湾","叙利亚","埃及","中日","苏联","习近平","毛泽东","薄熙来","周永康",
//    "金正恩","袁裕来","马英九","毛主席","邓小平","柴静","何兵","王立军","民主","宪政","社会主义","法治","爱国","普世","人权","言论自由","禁言","敏感","福利",
//    "民生","文革","维权","上访","司法独立","城管","腐败","污染","环保","反腐","谣言","贪官","强拆","劳教","删帖","食品安全","医改","贪污","公知","五毛","公民"
//    ,"二代","汉奸","敌人","富人","精英","左派","宪法","国人","大国","冤案","立案","政治","体制","言论","运动","民意","意识形态","执政","政权","货币","经济","城镇化",
//    "夏俊峰","薛蛮子","吴虹飞","秦火火","立二拆四","张雪忠","夏业良","许志永","陈永洲","王功权","斯诺登","曼德拉","贸易")

//    val uid_nick_date_content_seg = sc.textFile("hdfs://183.174.228.33:9000/user/bd/weibo/weibo_politics_seg").map(x=>{
//      val elems =x.split("\\t\\$\\$\\$\\$\\t")
//      val uid = elems(0)
//      val nick = elems(1)
//      val date = elems(2)
//      val content = elems(3)
//      val sentence = elems(4)
//      val res = getSimilarityPerTag(sentence,tags,wordIndex,wordVectors,100)
//      uid+"\t|"+nick+"\t|"+date+"\t|"+res+"\t|"+content
//    })

//    uid_nick_date_content_seg.saveAsTextFile("hdfs://183.174.228.33:9000/user/bd/weibo/uid_nick_date_tag_content")

//    val sentence ="未来 经济 发展 下滑 社会 矛盾 加剧 任由 自由 主义 发展 社会  失去 控制 失控 结果  国民 一味 顺从 奴性 迅速 不受 控制 任性 转变 冲破 堤岸 四处 泛滥 洪水  可怕  可能 舆论 言论 自由 权宜 打老虎 后 动作 决定性 打破 国企 垄断 放开 管制 社会 逐渐 具备 自治 能力 根本"

//val sentence ="可能 位于 重庆市 沙坪坝 公园 内 的 红卫兵 >      墓园 鲜 有人 知 墓园 占地 面积 ５０ 余 亩 每 一块 墓碑 上都 镌刻 着 文革 战团 八一 五 派 死者 的 名字 墓园 里 埋葬 着 上百 名 红卫兵 和 造反派 他们 基本 上都 是 在 １９６７ 年 ５ 月 至 ８ 月 间 被  打 死 的 中学 红卫兵 和 重庆 厂矿 企 事业 的 工人 造反派 年纪 大 的 多 在 十几岁 或 二 三十 岁 最小 的 仅 １１ 岁"

//val sentence = "国务院 葛延风 的 医改 判断 在 宏观 问题 上 仍然 有 不少 分歧 所以 需要 通过 进一步 研究 凝聚 共识 比如 在 医疗 卫生 问题 方面 政府 和 市场 政府 和 社会 力量 到底 是 什么 关系 政府 在 管 和 办 的 问题 到底 怎么 协调 分歧 不少"

//val sentence ="党报 反党 北京 日报 爱国 精英 现在 有些 人 迷恋 普世 价值 世界 公民 忘了 自己 首先 是  中国人 这些 人 凡事 崇洋 媚外 动辄 挟 洋 自重 甚至 卖身 勾当 中共 党章 中国 共产党 以 马克思 列宁 主义 作为 自己 的 行动 指南 马列 排 在 毛邓三"

//    val sentence = "国家 社会 科学 基金 重大 项目 的 逻辑 矛盾 ００１３ 年度 国      家 社会 科学 基金 重大 项目 招标 课题 研究 方向 其中 有 西方 言论 自由 新闻 自由 的 虚伪 本质 研究 ｈｔｔｐ ｔ ｃｎ ｚｙｌ ８１９ ｈ － 这 就 奇怪 了 既然 还 没有 研究 怎么 就 知道 它 本质 虚伪 呢 >      既然 知道 它 本质 虚伪 了 为什么 还要 花 纳税人 的钱 研究"
//    val sentence ="有人问我三中全会和你有毛关系，那么强烈关注它干什么？我说三中全会与房价医疗就业官员对我们态度甚至菜价等等都密切相关，在这个大变革>      的大时代，每个人都是参与者，互联网时代，你已不是手无寸铁，每个人都有责任关注国家未来发展方向，要对自己负责，要对子孙负责。"
//val sentence ="有人 问 我 三 中 全会 和你 有 毛 关系 那么 强烈 关注 它 干什么 我 说 三       中 全会 与 房价 医疗 就业 官员 对 我们 态度 甚至 菜价 等等 都 密切 相关 在 这个 大 变革 的 大 时代 每个 人 都 是 参与者 互联网 时代 你 已 不是 手 无 寸 铁 每个 人 都 有 责任 关注 国家 未来 发展 方向 要 对 自己 负责 要 对 子孙 负责"
//val sentence =" 朝鲜 停战 协定 》 全称 《 朝鲜 人民 军 最高 司令官 及 中国 人民 志愿军 司令员 一 方 与 联合国 军 总司令 另  一 方 关于 朝鲜 军事 停战 的 协定 》 朝鲜 已 于 ０００９ 年 ５ 月 ０７ 日 宣布 退出 朝鲜 停战 协定 请教 此 约 朝 中方 是 不是 只 剩下 中国 人民 志愿军 司令员 一 方 啦"
//
//    val sentence ="沉船 南京 长江 大桥 和 他 娘 所有 文革 余孽 一样 长       一身 又 臭 又 硬 贱骨头 一 艘 万 吨 海轮 撞 上 非但 纹丝 不动 自己 还 撞 沉 了 我 一看 它 脑袋 上 竖 的 那 几 面 又 臭 又 硬 的 破 红旗 气 就 不 打 一 处 来 就 他 娘 一 座 文革 余孽 桥 这 真 他 娘       改革 开放 和 民主 自由 最 大 耻辱 难道 我们 沉船 派 真要 沉船 了 ｈｔｔｐ ｔ ｃｎ ｚｔｒｎｕ ３ｑ"
//
//    findTopK("左派",40,sameModel)
  }

}
