package timeSeries

import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.Map

/**
  * Created by extends_die on 16/1/28.
  */
object AHC {

  def findMinEdit(str1:String,str2:String): Int ={
    var ret:Int = -1
    val n:Int = str1.length
    val m:Int = str2.length
    //如果str1长度为0,最小编辑距离为str2的长度
    //如果str2长度为0,最小编辑距离为str1的长度
    if(n==0) ret = m
    if(m==0) ret = n
    val rawA = new ArrayBuffer[Int](n)
    val rawB = new ArrayBuffer[Int](n)
    //初始化rawA,rawB
    for(i <- 0 until n)
      rawB+=(i+1)
    for(i <- 0 until n)
      rawA+=(i+1)
    var even:Boolean = true

    for (i <- 0 until m){
      val ch1:Char = str2.charAt(i)
      //println(ch1)
      even = i%2==0
      for (j <- 0 until n){
        val ch2:Char = str1.charAt(j)
        //println(ch2)
        val temp:Int = if(ch1==ch2) 0 else 1
        //若为偶数行,RawB为历史记录,RawA被更新
        if(even){
          rawA.update(j, List((if(j==0) i+1 else rawA(j-1))+1,rawB(j)+1,(if(j==0) i else rawB(j-1))+temp).min)
        }
        //若为奇数行,RawA为历史记录,RawB被更新
        else {
          rawB.update(j, List((if (j == 0) i + 1 else rawA(j - 1)) + 1, rawA(j) + 1, (if (j == 0) i else rawA(j - 1)) + temp).min)
        }
      }
    }
    ret = if(even) rawA(n-1) else rawB(n-1)
    ret
  }

  /*最小编辑距离越小,相似度越高*/
  def similarity(str1:String,str2:String): Double ={

    val count:Int = findMinEdit(str1,str2)
    //    println(count)
    //    println(Math.max(str1.length,str2.length))

    count.toDouble/Math.max(str1.length,str2.length)
  }

  def main(args: Array[String]) {

    val querysMap = List("中国人民大学那个专业好","中国人民大学哪个专业好","人大校长是谁","深圳楼价上涨50%","深圳楼市","楼市调控").zipWithIndex.map(x=>(x._2,x._1)).toMap

    val querys = List("中国人民大学那个专业好","中国人民大学哪个专业好","人大校长是谁","深圳楼价上涨50%","深圳楼市","楼市调控").zipWithIndex

    //声明group为一个空的可变Map,key为query索引,value为组号
    val groups = new scala.collection.mutable.HashMap[Int, Int]
    //为group初始化
    querys.map(x=>(x._2,x._2)).foreach(x=>{groups+=x})

    val simMap = collection.mutable.Map[String, Double]()
    querys.foreach(x=>{
      val query1 = x._1
      val index1 = x._2
      querys.foreach(y=>{
        val query2 = y._1
        val index2 = y._2
        if(index1 < index2){
          val sim = similarity(query1,query2)
          simMap += (index1+"#"+index2 -> sim)
        }})})
    var sortedList = simMap.toList.sortBy(_._2)

    //下面应该有一个while循环
    var sim:Double = 1.0
    var pair:String = ""
    var index1 = -1 ;
    var index2 = -1 ;
    var group1 = -1 ;
    var group2 = -1 ;

    while(!sortedList.isEmpty&&sortedList.head._2<0.9){
      index1 = sortedList.head._1.split("#")(0).toInt
      index2 = sortedList.head._1.split("#")(1).toInt
      sortedList = sortedList.drop(1)
      //下面需要合并了,index1和index2之前是哪个组的?
      group1 = groups(index1)
      group2 = groups(index2)
      if(group1!=group2){
        for((index,group) <- groups){
          if(group==group2){
            groups(index)=group1
          }
        }
      }
    }

    for((index,group)<-groups){
      println(querysMap(index)+" "+group)
    }
  }

}
