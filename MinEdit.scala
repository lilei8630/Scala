package timeSeries

import scala.collection.mutable.ArrayBuffer

/**
  * Created by extends_die on 16/1/10.
  */
object MinEdit {

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

  def similarity(str1:String,str2:String): Double ={

    val count:Int = findMinEdit(str1,str2)
//    println(count)
//    println(Math.max(str1.length,str2.length))

    count.toDouble/Math.max(str1.length,str2.length)
  }





  def main(args: Array[String]) {
    val str1 = "中国人民大学哪个专业好"
    val str2 = "中国人民大学学科排名"
    println(similarity(str1,str2))
  }

}
