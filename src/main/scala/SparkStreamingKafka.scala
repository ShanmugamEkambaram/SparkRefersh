import scala.collection.mutable.HashMap

/**object SparkStreamingKafka {

  def main(args: Array[String]): Unit = {

    isIsomorphic(s,t)


  }
  def isIsomorphic(s : String , t : String): Boolean = {
    var map:HashMap[Char,Char]=HashMap()
    var map1:HashMap[Char,Char]=HashMap()
    if(s.length()!=t.length()){
      print("Both are equal";
      return false
    }
    for(i<-s.length())
    {
      var e=s.charAt(i)
      var f=t.charAt(i)
      if(map.contains(e)){
        if(f!=map.get(f)){
          return false
        }
      }else{
        if(map1.contains(f)){
        return false
      }
        map.put(e,f)
        map1.put(f,e)
      }
    }
    return true
  }
}
**/