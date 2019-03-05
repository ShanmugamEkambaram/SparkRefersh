object ShanSample extends App {
  override
  def main(args: Array[String]): Unit = {
    filterById("Super Shan")
  }

  def filterById(shan: String) {
    var a = new String();
    a = "HyperLoop"
    println("Shanmugam " + a + " " + shan)
    var b = Map(1 -> "Shanmugam", 2 -> "Spark", 3 -> "Shock")
    val ab = List(1, 2, 4, 56, 6)
    print("")
    println("List::")
    //Iterating a List
    for (lis <- ab) {
      print("")
      //Multiplying a List by 5
      println("List::" + lis * 5)
    }
    //Declaring a Tuples
    val s = ("Shan", 1, "ram")
    val sam = ("Shan", 1, "ram")
    //Iterating Tuples
    println("Tuples::" + sam._1 + sam._2 + sam._3)
    //Iterating Tuples Using foreach
    s.productIterator.foreach { i => println("Value::" + i) }
    print("")
    //Checking the Map for Containing specific Key
    if (b.contains(1)) {
      println("Keys:::" + b.keys)
    }
    println("keys :" + b.keys)
    println("keys :" + b.values)
    //Iterating a Map
    b.keys.foreach { i =>
      print("Keys::" + i)
      println("Values::" + b(i))
    }
  }
}
