package mainTestPackage

import org.apache.log4j.{Level, LogManager}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession

import scala.collection.mutable.HashMap

object MainTester {
  def main(args: Array[String]): Unit = {

    /*
      For Q2
      Tabs Analysis:
       -  tab0 -> As X -> GraduateStudent ->    Cost: 1379623
       -  tab1 -> As Y -> University  ->        Cost: 724685
       -  tab2 -> As Z -> Department  ->        Cost: 309815
       -  tab3 -> AS X, AS Z -> memberOf ->     Cost: 5580609
       -  tab4 -> AS Z, AS Y -> suborganOf->    Cost: 160140
       -  tab5 -> AS X, AS Y -> undergraDegree->Cost: 1619476
     */

    //    WHERE tab4.Z=tab2.Z AND tab1.Y=tab4.Y AND tab4.Y=tab5.Y AND tab3.Z=tab4.Z AND tab5.X=tab0.X AND tab0.X=tab3.X
    //    We essentially want to find the Hamiltonial path  https://www.hackerearth.com/practice/algorithms/graphs/hamiltonian-path/tutorial/

    //    When there are N nodes, there are N - 1 directed edges that can lead from it (going to every other node)
    //  https://stackoverflow.com/questions/5058406/what-is-the-maximum-number-of-edges-in-a-directed-graph-with-n-nodes?fbclid=IwAR0EePHHzvtL1b0XtckIbjjaXhGaYG7HEgyNDEd5EMF34dKdIv9Oiz_zYok
    //  PROOF: https://www.quora.com/How-do-I-prove-that-the-minimum-number-of-edges-in-a-connected-graph-with-n-vertices-is-n-1


    var costMap: HashMap[String, Double] = HashMap("tab0" -> 1379623, "tab1" -> 724685, "tab2" -> 309815, "tab3" -> 5580609, "tab4" -> 160140, "tab5" -> 1619476)

    var fullMap: HashMap[String, Array[String]] = HashMap("X"-> Array("tab0", "tab3", "tab5"), "Y"-> Array("tab1","tab4", "tab5"), "Z"->Array("tab2","tab3","tab4"))
    println(fullMap)

//    var fmap: Array[String, String, Double] = Array()

//    hmap.foreach{
//      k => println("st " + k._1)
//    }

    var bestComb : List[(String, Double)] = Nil // A list to preserve the best combinations

    //First iterate over the Hashmap (key,values). Values is a list that contains all the available tables between them
    for (dictIter <- fullMap){
      println("\n\nFor the key " + dictIter._1)

      var tempComb : List[(String, Double)] = Nil
      for(innerList <- dictIter._2.indices){  //  Iterate over the list elements. innerList is an index value
        println("Iterate over list elements " +dictIter._2(innerList))
        for(remElem <- innerList+1 until dictIter._2.length){   // Go from the next index to the end of the table to track all combinations
          println("Remaining elements " +dictIter._2(remElem))

          val tempProd = costMap(dictIter._2(innerList)) * costMap(dictIter._2(remElem))  // Find the cost of join between two arrays, by calculating the product
//          println("\n\nCost of element "+ dictIter._2(innerList) +" is "+costMap(dictIter._2(innerList)))
//          println("Cost of element "+ dictIter._2(remElem) +" is "+costMap(dictIter._2(remElem)))
          val tempTuple = (dictIter._2(innerList)+"."+dictIter._1+"="+dictIter._2(remElem)+"."+dictIter._1, tempProd)  // Create a temporary tuple with the table name and the cost
//          println("Temporary tuple", tempDlt)
          tempComb = tempTuple :: tempComb
        }
      }
      println("List with all the combinations \t"+ tempComb)
      val sortedtempComb = tempComb.sortBy(_._2)  //Sort everything by the join cost(ascending)
      println("List with SORTED the combinations \t"+ sortedtempComb)

      for(bestElem <- 0 until sortedtempComb.length - 1){ //Keep the N - 1 elements that we want
        println(sortedtempComb(bestElem))
        bestComb = sortedtempComb(bestElem) :: bestComb
      }

    }

    println("Final list with best combinations "+ bestComb)
//    val sortedBestComb = bestComb.sortBy(_._2)
    val sortedBestComb = bestComb.sortBy(_._2).map(x => x._1) //Sort once again to get the final ordering, and keep the only the combinations
    println("Final list with SORTED best combinations "+ sortedBestComb)
  }

}



