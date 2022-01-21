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

    var hmap: HashMap[String, Array[String]] = HashMap("X"-> Array("tab0", "tab3", "tab5"), "Y"-> Array("tab1","tab4", "tab5"), "Z"->Array("tab2","tab3","tab4"))
    println(hmap)

    costMap.foreach
    {
      case (key, value) => println (key + " -> " + value)
    }

  }

}



