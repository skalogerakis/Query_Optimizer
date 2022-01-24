package mainTestPackage

import org.apache.commons.lang.StringUtils
import org.apache.log4j.{Level, LogManager}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession

import scala.collection.mutable.HashMap
import scala.util.control.Breaks._

object MainTester {

  def QueryOptimizer() : Unit = {


    val query = "X SELECT tab5.Y AS Y,tab2.Z AS Z,tab3.X AS X \nFROM \n(SELECT s AS Y FROM table00001__3_E__http___www_w3_org_1999_02_22_rdf_syntax_ns_type_ WHERE o == '<http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#University>') AS tab1, \n(SELECT s AS Z, o AS Y FROM table00001__3_E__http___www_lehigh_edu__zhp2_2004_0401_univ_bench_owl_suborganizationof_) AS tab4, \n(SELECT s AS X, o AS Y FROM table00001__3_E__http___www_lehigh_edu__zhp2_2004_0401_univ_bench_owl_undergraduatedegreefrom_) AS tab5, \n(SELECT s AS X FROM table00002__3_E__http___www_w3_org_1999_02_22_rdf_syntax_ns_type_ WHERE o == '<http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#GraduateStudent>') AS tab0, \n(SELECT s AS X, o AS Z FROM table00004__3_E__http___www_lehigh_edu__zhp2_2004_0401_univ_bench_owl_memberof_) AS tab3, \n(SELECT s AS Z FROM table00004__3_E__http___www_w3_org_1999_02_22_rdf_syntax_ns_type_ WHERE o == '<http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#Department>') AS tab2 \nWHERE tab1.Y=tab4.Y AND tab4.Y=tab5.Y AND tab0.X=tab3.X AND tab3.X=tab5.X AND tab2.Z=tab3.Z AND tab3.Z=tab4.Z"
//    val query  = "X SELECT tab4.X AS X,tab2.Y1 AS Y1,tab3.Y2 AS Y2,tab4.Y3 AS Y3 \nFROM \n(SELECT s AS X FROM table00003__3_E__http___www_w3_org_1999_02_22_rdf_syntax_ns_type_ WHERE o == '<http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#FullProfessor>') AS tab0, \n(SELECT s AS X FROM table00003__3_E__http___www_lehigh_edu__zhp2_2004_0401_univ_bench_owl_worksfor_ WHERE o == '<http://www.Department0.University0.edu>') AS tab1, \n(SELECT s AS X, o AS Y1 FROM table00003__3_E__http___www_lehigh_edu__zhp2_2004_0401_univ_bench_owl_name_) AS tab2, \n(SELECT s AS X, o AS Y2 FROM table00003__3_E__http___www_lehigh_edu__zhp2_2004_0401_univ_bench_owl_emailaddress_) AS tab3, \n(SELECT s AS X, o AS Y3 FROM table00003__3_E__http___www_lehigh_edu__zhp2_2004_0401_univ_bench_owl_telephone_) AS tab4 \nWHERE tab0.X=tab1.X AND tab1.X=tab2.X AND tab2.X=tab3.X AND tab3.X=tab4.X "
    println("Initial Query is \n"+query+"\n\n" )

    //Extracts contents of the query from the first FROM statement until the last WHERE STATEMENT
    var initQueryDiv : String = StringUtils.substringAfter(query, "FROM")
    initQueryDiv = StringUtils.substringBeforeLast(initQueryDiv, "WHERE").replaceAll("\n", " ").trim
    println("\n\nAfter clearing "+ initQueryDiv+"\n\n")

    //Regex to split string by commas that is not included inside the parentheses
    val tabSplitter: Array[String] = initQueryDiv.split(",\\s*(?![^()]*\\))")
    tabSplitter.foreach(println)

    // Structure that contains an Array[(tab name, AS statements ,whole from statatement)]
    val tableIdentifier: Array[(String, String, String)] = tabSplitter.map(x=>(StringUtils.substringAfterLast(x, "AS").trim,  StringUtils.substringBetween(x, "SELECT", "FROM"), x))
    tableIdentifier.foreach(println)

    val rgxAS = "AS\\s((\\w+))|as\\s(\\w+)".r //Will extract statements that start with AS or as
    var fullMap: HashMap[String, Array[String]] = new HashMap() //A hashmap with keys the statements stored in AS statemets and values all the possible tabs for join
    tableIdentifier.foreach( x => {

      rgxAS.findAllIn(x._2).matchData.foreach(

        m => fullMap.get(m.group(1)) match {
          case None => fullMap.update(m.group(1), Array(x._1))  //In case the element does not exist initialize the array
          case Some(value) => fullMap.update(m.group(1), value :+ x._1) //Otherwise append to the list the new element
        }

      )
    })

//    for (dictIter <- fullMap){
//      println("\n\nFor the key " + dictIter._1)
//      for (iter <- dictIter._2){
//        println("Val "+ iter)
//      }
//    }

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

//    Q2 data
    var costMap: HashMap[String, Double] = HashMap("tab0" -> 1379623, "tab1" -> 724685, "tab2" -> 309815, "tab3" -> 5580609, "tab4" -> 160140, "tab5" -> 1619476)
//
//    var fullMap: HashMap[String, Array[String]] = HashMap("X"-> Array("tab0", "tab3", "tab5"), "Y"-> Array("tab1","tab4", "tab5"), "Z"->Array("tab2","tab3","tab4"))

//    Q4 Data
//    var costMap: HashMap[String, Double] = HashMap("tab0" -> 1379623, "tab1" -> 724685, "tab2" -> 309815, "tab3" -> 5580609, "tab4" -> 160140)
//
//    var fullMap: HashMap[String, Array[String]] = HashMap("X"-> Array("tab0", "tab1", "tab2", "tab3", "tab4"), "Y1" -> Array("tab2"), "Y2" -> Array("tab3"), "Y3" -> Array("tab4"))


    var bestComb : List[(String, Double)] = Nil // A list to preserve the best combinations

    //First iterate over the Hashmap (key,values). Values is a list that contains all the available tables between them
    //We are certain that there are no joins where only one table of a specific key exists
    for (dictIter <- fullMap; if dictIter._2.length > 1 ){
      println("\n\nFor the key " + dictIter._1)

      var tempComb : List[(String, Double)] = Nil
      for(innerList <- dictIter._2.indices){  //  Iterate over the list elements. innerList is an index value
//        println("Iterate over list elements " +dictIter._2(innerList))
        for(remElem <- innerList+1 until dictIter._2.length){   // Go from the next index to the end of the table to track all combinations
//          println("Remaining elements " +dictIter._2(remElem))

          val tempProd: Double = costMap(dictIter._2(innerList)) * costMap(dictIter._2(remElem))  // Find the cost of join between two arrays, by calculating the product
//          println("\n\nCost of element "+ dictIter._2(innerList) +" is "+costMap(dictIter._2(innerList)))
//          println("Cost of element "+ dictIter._2(remElem) +" is "+costMap(dictIter._2(remElem)))
          val tempTuple: (String, Double) = (dictIter._2(innerList)+"."+dictIter._1+"="+dictIter._2(remElem)+"."+dictIter._1, tempProd)  // Create a temporary tuple with the table name and the cost
//          println("Temporary tuple", tempTuple)
          tempComb = tempTuple :: tempComb
        }
      }

      println("List with all the combinations \t"+ tempComb)
      val sortedtempComb : List[(String, Double)]= tempComb.sortBy(_._2)  //Sort everything by the join cost(ascending)
      //      println("List with SORTED the combinations \t"+ sortedtempComb)

      for(bestElem <- 0 until dictIter._2.length - 1){ //Keep the N - 1 elements that we want from each case. NOTE N-1 from the initial list with the elements
        //        println(sortedtempComb(bestElem))
        bestComb = sortedtempComb(bestElem) :: bestComb
      }

    }

    //    println("Final list with best combinations "+ bestComb)
    //    val sortedBestComb = bestComb.sortBy(_._2)
    val sortedBestComb : List[String] = bestComb.sortBy(_._2).map(x => x._1) //Sort once again to get the final ordering, and keep the only the combinations
    println("Final list with SORTED best combinations "+ sortedBestComb)
  }

  def main(args: Array[String]): Unit = {

    QueryOptimizer()
  }



}



