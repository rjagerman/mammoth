package ch.ethz.inf.da.mammoth.io

import java.io.FileOutputStream
import org.apache.spark.mllib.feature.DictionaryTF

/**
 * Input/Output for Dictionary
 *
 * Uses pickling for serialization/deserialization
 */
object DictionaryIO {

  /**
    * Reads a dictionary from given file
    *
    * @param file The file
    * @param n The maximum size of the dictionary
    * @return A DictionaryTF object
    */
  def read(file:String, n:Int = 10000000): DictionaryTF = {
    val mapping:Map[Any, Int] = scala.io.Source.fromFile(file).getLines().take(n).zipWithIndex.toMap
    new DictionaryTF(mapping, mapping.size)
  }

  /**
    * Writes given dictionary to given file
    *
    * @param file The file
    * @param dictionary The DictionaryTF object
    * @return The DictionaryTF object
    */
  def write(file:String, dictionary: DictionaryTF): DictionaryTF = {
    val items = dictionary.mapping.map(x => x.swap)
    val output = new FileOutputStream(file)
    (0 until items.size).foreach { x => output.write((items(x) + "\n").getBytes) }
    output.close()
    dictionary
  }

}
