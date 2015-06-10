# Mammoth
Web-scale topic modelling for the [ClueWeb12](http://www.lemurproject.org/clueweb12.php/) dataset using [Spark](https://spark.apache.org/).

## Deployment

Compile and copy to the server:

    sbt compile assembly
    scp target/scala-2.10/Mammoth-assembly-0.1.jar username@server:~/Mammoth-assembly-0.1.jar

