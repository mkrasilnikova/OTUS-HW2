package example

import org.apache.hadoop.conf._
import org.apache.hadoop.fs._

import java.net.URI

object Main extends App {

  val conf = new Configuration()
  val fileSystem = FileSystem.get(new URI("hdfs://localhost:9000"), conf)

  val sourcePath = new Path("/stage")
  val destPathRoot = new Path("/ods")

  try {
    createFolder(destPathRoot)
    fileSystem.listStatus(sourcePath).foreach(fs => {
      val dirPath = fs.getPath
      println("folder " + dirPath)

      val fileStatuses = fileSystem.listStatus(dirPath)
        .filter(fs => fs.getPath.getName.contains("csv"))

      println("files: ")
      fileStatuses
        .foreach(fs => println(fs.getPath.getName))

      val concatFilePath = new Path(dirPath, fileStatuses(0).getPath.getName)
      println("concatenating files in the folder ... resulting file \n" + concatFilePath)

      val sourcePaths: Array[Path] = fileStatuses.map(fs => fs.getPath).tail
      if (sourcePaths.size > 0) {
        fileSystem.concat(concatFilePath, sourcePaths)
      }

      val destDir = new Path(destPathRoot, fs.getPath.getName)
      println("create folder " + destDir)
      createFolder(destDir)

      val destFilePath = new Path(destDir, fileStatuses(0).getPath.getName)
      println("tranfer file into new directory " + destFilePath)
      fileSystem.rename(concatFilePath, destFilePath)

      println("delete old folder " + dirPath)
      fileSystem.delete(dirPath, true)

      println("_________________________")
    })
  } catch {
    case e: Exception => println(e.getMessage)
  } finally {
    fileSystem.close()
  }


  def createFolder(path: Path): Unit = {
    if (!fileSystem.exists(path)) {
      fileSystem.mkdirs(path)
    }
  }
}


