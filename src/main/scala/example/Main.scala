package example

import org.apache.hadoop.conf._
import org.apache.hadoop.fs._

import java.net.URI

object Main extends App {

  val conf = new Configuration()
  val fileSystem = FileSystem.get(new URI("hdfs://localhost:9000"), conf)

  val sourcePath = new Path("/stage")
  val destPathRoot = new Path("/ods")

  createFolder(destPathRoot)
  fileSystem.listStatus(sourcePath).foreach(fs => {
      val dirPath = fs.getPath
      val fileStatuses = fileSystem.listStatus(dirPath)
        .filter(fs => fs.getPath.getName.contains("csv"))
      val concatFilePath = new Path(dirPath, fileStatuses(0).getPath.getName)
      val sourcePaths: Array[Path] = fileStatuses.map(fs => fs.getPath).tail
      if (sourcePaths.size > 0) {
        fileSystem.concat(concatFilePath, sourcePaths)
      }
      val destDir = new Path(destPathRoot, fs.getPath.getName)
      createFolder(destDir)
      val destFilePath = new Path(destDir, fileStatuses(0).getPath.getName)
      val renamed = fileSystem.rename(concatFilePath, destFilePath)
      fileSystem.delete(dirPath, true)
    })


  def createFolder(path: Path): Unit = {
    if (!fileSystem.exists(path)) {
      fileSystem.mkdirs(path)
    }
  }
}


