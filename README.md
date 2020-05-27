
# Dicom Data Source for Apache Spark

A library for reading the dicom images as a `spark SQL` data frame.

## Linking 
This library is cross-published for `scala 2.11` and `java 1.8.0_221`.The third party library named
`dcm-4che` source code was also used for developing the library.

This package doesn't have any releases published in the Spark Packages repo yet, or with maven coordinates supplied.
You may have to build this package from source using `maven package` command and optionally it can also be installed 
in the local maven repository using `maven install:install-file` command.

The built JAR can be also be used as external dependency by adding it to the class-path.
 
## Features

Dicom Data Source for Spark supports reading the dicom images in the `Spark SQL` data frame.It also returns the seperate 
data frame containing the details regarding the corrupt files.

`Schema`  of the data frame containing the dicom data is as follows:

It contains three columns named `origin`,`metadata` and `pixeldata`.

1. `origin` contains the file path of dicom file.
2. `metadata` contains the patient metadata.
3. `pixeldata` contains the pixel data as array of bytes.

`Schema` of the data frame containing the corrupt data is as follows:

It contains two columns named `origin`,`exception`.

1. `origin` contains the file path of the dicom file.
2. `exception` conatins the message displaying the exception occured while reading the dicom file.


## Code Snippets 


#### Scala API.

```scala

//import the package

import dicomread
import java.io.{ByteArrayInputStream,File}
import javax.imageio.ImageIO

//read the dicom files from hadoop file system or local filesystem
//dcmdf is the data frame containing the dicom files data.
//cdf is the corrupt data frame containing the information about corrupt file.
val (dcmdf,cdf) = dicomread.DicomSchema.readDicom(path,sparksession,numpartitions)

//retrieve a particular row froma a data frame.
val row = dcmdf.rdd.take(1).last

//retrieve metadata which is stored as json string in a dataframe.
//json string can be converted to the json object and can be further processed using appropriate library.
val jsonString = row(1).asInstanceOf[String]

//retrieve the pixeldata and convert it into the jpg format or some other format.
val imgByteArray = row(2).asInstanceOf[Array[Byte]]
val bis = new ByteArrayInputStream(imgByteArray)
val bimage = ImageIO.read(bis)

val width = bimage.getWidth
val height = bimage.getHeight

ImageIO.write(bimage,"jpg",new File("abc.jpg"))

```







