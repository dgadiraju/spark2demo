# Spark 2 Demo
As this project is for learning purposes, we have created separate class/object for each use case with main function.

Here are the details about using this project.
* Clone the repository.
* Open with IntelliJ or Eclipse with Scala and sbt plugin.
* Go to the respective program and identify the arguments.
* Validate either using IDE or by using local spark or based up on the instructions provided.

## Setup Instructions
Let us understand how to setup the project.
 * Clone [GitHub Repository](https://github.com/dgadiraju/spark2demo).
```shell script
git clone https://github.com/dgadiraju/spark2demo.git
```
 * It will create a folder by name `spark2demo`
 * We are externalizing properties such as input directory, output directory etc.
 * Before running any program or building jar file, make sure to go to `src/main/resources/application.properties` and do the following:
   * Review and modify the properties as per your paths.
   * Make sure the paths defined in `application.properties` are correct.
   * Make sure file formats used in the programs and files in the defined paths are consistent.
 * Go to `spark2demo` and run `sbt package`
 * It will build the jar file in `target/scala-2.11` folder.
 
## Running Programs

We will be having several programs as part of this repository. 
* Each one of it will have a main function.
* We will see how each program can run locally or on [ITVersity labs](https://labs.itversity.com) or on EMR Cluster.

### GetDailyProductRevenue

Let us understand how to run `GetDailyProductRevenue` using different options.

* Validate using IDE
* Run using Local Spark
* Run on [ITVersity labs](https://labs.itversity.com)
* Run on EMR Cluster.

#### Validate using IDE
Here is how you can validate the program locally.

* Go to the program you want to run - `GetDailyProductRevenue`
* Right click and then click on `Run 'GetDailyProductRevenue'`
* Program might fail for the first time as we have to pass arguments to it.
* If it fails - go to `Run -> Edit Configurations` and pass program arguments.
* This program takes only one argument and we need to pass `dev` to validate locally.
* If successful the program will exit with exit code 0.
* Make sure to validate by going to the output directory.

#### Run using Local Spark
Let us ensure that jar file is built so that we can submit the job using Spark that is setup locally using `local` mode.

* Go to working directory and run `sbt package`
* It will create jar file in this location - `target/scala-2.11/spark2demo_2.11-0.1.jar`
* As the application is dependent on 3rd party plugin called as `typesafe config`, we need to pass the jar file while running job using `spark-submit`.
* We can pass it either by using `--packages com.typesafe:config:1.3.2` or by using `--jars ~/.ivy2/jars/com.typesafe_config-1.3.2.jar`.
* `.ivy2` is the directory that is created as part of setting up sbt based application. It is used to cache the jar files that are required for sbt based applications.
* `spark-submit` command looks like this to run the spark application using local spark in local mode.
```shell script
spark-submit \
  --class retail_db.GetDailyProductRevenue \
  --packages com.typesafe:config:1.3.2 \
  target/scala-2.11/spark2demo_2.11-0.1.jar dev
```