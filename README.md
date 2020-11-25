***

<h1 align="center">Welcome to the COVID-19 Data Analysis application 👋</h1>
<p>
</p>

> COVID19 Data Analysis (CDA) is written in Java and utilizes Spark and Hadoop with a terminal-based interface. This system is used to track information about how COVID-19 will spread; its percentages of positive, negative and inconclusive results in regions, countries, etc; predicting trends with live data, and much more.

## Author

👤 **Dan Murphy**

* Website: https://www.linkedin.com/in/cs-dan-murphy/
* Github: [@pseudodan](https://github.com/pseudodan)

## Technologies

- Hadoop 3.2.1
- Apache Spark 3.0.1
- Java JDK
- IntelliJ

## Requirements

```bash
# Install homebrew
/bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/master/install.sh)"
```

```bash
# Mac Xcode development/commandline tools
xcode-select –install
# NOTE: if you are running MacOS Big Sur, run the 2 following commands.
sudo rm -rf /Library/Developer/CommandLineTools # Removes old cmd tools
sudo xcode-select --install # Installs updated tools for new MacOS release
```

```bash
# Installing prerequisites
brew cask install java
brew install scala
brew install apache-spark
brew install hadoop
```

## Configuration

### Ubuntu:

[Installing/Configuring Hadoop & Spark on Ubuntu](https://dev.to/awwsmm/installing-and-running-hadoop-and-spark-on-ubuntu-18-393h)

### MacOS: 

[Installing/Configuring Hadoop on a Mac](https://towardsdatascience.com/installing-hadoop-on-a-mac-ec01c67b003c)

```bash
# Environment Variables for Spark
# Add the following environment variables to your .bash_profile or .zshrc
export SPARK_HOME=/usr/local/Cellar/apache-spark/2.4.5/libexec 
export PATH="$SPARK_HOME/bin/:$PATH"
# If not sure how to do this, run the following command for either your .bash_profile or .zshrc
nano .bash_profile
nano .zshrc
# Then paste the environment variables above into either your .bash_profile or .zshrc
```

```bash
# Grant binaries executable permissions
chmod +x /usr/local/Cellar/apache-spark/2.4.5/libexec/bin/*
```

```bash
# Verification that spark is installed correctly
spark-shell
```

## Testing

You can simply use the ```compile.sh``` and ```stop.sh``` scripts in the root of the repo to start/run the project and stop the services once you are done. If you are to do this, please ensure the hdfs and apache-spark paths are correct.

If you would like a hands-on experience, you can follow the instructions below to start the services manually.

1. Start HDFS manually

``` bash
$ cd /opt/hadoop-3.2.1/sbin #ubuntu
or
$ cd /usr/local/Cellar/hadoop/3.3.0/sbin #mac
$ ./start-dfs.sh
$ jps # verify that the datanodes and namenodes were started
		Example Output:
		2705 Jps
		2246 NameNode
		2540 SecondaryNameNode
		2381 DataNode
```

2. Start Spark manually

```bash
$ cd /home/linuxbrew/.linuxbrew/Cellar/apache-spark/3.0.1/sbin #ubuntu
or
$ cd /usr/local/Cellar/apache-spark/3.0.1/libexec/sbin #mac
$ ./start-master.sh #spark://your_computer_name_here:7077 
$ ./start-slave.sh spark://your_computer_name_here:7077 #master is taken as an argument
```

3. Open the project in IntelliJ. 

   1. Ensure the .txt files in the root of the repo are in the /root dir

      **Note:** If you have issues compiling the project, open your terminal and navigate to the root project directory and compile it using maven.

      ``` bash
      $ cd ~/IdeaProjects/COVID19-Data-Analysis
      $ mvn compile
      $ mvn package
      ```

   2. File > Project Structure > Artifacts > navigate to the directory where the jar is located in the project dir

      ``` bash
      #For example:
      $ cd /home/your_username_here/IdeaProjects/COVID19-Data-Analysis/out/artifacts/<jar_file_here>
      ```

   3. File > Project Structure > Libraries > ..

      1. Classes

         ``` bash
         /home/linuxbrew/.linuxbrew/Cellar/apache-spark/3.0.1/libexec/jars #ubuntu
         or
         /usr/local/Cellar/apache-spark/3.0.1/libexec/jars #mac
         ```

      2. Sources

         ``` bash
         /home/linuxbrew/.linuxbrew/Cellar/apache-spark/3.0.1/libexec/jars #ubuntu
         or
         /usr/local/Cellar/apache-spark/3.0.1/libexec/jars #mac
         ```

   Note: this should also show the JAR path you will use in the build window.

4. Run the spark-submit script to run the project

``` bash
$ cd /usr/local/Cellar/apache-spark/3.0.1/bin #navigate to Spark's bin dir
$ ./spark-submit --class <Project Package Name>.SparkMainApp <JAR File of the project> --master <Spark URL you used to start the slave> 

Example input for reference:
$ ./spark-submit --class COVID19-Data-Analysis.SparkMainApp /home/user/IdeaProjects/COVID19-Data-Analysis/target/test-1.8-SNAPSHOT.jar --master Spark://user:7077
```

## Sources

[USA Dataset](https://healthdata.gov/sites/default/files/covid-19_diagnostic_lab_testing_20201122_2250.csv)

[Global Dataset](https://covid.ourworldindata.org/data/owid-covid-data.csv)

***



