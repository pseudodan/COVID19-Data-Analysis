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

``` bash
# Installing prerequisites on Ubuntu
sudo apt install openjdk-8-jdk -y
sudo apt install openssh-server openssh-client -y
ssh-keygen -t rsa -P '' -f ~/.ssh/id_rsa
cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys
ssh localhost
wget https://downloads.apache.org/hadoop/common/hadoop-3.2.1/hadoop-3.2.1.tar.gz
tar xzf hadoop-3.2.1.tar.gz
which javac # provides the path to the Java binary dir
readlink -f /usr/bin/javac # linked and assigned to $JAVA_HOME
sudo apt install maven
```

```bash
# Installing prerequisites on Mac
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
# Environment Variables for Hadoop & Spark
# Add the following environment variables to your .bash_profile or .zshrc
# ----------------------------------------------------
# Note: If not sure how to do this, run the following command for either your .bash_profile or .zshrc
sudo nano .bash_profile # or -> sudo nano .zshrc
# ----------------------------------------------------
# >>> .bashrc file below <<<
#Hadoop Related Options
export HADOOP_HOME=/home/lianli/hadoop-3.2.1
export HADOOP_INSTALL=$HADOOP_HOME
export HADOOP_MAPRED_HOME=$HADOOP_HOME
export HADOOP_COMMON_HOME=$HADOOP_HOME
export HADOOP_HDFS_HOME=$HADOOP_HOME
export YARN_HOME=$HADOOP_HOME
export HADOOP_COMMON_LIB_NATIVE_DIR=$HADOOP_HOME/lib/native
export PATH=$PATH:$HADOOP_HOME/sbin:$HADOOP_HOME/bin
export HADOOP_OPTS="-Djava.library.path=$HADOOP_HOME/lib/native"

#Apache Spark
export SPARK_HOME=/home/linuxbrew/.linuxbrew/Cellar/apache-spark/3.0.1/libexec
export PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin

# >>> end of .bashrc file <<<
# ----------------------------------------------------
# Then paste the environment variables above into either your .bash_profile or .zshrc
# Please ensure you run the following command to apply your changes
source ~/.bashrc # or -> source ~/.zshrc
```

```bash
# Grant binaries executable permissions
chmod +x /home/linuxbrew/.linuxbrew/Cellar/apache-spark/3.0.1/libexec/bin/*
```

```bash
# Verification that spark is installed correctly
spark-shell # runs an instance of spark using scala
```

## Testing

You can simply use the ```start.sh``` , ```run.sh``` and ```stop.sh``` scripts in the root of the repo to start/run the project and stop the services once you are done. If you are to do this, please ensure the hdfs and apache-spark paths are correct.

``` bash
source start.sh #starts Hadoop and Spark services
source run.sh #compiles & packages project using maven, executes the project -> terminal menu
source stop.sh #stops all services
```

**Note: In the ``` start.sh``` script, if you are experiencing that Apache Spark cannot start it's workers, you may need to manually modify the script by replacing ```$hostname``` with the name of your computer.** 

If you would like a hands-on experience, you can follow the instructions below to start the services manually.

1. Start HDFS manually

``` bash
$ cd ~/hadoop-3.2.1/sbin #ubuntu
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

2. Create an HDFS directory and put the .csv files in that directory. The .csv files are [here](##Sources) or scroll to the bottom of the README

   ``` bash
   hdfs dfs -mkdir /COVID19
   hdfs dfs -put ~/Downloads/USA.csv /COVID19
   hdfs dfs -put ~/Downloads/Global.csv /COVID19
   ```

3. Start Spark manually

```bash
$ cd /home/linuxbrew/.linuxbrew/Cellar/apache-spark/3.0.1/libexec/sbin #ubuntu
or
$ cd /usr/local/Cellar/apache-spark/3.0.1/libexec/sbin #mac
$ ./start-master.sh #spark://$hostname:7077 
$ ./start-slave.sh spark://$hostname:7077 #master is taken as an argument
```

4. Open the project in IntelliJ. 
   1. Ensure the .txt files in the root of the repo are in the /root dir

   **Note:** If you have issues compiling the project, open your terminal and navigate to the root project directory and compile it using maven.

   ``` bash
   $ cd ~/IdeaProjects/COVID19-Data-Analysis # or ~/Documents/Github/.. if cloned there
   $ mvn compile
   $ mvn package
   ```

   2. File > Project Structure > Artifacts > navigate to the directory where the jar is located in the project dir

   ``` bash
   #For example:
   $ cd /home/your_username_here/IdeaProjects/COVID19-Data-Analysis/out/artifacts/<jar_file_here>
   ```

   3. File > Project Structure > Libraries > ..

   4. Classes

   ``` bash
   /home/linuxbrew/.linuxbrew/Cellar/apache-spark/3.0.1/libexec/jars #ubuntu
   or
   /usr/local/Cellar/apache-spark/3.0.1/libexec/jars #mac
   ```

   5. Sources

   ``` bash
   /home/linuxbrew/.linuxbrew/Cellar/apache-spark/3.0.1/libexec/jars #ubuntu
   or
   /usr/local/Cellar/apache-spark/3.0.1/libexec/jars #mac
   ```

Note: this should also show the JAR path you will use in the build window.

5. Run the spark-submit script to run the project

``` bash
$ cd /usr/local/Cellar/apache-spark/3.0.1/bin #navigate to Spark's bin dir
$ ./spark-submit --class <Project Package Name>.SparkMainApp <JAR File of the project> --master <Spark URL you used to start the slave> 

Example input for reference:
$ ./spark-submit --class COVID19-Data-Analysis.SparkMainApp /home/user/IdeaProjects/COVID19-Data-Analysis/target/test-1.8-SNAPSHOT.jar --master Spark://user:7077
```

## Functionality

### USA

- [x] Number of Case Outcomes by State
- [x] Number of Total Tests Administered
- [x] Number of Specified Tests by Date Range
- [x] Total Results Reported Filtered by State and Quarter of the Year
- [x] Top 'K' Results Reported by State
- [x] Total Number of Cases by Date Range
- [x] Total Number of New Cases by Date Range
- [x] List Total Quarterly New Results Reported by Case Outcome
- [x] List Total Quarterly Case Outcomes by State
- [x] COVID-19 Recent Statistics

### Global

- [x] Number of Tests Administered by Country
- [x] Number of Tests Administered by Continent
- [x] Number of Total Cases by Country
- [x] Number of Total Cases by Continent
- [x] Number of Total Cases Globally
- [x] Average Life Expectancy Once Tested Positive
- [x] Average Amount of New Cases by Country
- [x] Most Recent Deaths by Country
- [x] Top 'K' Countries by Total Cases on a Specific Date
- [x] Top 'K' Countries by Total Deaths on a Specific Date
- [x] Top 'K' Hospitalized Patients In Europe Based On Total Cases
- [x] Top 'K' ICU Patients In Europe Based on Total Cases
- [x] Total Number of Positive Cases Per Month
- [x] Which Month Saw the Greatest Number of Cases?
- [x] Total Cases by Country Per Quarter
- [x] Total Cases by Continent Per Quarter
- [ ] Predict Number of Total Cases for the Following Month
- [ ] Predict Number of New Cases by Country for the Following Month
- [ ] Predict Number of New Cases by Continent for the Following Month

## Sources

[USA.csv](https://healthdata.gov/sites/default/files/covid-19_diagnostic_lab_testing_20201122_2250.csv)

[Global.csv](https://covid.ourworldindata.org/data/owid-covid-data.csv)

***



