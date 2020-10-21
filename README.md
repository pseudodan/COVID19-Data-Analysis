<h1 align="center">Welcome to the COVID-19 Data Analysis application 👋</h1>
<p>
</p>




> COVID Data Analysis (CDA) is written in PSQL with a Java-based interface. This system is used to track information about how COVID-19 will spread; its percentages of positive, negative and inconclusive results in regions, countries, etc; predicting trends with live data, and much more.

## Author

👤 **Dan Murphy**

* Website: https://www.linkedin.com/in/cs-dan-murphy/
* Github: [@pseudodan](https://github.com/pseudodan)

## Testing The Application

CDA requires [PSQL](https://www.postgresql.org/download/) and a [JRE](https://www.java.com/en/download/) to run.

Clone the project [here](https://github.com/pseudodan/Covid-Data-Analysis.git) and enter its respective directory.

Install the dependencies prior to running the application.

Next, we need to start and create a PSQL instance.

```sh
$ cd postgresql
$ source startPostgreSQL.sh
$ source createPostgreSQL.sh
```

Now that the server instance is started and created, proceed to enter the ```java/src``` dir

```sh
$ cd java/src
$ source compile.sh
```

The application will now be running in your terminal with a text-based menu. 



***
