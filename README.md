# Spark Practice

In this repo, I try to use Spark (PySpark) to look into a downloading log file in `.CSV` format. This repo can be considered as an introduction to the very basic functions of Spark. It may be helpful for those who are beginners to Spark.

Please note:
 - Hadoop knowledge will not be covered in this practice.
 - I used single-node mode here. Cluster deployment will not be discussed. 
 - If you're also interested in using Scala for Spark rather than Python, you can refer to my another GitHub repo [spark-practice-scala](https://github.com/XD-DENG/spark-practice-scala).

Additionally, we're using a real log file as sample data in this tutorial and trying to cover some operations commonly used in daily works. If you would like to get to know more operations with minimal sample data, you can refer to a seperate script I prepared, [*Basic Operations in PySpark*](https://github.com/XD-DENG/Spark-practice/blob/master/others/basic_operations.py).

- [1. Preparation](#1-preparation)
- [2. Sample Data](#2-sample-data)
- [3. How We Use Spark (PySpark) Interactively](#3-how-we-use-spark-pyspark-interactively)
  - [Start PySpark](#start-pyspark)
  - [Load Data](#load-data)
  - [Show the Head](#show-the-head-first-n-rows)
  - [Transformation (map & flatMap)](#transformation-map--flatmap)
  - [Reduce and Counting](#reduce-and-counting)
  - [Sorting](#sorting)
  - [Filter](#filter)
  - [Collect Result ('Export' into Python)](#collect-result-export-into-python)
  - [Set Operation](#set-operation)
  - [Join](#join)
  - [Persisting (Caching)](#persisting-caching)
- [4. Submitting Application](#4-submitting-application)
- [5. Spark SQL and DataFrames](#5-spark-sql-and-dataframes)
- [References](#references)
- [License](#license)


## 1. Preparation

The environment I worked on is an Ubuntu machine. It's quite simple to install Spark on Ubuntu platform. 

Firstly, ensure that JAVA is install properly. If not, we can install by 
```bash
$  sudo apt-get install openjdk-8-jdk
```

Then we can download the latest version of Spark from http://spark.apache.org/downloads.html and unzip it. Then we can simply test if Spark runs properly by running the command below in the Spark directory

```bash
$  ./bin/pyspark
```
or
```bash
$ ./bin/spark-shell
```





## 2. Sample Data
The sample data we use is from http://cran-logs.rstudio.com/. It is the full downloads log of R packages from Rstudio's CRAN mirror on December 12 2015 (you can get the data in the `sample_data` folder of this repository). 

![\[pic link\]](https://github.com/XD-DENG/Spark-practice/blob/master/sample_data/data_screenshot.png?raw=true)

We will try to use Spark to do some simple analytics on this data.




## 3. How We Use Spark (PySpark) Interactively

### Start PySpark 

We can directly call `pyspark` to start Spark
```bash
$  ./bin/pyspark
```
Instead, we can also use **IPython**. It can bring some convenient features like auto-completion.
```bash
$  PYSPARK_DRIVER_PYTHON=ipython ./bin/pyspark
```

If you have **Jupyter** installed, you can also choose to run PySpark in *Jupyter Notebook* or *Jupyter QtConsole*.

```bash
$  PYSPARK_DRIVER_PYTHON=jupyter PYSPARK_DRIVER_PYTHON_OPTS=notebook ./bin/pyspark
$  PYSPARK_DRIVER_PYTHON=jupyter PYSPARK_DRIVER_PYTHON_OPTS=qtconsole ./bin/pyspark
```

If you want to specify the Python version you prefer, you can use `PYSPARK_PYTHON`, 

```bash
$  PYSPARK_PYTHON=python3.6 ./bin/pyspark
```

After Spark is started, a default SparkContext will be created (usually named as "sc").


### Load Data

The most common method used to load data is `sc.textFile`. This method takes an URI for the file (local file or other URI like `hdfs://`), and will read the data as a collections of lines. 

```python
# Load the data
>>> raw_content = sc.textFile("2015-12-12.csv")

# Print the type of the object
>>> type(raw_content)
<class 'pyspark.rdd.RDD'>

# Print the number of lines
>>> raw_content.count()
421970
```

You may want to note that all Spark’s file-based input methods, including `textFile`, support running on directories, compressed files, and wildcards as well [1]. For example, you can use `textFile("/my/directory")`, `textFile("/my/directory/*.txt")`, or `textFile("/my/directory/*.gz")`. In our case, the two commands below will help load exactly the same data.

```python
>>> a = sc.textFile("2015-12-12.csv")
>>> b = sc.textFile("2015-12-12.csv.gz")
>>> a.count()
421970
>>> b.count()
421970
```

This feature makes things much simpler when we have multiple text data files to load. By giving the directory under where these files are, we can load many data files with only one line. Additionally, we can also use wildcards to specify the file types we would like to load, like with `textFile("/my/directory/*.txt")`, we will only load those files with `.txt` file type in the directory we specified.


### Show the Head (First `n` rows)
We can use `take` method to return first `n` rows.

```python
>>> raw_content.take(5)
[u'"date","time","size","r_version","r_arch","r_os","package","version","country","ip_id"',
 u'"2015-12-12","13:42:10",257886,"3.2.2","i386","mingw32","HistData","0.7-6","CZ",1',
 u'"2015-12-12","13:24:37",1236751,"3.2.2","x86_64","mingw32","RJSONIO","1.3-0","DE",2',
 u'"2015-12-12","13:42:35",2077876,"3.2.2","i386","mingw32","UsingR","2.0-5","CZ",1',
 u'"2015-12-12","13:42:01",266724,"3.2.2","i386","mingw32","gridExtra","2.0.0","CZ",1']
```

We can also take samples randomly with `takeSample` method. With `takeSample` method, we can give three arguments and need to give at least two of them. They are `if replacement`, `number of samples`, and `seed` (optional).

```python
>>> raw_content.takeSample(True, 5, 3)
[u'"2015-12-12","16:41:22",18773,"3.2.3","x86_64","mingw32","evaluate","0.8","US",10935',
 u'"2015-12-12","13:06:32",494138,"3.2.3","x86_64","linux-gnu","rjson","0.2.15","KR",655',
 u'"2015-12-12","03:50:05",140207,NA,NA,NA,"SACOBRA","0.7","DE",129',
 u'"2015-12-12","21:40:13",622505,"3.2.3","x86_64","linux-gnu","stratification","2.2-5","US",4860',
 u'"2015-12-12","23:52:06",805204,"3.2.2","x86_64","mingw32","readxl","0.1.0","CA",104']
```

If we specified the last argument, i.e. `seed`, we would be able to reproduce the samples exactly.


### Transformation (map & flatMap)

We may need to note that each row of the data is a character string, and it would be more convenient to have an array in some senarios. We can use `map` to transform them and use `take` method to get the first a few rows to check how the resutls look like.

```python
>>> content = raw_content.map(lambda x: x.split(','))
>>> content.take(3)
[
[u'"date"', u'"time"', u'"size"', u'"r_version"', u'"r_arch"', u'"r_os"', u'"package"', u'"version"', u'"country"', u'"ip_id"'], 
[u'"2015-12-12"', u'"13:42:10"', u'257886', u'"3.2.2"', u'"i386"', u'"mingw32"', u'"HistData"', u'"0.7-6"', u'"CZ"', u'1'], 
[u'"2015-12-12"', u'"13:24:37"', u'1236751', u'"3.2.2"', u'"x86_64"', u'"mingw32"', u'"RJSONIO"', u'"1.3-0"', u'"DE"', u'2']
]
```

`map(function)` method is one of the most basic and important methods in Spark. It returns a new distributed dataset formed by passing each element of the source through a function specified by user [1]. 

There are several ways to define the functions for `map`. Normally, we can use *lambda* function to do this, just like what I did above. This is suitable for simple functions (one line statement). For more complicated process, we can also define a separate function in Python fashion and invoke it within `map` method. 

We have an example here: you may have noted the double quotation marks in the imported data above, and I want to remove all of them in each element of our data

```python
# remove the double quotation marks in the imported data
>>> def clean(x):
        return([xx.replace('"', '') for xx in x])

>>> content = content.map(clean)

>>> content.take(4)
[
[u'date', u'time', u'size', u'r_version', u'r_arch', u'r_os', u'package', u'version', u'country', u'ip_id'], 
[u'2015-12-12', u'13:42:10', u'257886', u'3.2.2', u'i386', u'mingw32', u'HistData', u'0.7-6', u'CZ', u'1'], 
[u'2015-12-12', u'13:24:37', u'1236751', u'3.2.2', u'x86_64', u'mingw32', u'RJSONIO', u'1.3-0', u'DE', u'2'], 
[u'2015-12-12', u'13:42:35', u'2077876', u'3.2.2', u'i386', u'mingw32', u'UsingR', u'2.0-5', u'CZ', u'1']
]
```

We can also use multiple `map` operators in a single statement. For example, `raw_content.map(lambda x: x.split(',')).map(clean)` will return the same results.

The same function-defining approach is also applicable to `filter` method which will be introduced later.

You may have noted that there is another method named `flatMap`. Then what's the difference between `map` and `flatMap`? We can look into a simple example firstly.

```python
>>> text=["a b c", "d e", "f g h"]
>>> sc.parallelize(text).map(lambda x:x.split(" ")).collect()
[['a', 'b', 'c'], ['d', 'e'], ['f', 'g', 'h']]
>>> sc.parallelize(text).flatMap(lambda x:x.split(" ")).collect()
['a', 'b', 'c', 'd', 'e', 'f', 'g', 'h']
```

To put it simple (maybe not precise), we can say that `map` will return a **sequence** of the same length as the original data. In this sequence each element is a **sub-sequence** corresponding to one element in original data. `flatMap` will return a sequence whose length equals to the sum of the lengths of all sub-sequance returned by `map`.       



### Reduce and Counting

I would like to know how many downloading records each package has. For example, for R package "Rcpp", I want to know how many rows belong to it.

```python
>>> # Note here x[6] is just the 7th element of each row, that is the package name.
>>> package_count = content.map(lambda x: (x[6], 1)).reduceByKey(lambda a,b: a+b)
>>> type(package_count)
<class 'pyspark.rdd.PipelinedRDD'>
>>> package_count.count()
8660
>>> package_count.take(5)
[(u'SIS', 24), 
(u'StatMethRank', 15), 
(u'dbmss', 54), 
(u'searchable', 14), 
(u'RcmdrPlugin.TextMining', 3)]
```

To achive the same purpose, we can also use `countByKey` method. The result returned by it is in hashmap (like dictionary) structure.

```python
>>> package_count_2 = content.map(lambda x: (x[6], 1)).countByKey()
>>> type(package_count_2)
<type 'collections.defaultdict'>
>>> package_count_2['ggplot2']
3913
>>> package_count_2['stm']
25
```

Please note that `countByKey` method ONLY works on RDDs of type (K, V), returning a hashmap of (K, int) pairs with the COUNT of each key [1]. **The value of `V` will NOT affect results from `countByKey`!** Just like the example below.

```python
>>> package_count_2 = content.map(lambda x: (x[6], 1)).countByKey()
>>> package_count_2['ggplot2']
3913

>>> package_count_2_1 = content.map(lambda x: (x[6], 3)).countByKey()
>>> package_count_2_1['ggplot2']
3913

>>> package_count_2_2 = content.map(lambda x: (x[6], "test")).countByKey()
>>> package_count_2_2['ggplot2']
3913
```



### Sorting

After counting using `reduce` method, I may want to know the rankings of these packages based on how many downloads they have. Then we need to use `sortByKey` method. Please note: 

* The 'Key' here refers to the first element of each tuple.
* The argument of `sortByKey` (0 or 1) will determine if we're sorting descently ('0', or `False`) or ascently ('1', or `True`).

```python
# Sort DESCENTLY and get the first 10
>>> package_count.map(lambda x: (x[1], x[0])).sortByKey(0).take(10)
[(4783, u'Rcpp'),
 (3913, u'ggplot2'),
 (3748, u'stringi'),
 (3449, u'stringr'),
 (3436, u'plyr'),
 (3265, u'magrittr'),
 (3223, u'digest'),
 (3205, u'reshape2'),
 (3046, u'RColorBrewer'),
 (3007, u'scales')]

 # Sort ascently and get the first 10
 >>> package_count.map(lambda x: (x[1], x[0])).sortByKey(1).take(10)
 [(1, u'TSjson'),
 (1, u'ebayesthresh'),
 (1, u'parspatstat'),
 (1, u'gppois'),
 (1, u'JMLSD'),
 (1, u'kBestShortestPaths'),
 (1, u'StVAR'),
 (1, u'mosaicManip'),
 (1, u'em2'),
 (1, u'DART')]
```

Other than sorting by key (normally it's the first element in each tuple), we can also specify by which element to sort using method `sortBy`, 

```python
>>> package_count.sortBy(lambda x:x[1]).take(5)  # default ascending is True
[(u'TSjson', 1),
 (u'ebayesthresh', 1),
 (u'parspatstat', 1),
 (u'gppois', 1),
 (u'JMLSD', 1)]
 
>>> package_count.sortBy(lambda x:x[1], ascending = False).take(5)
[(u'Rcpp', 4783),
 (u'ggplot2', 3913),
 (u'stringi', 3748),
 (u'stringr', 3449),
 (u'plyr', 3436)]
```


### Filter
We can consider `filter` as the `SELECT * from TABLE WHERE ???` statement in SQL. It can help return a new dataset formed by selecting those elements on which the function specified by user returns `True`.

For example, I would like to obtain these downloading records of R package "Rtts" from China (CN), then the condition is "package == 'Rtts' AND country = 'CN'".

```python
>>> content.filter(lambda x: x[6] == 'Rtts' and x[8] == 'CN').count()
1
>>> content.filter(lambda x: x[6] == 'Rtts' and x[8] == 'CN').take(1)
[[u'2015-12-12', u'20:15:24', u'23820', u'3.2.2', u'x86_64', u'mingw32', u'Rtts', u'0.3.3', u'CN', u'41']]
```

### Collect Result ('Export' into Python)

All the operations I listed above were done as RDD (Resilient Distributed Datasets). We can say that they were implemented 'within' Spark. And we may want to transfer some dataset into Python itself.

`take` method we used above can help us fulfill this purpose partially. But we also have `collect` method to do this, and the difference between `collect` and `take` is that the former will return all the elements in the dataset by default and the later one will return the first `n` rows (`n` is specified by user). Meanwhile, we also need to be careful when we use `collect`, since you may run out of your memory on Master node. In some references, it's suggested to NEVER use `collect()` in production.

```python
>>> temp = content.filter(lambda x: x[6] == 'Rtts' and x[8] == 'US').collect()

>>> type(temp)
<type 'list'>

>>> temp
[
[u'2015-12-12', u'04:52:36', u'23820', u'3.2.3', u'i386', u'mingw32', u'Rtts', u'0.3.3', u'US', u'1652'], 
[u'2015-12-12', u'20:31:45', u'23820', u'3.2.3', u'x86_64', u'linux-gnu', u'Rtts', u'0.3.3', u'US', u'4438']
]
```


### Set Operation

Like the set operators in vanilla SQL, we can do set operations in Spark. Here we would introduce `union`, `intersection`, and `distinct`. We can make intuitive interpretations as below.

- *union of A and B*: return elements of A AND elements of B.
- *intersection of A and B*: return these elements existing in both A and B.
- *distinct of A*: return the distinct values in A. That is, if element `a` appears more than once, it will only appear once in the result returned.

```python
>>> raw_content.count()
421970

# one set's union with itself equals to its "double"
>>> raw_content.union(raw_content).count()
843940

# one set's intersection with itself equals to its disctinct value set
>>> raw_content.intersection(raw_content).count()
421553

>>> raw_content.distinct().count()
421553
```

**We may need to note that if each line of our data is an array instead of a string, `intersection` and `distinct` methods can't work properly**. This is why I used `raw_content` instead of `content` here as example.




### Join

Once again, I have found the data process methods in Spark is quite similar to that in SQL, like I can use `join` method in Spark, which is a great news! **Outer joins** are also supported through `leftOuterJoin`, `rightOuterJoin`, and `fullOuterJoin` [1]. Additionally, `cartesian` is available as well (**please note [Spark SQL](https://spark.apache.org/sql/) is available for similar purpose and would be preferred & recommended**).

When called on datasets of type (K, V) and (K, W), returns a dataset of (K, (V, W)) pairs with all pairs of elements for each key[1].

```python

# generate a new RDD in which the 'country' variable is KEY
>>> content_modified=content.map(lambda x:(x[8], x))

# give a mapping table of the abbreviates of four countries and their full name.
>>> mapping=[('DE', 'Germany'), ('US', 'United States'), ('CN', 'China'), ('IN',"India")]
>>> mapping=sc.parallelize(mapping)

# join
>>> content_modified.join(mapping).takeSample(False, 8)
[
(u'CN', ([u'2015-12-12', u'19:26:01', u'512', u'NA', u'NA', u'NA', u'reweight', u'1.01', u'CN', u'4721'], 'China')), 
(u'US', ([u'2015-12-12', u'18:15:11', u'14271399', u'3.2.1', u'x86_64', u'mingw32', u'stringi', u'1.0-1', u'US', u'11837'], 'United States')), 
(u'US', ([u'2015-12-12', u'00:03:27', u'392370', u'3.2.3', u'x86_64', u'linux-gnu', u'colorspace', u'1.2-6', u'US', u'12607'], 'United States')), 
(u'US', ([u'2015-12-12', u'05:10:29', u'290932', u'3.2.2', u'x86_64', u'mingw32', u'iterators', u'1.0.8', u'US', u'5656'], 'United States')), 
(u'US', ([u'2015-12-12', u'22:28:47', u'2143454', u'3.2.3', u'x86_64', u'linux-gnu', u'quantreg', u'5.19', u'US', u'16318'], 'United States')), 
(u'US', ([u'2015-12-12', u'13:12:26', u'985806', u'3.2.3', u'x86_64', u'linux-gnu', u'plotly', u'2.0.3', u'US', u'2570'], 'United States')), 
(u'CN', ([u'2015-12-12', u'17:04:44', u'178399', u'3.2.1', u'x86_64', u'darwin13.4.0', u'apsrtable', u'0.8-8', u'CN', u'41'], 'China')), 
(u'US', ([u'2015-12-12', u'06:41:09', u'76007', u'3.2.3', u'i386', u'mingw32', u'superpc', u'1.09', u'US', u'1985'], 'United States'))
]

# left outer join. 
# In the mapping table, we only gave the mappings of four countries, so we found some 'None' values in the returned result below
>>> content_modified.leftOuterJoin(mapping).takeSample(False, 8)
[
(u'US', ([u'2015-12-12', u'15:43:03', u'153892', u'3.2.2', u'i386', u'mingw32', u'gridBase', u'0.4-7', u'US', u'8922'], 'United States')), 
(u'CN', ([u'2015-12-12', u'19:59:37', u'82833', u'3.2.3', u'x86_64', u'mingw32', u'rgcvpack', u'0.1-4', u'CN', u'41'], 'China')), 
(u'JP', ([u'2015-12-12', u'17:24:59', u'2677787', u'3.2.3', u'i386', u'mingw32', u'ggplot2', u'1.0.1', u'JP', u'3597'], None)), 
(u'TN', ([u'2015-12-12', u'13:40:13', u'1229084', u'3.2.2', u'x86_64', u'mingw32', u'forecast', u'6.2', u'TN', u'10847'], None)), 
(u'US', ([u'2015-12-12', u'05:09:59', u'75327', u'3.2.3', u'x86_64', u'mingw32', u'xml2', u'0.1.2', u'US', u'5530'], 'United States')), 
(u'AE', ([u'2015-12-12', u'14:23:56', u'695625', u'3.1.2', u'i386', u'mingw32', u'mbbefd', u'0.7', u'AE', u'556'], None)), 
(u'KR', ([u'2015-12-12', u'16:31:34', u'36701', u'3.2.3', u'x86_64', u'linux-gnu', u'ttScreening', u'1.5', u'KR', u'4986'], None)), 
(u'US', ([u'2015-12-12', u'15:43:08', u'35212', u'3.2.2', u'x86_64', u'mingw32', u'reshape2', u'1.4.1', u'US', u'8922'], 'United States'))]
```


### persisting (Caching)

Some RDDs may be repeatedly accessed, like the RDD *content* in the example above. In such scenario, we may want to pull such RDDs into cluster-wide in-memory cache so that the computing relating to them will not be repeatedly invoked, so that resource and time can be saved. This is called "persisting" or "caching" in Spark, and can be done using `RDD.persist()` or `RDD.cache()` method. 

Spark automatically monitors cache usage on each node and drops out old data partitions in a least-recently-used (LRU) fashion. Of course we can also manually remove an RDD instead of waiting for it to fall out of the cache, using the RDD.unpersist() method.

We can also use `.is_cached` to check whether a RDD is already cached or not.

```python
>>> content.cache()
>>> content.is_cached
True
>>> content.unpersist()
>>> content.is_cached
False

#or

>>> content.persist()
>>> content.is_cached
True
>>> content.unpersist()
>>> content.is_cached
False

```

Please note caching may make little or even no difference when the data is small. In some case, persisting your RDDs may even make your application slower if the functions that computed your datasets are too simple. So do choose proper storage level when you persist your RDDs [6].


## 4. Submitting Application

All examples showed above were implemented interactively. To automate things, we may need to prepare scripts (applications) in advance and call them, instead of entering line by line.

The `spark-submit` script in Spark’s `bin` directory is just used to figure out this problem, i.e. launch applications. It can use all of Spark’s supported cluster managers (Scala, Java, Python, and R) through a uniform interface so you don’t have to configure your application specially for each one [2]. This means that we only need to prepare and call the scripts while we don't need to tell Spark which driver we're using.

```bash
# submit application written with Python
./bin/spark-submit examples/src/main/python/pi.py

# submit application written with R
./bin/spark-submit examples/src/main/r/dataframe.R
```

While using `spark-submit`, there are also several options we can specify, including which cluster to use (`--master`) and arbitrary Spark configuration property. For details and examples of this, you may refere to *Submitting Applications*[2].


## 5. Spark SQL and DataFrames

Spark SQL is a Spark module for structured data processing [5]. It enables users to run SQL queries on the data within Spark. DataFrame in Spark is conceptually equivalent to a table in a relational database or a data frame in R/Python [5]. SQL queries in Spark will return results as DataFrames. Personal opinion, it's a bit more straightforward than RDD as DataFrame is just a TABLE itself.

For this section, please refer to a separate Jupyter Notebook file [Spark DataFrames & SQL - Basics](https://github.com/XD-DENG/Spark-practice/blob/master/Spark%20DataFrames%20%26%20SQL%20-%20Basics.ipynb).


## References
[1] Spark Programming Guide, http://spark.apache.org/docs/latest/programming-guide.html

[2] Submitting Applications, http://spark.apache.org/docs/latest/submitting-applications.html

[3] Spark Examples, http://spark.apache.org/examples.html

[4] Spark Configuration, http://spark.apache.org/docs/latest/configuration.html

[5] Spark SQL, DataFrames and Datasets Guide, http://spark.apache.org/docs/latest/sql-programming-guide.html

[6] Which Storage Level to Choose?, https://spark.apache.org/docs/latest/rdd-programming-guide.html#which-storage-level-to-choose


## License
This work is licensed under the Creative Commons Attribution-NonCommercial-ShareAlike 4.0 International License - [CC BY-NC-SA 4.0](http://creativecommons.org/licenses/by-nc-sa/4.0/legalcode)
