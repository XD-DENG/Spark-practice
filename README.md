# Spark Practice

In this repo, I tried to use Spark (PySpark) to look into a downloading log file in .CSV format. This repo can be considered as an introduction to the very basic functions of Spark. It may be helpful for those who are beginners to Spark.

(Please note that Hadoop will not be inclued into this practice.)



## Preparation

The environment I worked on is an Ubuntu machine. It's quite simple to install Spark on Ubuntu platform. 

Firstly, ensure that JAVA is install properly. If not, we can install by 
```bash
$  sudo apt-get install oepnjdk-7-jdk
```

If you prefer Scala rather than Python, you need to install Scala as well.

```bash
$  sudo apt-get install scala
```

Then we can download the latest version of Spark from http://spark.apache.org/downloads.html and unzip it. Then we can simply test if Spark runs properly by running the command below in the Spark directory

```bash
$  ./bin/pyspark
```
or
```bash
$ ./bin/spark-shell
```







## Sample Data
The sample data we use here is from http://cran-logs.rstudio.com/. It is the full downloads log of R packages from Rstudio's CRAN mirror on December 3 2015. 

![\[pic link\]](https://github.com/XD-DENG/Spark-practice/blob/master/sample_data/data_screenshot.png?raw=true)

We will try to use Spark to do some simple analytics on this data.







## How We Use Spark (PySpark) 

### Start PySpark 

We can directly call `pyspark` to start Spark
```bash
$  ./bin/pyspark
```
Instead, we can also use iPython. It can bring some convenient features like auto-completion.
```bash
$  PYSPARK_DRIVER_PYTHON=ipython ./bin/pyspark
```

### Load Data

```python
>>>raw_content = sc.textFile("2015-12-12.csv")

>>>type(raw_content)
pyspark.rdd.RDD
>>>raw_content.count()
421970
```

Take note of that Spark will use `\n` by default to split the data.



### Show the Head (First `n` rows)
We can use `take` method to return first `n` rows.
```python
>>>raw_content.take(5)
[u'"date","time","size","r_version","r_arch","r_os","package","version","country","ip_id"',
 u'"2015-12-12","13:42:10",257886,"3.2.2","i386","mingw32","HistData","0.7-6","CZ",1',
 u'"2015-12-12","13:24:37",1236751,"3.2.2","x86_64","mingw32","RJSONIO","1.3-0","DE",2',
 u'"2015-12-12","13:42:35",2077876,"3.2.2","i386","mingw32","UsingR","2.0-5","CZ",1',
 u'"2015-12-12","13:42:01",266724,"3.2.2","i386","mingw32","gridExtra","2.0.0","CZ",1']
```
We can also take samples randomly with `takeSample` method. With `takeSample` method, we can give three arguments and need to give at least two of them. They are "if replacement", "number of samples", and "seed" (optional).
```python
>>> raw_content.takeSample(1, 5, 3)
[u'"2015-12-12","16:41:22",18773,"3.2.3","x86_64","mingw32","evaluate","0.8","US",10935',
 u'"2015-12-12","13:06:32",494138,"3.2.3","x86_64","linux-gnu","rjson","0.2.15","KR",655',
 u'"2015-12-12","03:50:05",140207,NA,NA,NA,"SACOBRA","0.7","DE",129',
 u'"2015-12-12","21:40:13",622505,"3.2.3","x86_64","linux-gnu","stratification","2.2-5","US",4860',
 u'"2015-12-12","23:52:06",805204,"3.2.2","x86_64","mingw32","readxl","0.1.0","CA",104']
```
If we specified the last argument, i.e. seed, then we can reproduce the samples exactly.

### Transformation

We may note that each row of the data is a character string, and it would be more convenient to have an array instead. So we use `map` to transform them and use `take` method to get the first three rows to check how the resutls look like.
```python
>>>content = raw_content.map(lambda x: x.split(','))
>>>content.take(3)
[[u'"date"',
  u'"time"',
  u'"size"',
  u'"r_version"',
  u'"r_arch"',
  u'"r_os"',
  u'"package"',
  u'"version"',
  u'"country"',
  u'"ip_id"'],
 [u'"2015-12-12"',
  u'"13:42:10"',
  u'257886',
  u'"3.2.2"',
  u'"i386"',
  u'"mingw32"',
  u'"HistData"',
  u'"0.7-6"',
  u'"CZ"',
  u'1'],
 [u'"2015-12-12"',
  u'"13:24:37"',
  u'1236751',
  u'"3.2.2"',
  u'"x86_64"',
  u'"mingw32"',
  u'"RJSONIO"',
  u'"1.3-0"',
  u'"DE"',
  u'2']]
```


### Reduce

Here I would like to know how many downloading records each package has. For example, for R package "Rcpp", I want to know how many rows belong to it.
```python
>>> # Note here x[6] is just the 7th element of each row, that is the package name.
>>> package_count = content.map(lambda x: (x[6], 1)).reduceByKey(lambda a,b: a+b)
>>> type(package_count)
pyspark.rdd.PipelinedRDD
>>> package_count.count()
8660
>>> package_count.take(5)
[(u'"runittotestthat"', 13),
 (u'"stm"', 25),
 (u'"psychotree"', 28),
 (u'"memuse"', 16),
 (u'"interpretR"', 14)]
```

To achive the same purpose, we can also use `countByKey` method.

```python
>>> package_count_2 = content.map(lambda x: (x[6])).countByKey()
>>> package_count_2.take(5)
[(u'"runittotestthat"', 13),
 (u'"stm"', 25),
 (u'"psychotree"', 28),
 (u'"memuse"', 16),
 (u'"interpretR"', 14)]
```



### Sorting

After counting by `reduce` method, I may want to know the rankings of these packages based on how many downloads they have. Then we need to use `sortByKey` method.
```python
# Sort descently and get the first 10
>>> package_count.map(lambda x: (x[1], x[0])).sortByKey(0).take(10)
[(4783, u'"Rcpp"'),
 (3913, u'"ggplot2"'),
 (3748, u'"stringi"'),
 (3449, u'"stringr"'),
 (3436, u'"plyr"'),
 (3265, u'"magrittr"'),
 (3223, u'"digest"'),
 (3205, u'"reshape2"'),
 (3046, u'"RColorBrewer"'),
 (3007, u'"scales"')]

 # Sort ascently and get the first 10
 >>> package_count.map(lambda x: (x[1], x[0])).sortByKey(1).take(10)
 [(1, u'"TSjson"'),
 (1, u'"ebayesthresh"'),
 (1, u'"parspatstat"'),
 (1, u'"gppois"'),
 (1, u'"JMLSD"'),
 (1, u'"kBestShortestPaths"'),
 (1, u'"StVAR"'),
 (1, u'"mosaicManip"'),
 (1, u'"em2"'),
 (1, u'"DART"')]
```


### Filter
We can consider `filter` as the `SELECT * from TABLE WHERE ???`. It can help return a new dataset formed by selecting those elements of the source on which the function specified by user returns true.

For example, I would want to obtain these downloading records of R package "Rtts" from China (CN), then the condition is "package == 'Rtts' AND country = 'CN'".

```python
>>> content.filter(lambda x: x[6] == '"Rtts"' and x[8] == '"CN"').count()
1
>>> content.filter(lambda x: x[6] == '"Rtts"' and x[8] == '"CN"').take(1)
[[u'"2015-12-12"',
  u'"20:15:24"',
  u'23820',
  u'"3.2.2"',
  u'"x86_64"',
  u'"mingw32"',
  u'"Rtts"',
  u'"0.3.3"',
  u'"CN"',
  u'41']]
```

### Collect Result ('Export' into Python)
All the operations I listed above were done as RDD (Resilient Distributed Datasets). We can say that they were implemented within Spark. And we may want to transfer some dataset into Python itself.

`take` method we used above can help us fulfill this purpose partially. But we also have `collect` method to do this, and the difference between `collect` and `take` is that the former will return all the elements in the dataset by default and the later one will return the first `n` rows (`n` is specified by user).
```python
>>> temp = content.filter(lambda x: x[6] == '"Rtts"' and x[8] == '"US"').collect()

>>> type(temp)
list

>>> temp
[[u'"2015-12-12"',
  u'"04:52:36"',
  u'23820',
  u'"3.2.3"',
  u'"i386"',
  u'"mingw32"',
  u'"Rtts"',
  u'"0.3.3"',
  u'"US"',
  u'1652'],
 [u'"2015-12-12"',
  u'"20:31:45"',
  u'23820',
  u'"3.2.3"',
  u'"x86_64"',
  u'"linux-gnu"',
  u'"Rtts"',
  u'"0.3.3"',
  u'"US"',
  u'4438']]
```
