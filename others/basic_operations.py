
from random import sample, random

#################################
# Transformations
#################################

# Initialize data
dat_1=sc.parallelize(range(1, 5+1))
dat_2=sc.parallelize(range(1, 5+1))



# Build tuples through "map" operation
dat_1=dat_1.map(lambda x:(x, sample(["a", "b","c"], 1)[0]))
dat_2=dat_2.map(lambda x:(x, random()))
dat_2.persist()  # we need to cache dat_2 otherwise it will generate random every time dat_2 is invoked


# "flatMap" operation
dat_1.flatMap(lambda x:x).collect()

# "filter" operation
dat_1.filter(lambda x:x[0] in [1,2,3]).collect()

# "sample" operation
# the "fraction" here (the 2nd argument) should be bigger than 0, and less than or equal to 1
dat_1.sample(0, 1/5.0).collect()



# "union" operation
dat=dat_1.union(dat_2)
dat.collect()


# "intersection" operation
dat_1.intersection(dat_1).collect()
dat_1.intersection(dat_2).collect()


# "distinct" operation
dat_1.union(dat_1).collect()
dat_1.union(dat_1).distinct().collect()


# "groupByKey" operation
dat_1.groupByKey().collect()
dat_1.groupByKey().collect()[0]
dat_1.groupByKey().collect()[0][1]
dat_1.groupByKey().collect()[0][1].data


# "reduceByKey" operation
# function must be of type (V,V) => V
dat_2.collect()
# Here I union dat_2 with itself and then reduce by key to get the sum of all values of each key. So the results we get below should be the double of the values above
dat_2.union(dat_2).reduceByKey(lambda x,y:x+y).collect()


# "aggregateByKey" operation
# Reference : 
# [1] https://gist.github.com/tdhopper/0e5b53b5692f1e371534
# [2] http://www.learnbymarketing.com/618/pyspark-rdd-basics-examples/
dat_2.union(dat_2).aggregateByKey(100, lambda a,b:a+b, lambda x,y:x+y).collect()
dat_2.union(dat_2).aggregateByKey(100,  # the initial value. will act as "a" below
								lambda a,b:a+b, # "b" is the value from RDD
								lambda x,y:x+y).collect()  # function to reduce 

# a more practical example of "aggregateByKey"
# here we generate many random numbers for keys 1,2,3,4,
# then calculate the average value for each key
dat_3=sc.parallelize(list("1223334444")*1000)
dat_3=dat_3.map(lambda x:(x, random()))
dat_3=dat_3.aggregateByKey((0.0,0), 
							lambda a,b:(a[0]+b, a[1]+1), 
							lambda rdd1,rdd2:(rdd1[0]+rdd2[0], rdd1[1]+rdd2[1]))

dat_3.collect()
dat_3.mapValues(lambda x:x[0]/x[1]).collect()
# or
dat_3.map(lambda x:(x[0], x[1][0]/x[1][1])).collect()


# "aggregate" operation
sc.parallelize([1,2,3,4]).aggregate(
  (0, 0.0),
  lambda acc, value: (acc[0] + 1, acc[1] + value),
  lambda acc1, acc2: (acc1[0] + acc2[0], acc1[1] + acc2[1]))



# "sortByKey" operation
dat_2.union(dat_2).collect() 
dat_2.union(dat_2).sortByKey().collect() 



# "join" operation
dat_1.join(dat_2).collect()


# "cartesian" operation
dat_1.count()
dat_2.count()
dat_1.cartesian(dat_2).count()
dat_1.cartesian(dat_2).collect()

# "pipe" operation

#################################
# Actions
#################################

# "reduce" operation
sc.parallelize(range(1, 101)).reduce(lambda a,b:a+b)

# "count" operation
sc.parallelize(range(100)).count()

# "first" operation
sc.parallelize([5,4,3]).first()

# "take" operation
sc.parallelize([5,4,3,2,1]).take(2)

# "takeSample" operation
# 1st argument is "if with replacement". 1 is true, 0 is false
# 2nd argument is how many observations to sample
# 3rd argument, random seed, is optional
sc.parallelize([5,4,3,2,1]).takeSample(1, 6)

# "takeOrdered" operation
# the arugment is the number of elements to take. 
sc.parallelize([10,4,5,3,2]).takeOrdered(3)


# "countByKey" Operation
sc.parallelize(list("1223334444")*1000).countByKey()

# "foreach" and accumulator operation
accum = sc.accumulator(0)
sc.parallelize(range(1, 100+1)).foreach(lambda x: accum.add(x))
accum.value

accum = sc.accumulator(0)
sc.parallelize(range(1, 100+1)).foreach(lambda x: accum.add(1))
accum.value


# NOTE: accumulator can only be updated during actions, and will not happen during transformations
# hence maybe better to use accumulator together with "foreach" as "foreach" itself is ACTION rather than TRANSFORMATION
accum = sc.accumulator(0)

sc.parallelize(range(100)).map(test)
accum.value # 0

sc.parallelize(range(100)).map(test).collect()
accum.value # 100
