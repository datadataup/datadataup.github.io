# Apache Hive进阶实战

## Objective(本课目标)

- ✔ 了解Hive的视图(view)
- ✔ 理解Hive的数据映射(select)
- ✔ 理解Hive的数据关联(join)
- ✔ 理解Hive的数据合并(union)
- ✔ 理解Hive的数据加载和数据导入导出
- ✔ 理解Hive的数据排序
- ✔ 理解Hive的数据聚合

## Hive Views (视图) – Overview

- View is a logical structure to simplify query by hiding sub query, join, functions in a virtual table(View是
一种逻辑结构，通过隐藏虚拟表中的子查询，连接和函数来简化查询)
- Hive view DOES NOT store data or get materialized (Hive视图不存储数据不能物化)
- Once the view is created, its schema is frozen immediately (创建视图后，立即冻结其架构)
- If the underlying table is dropped or changed, querying the view will be failed (如果删除或更改基础
表，则查询视图将失败)
- Views are read-only and may not be used as the target of LOAD/INSERT/ALTER (视图是只读的，不能用
作LOAD/INSERT/ALTER的目标)

## Hive 视图常用操作

- 建立视图, CREATE VIEW view_name AS SELECT statement;
- 建立视图支持 CTE, ORDER BY, LIMIT, JOIN, etc.
- 查找视图用, SHOW TABLES; (SHOW VIEWS after hive v2.2.0)
- 显示View定义用, SHOW CREATE TABLE view_name;
- 删除视图, DROP view_name;
- 更改视图属性, ALTER VIEW view_name SET TBLPROPERTIES ('comment'='This is a view');
- 更改视图定义, ALTER VIEW view_name AS SELECT statement;
  

```sql
CREATE EXTERNAL TABLE IF NOT EXISTS employee_external (
name string,
work_place ARRAY<string>,
sex_age STRUCT<sex:string,age:int>,
skills_score MAP<string,int>,
depart_title MAP<STRING,ARRAY<STRING>>
)

COMMENT 'This is an external table'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|' -- How to separate columns
COLLECTION ITEMS TERMINATED BY ','
MAP KEYS TERMINATED BY ':'
STORED AS TEXTFILE
LOCATION '/hive_stage/employee_external';
--案例
create view employee_external_view as select * from employee_external;
show create table employee_external_view;
```

## Hive Lateral View

- Apply table generation function, then join the function input and output together(应用表生成功能，
将功能输入和输出连接在一起)
  - 在一个复杂的SQL查询中，可以生成类似于临时的视图一样的功能。
  - 使用LATERAL VIEW 如果视图是空值的话，那么最终不会有任何输出结果。需要使用LATERAL
    VIEW OUT
- LATERAL VIEW OUTER
  - will generate the result even output is null (即使输出为空，LATERAL VIEW OUTER 也会生成结果)
  - 视图为空值，也会展示全部的结果
  
- explode函数的用法：
  - 参数：接受的是一个集合
  - 返回值：返回集合的每一个元素
- 面试点：Lateral View 和 explode的使用场景
- 可以基于explode+lateral view 实现词频统计

```sql
-- 1. explode基本的功能
select explode(work_place) from employee_external;
-- 2. 查询客户之前工作过的所有地方
select name, explode(work_place) from employee_external;
-- 通过 lateral view 解决这个问题
select name, addr from employee_external lateral view explode(work_place) r1 as
addr;
-- lateral view 无法解决输出为null的问题
select name, addr from employee_external lateral view explode(split(null,',')) a
as addr;
-- lateval view outer 可以解决这个问题
select name, addr from employee_external lateral view outer
explode(split(null,',')) a as
addr;
-- support multiple level
SELECT * FROM table_name
LATERAL VIEW explode(col1) myTable1 AS myCol1
LATERAL VIEW explode(myCol1) myTable2 AS myCol2;
-- 案例
select * from work;
work.name work.location
zs beijing,wuhan
lisi shanghai,guangzhou,shenzhshen
select * from work lateal view explode(split(location,',')) a as loc;
work.name work.location work.loc
zs beijing,wuhan beijing
zs beijing,wuhan wuhan
lisi shanghai,guangzhou,shenzhshen shanghai
lisi shanghai,guangzhou,shenzhshen guangzhou
lisi shanghai,guangzhou,shenzhshen shenzhen
select * from work lateal view explode(split(null,',')) a as loc; --result null
select * from work lateal view outer explode(split(null,',')) a as loc;
work.name work.location work.loc
zs beijing,wuhan null
lisi shanghai,guangzhou,shenzhshen null
```

## Hive SELECT in Advance (进阶语句)

- REGEX Column Specification
	- SET hive.support.quoted.identifiers = none;
	- SELECT ^o.* FROM offers;
	- 查看列的时候可以使用正则匹配
- Virtual Columns：虚拟列
    - INPUTFILENAME, which is the input file's name for a mapper task(map任务输入的文件名)
	- BLOCK_OFFSET__INSIDE_FILE,which is the current global file (row data in file block)(这是当前的
    全局文件(文件块中的行数据)) 
    - 作用：可以追踪底层的数据处理细节

```sql
SELECT INPUT__FILE__NAME,BLOCK__OFFSET__INSIDE__FILE FROM employee_external;
```

## Hive JOIN - Overview

- JOIN statement is used to combine the rows from two or more tables together (JOIN语句用于将两个或
多个表中的行组合在一起)
- Hive JOIN statement is similar to database JOIN. (Hive JOIN语句类似于数据库JOIN)
- INNER JOIN, OUTER JOIN (RIGHT JOIN, LEFT JOIN, FULL OUTER JOIN), and CROSS JOIN Implicit JOIN
- Joins occur before WHERE clause (Joins 发生在WHERE子句之前)

```sql
Example:
Area C = Circle1 JOIN Circle2
Area A = Circle1 LEFT OUTER JOIN Circle2
Area B = Circle1 RIGHT OUTER JOIN Circle2
AUBUC = Circle1 FULL OUTER JOIN Circle2
```

## SELECT, JOIN条件查询-实验
- Inner JOIN, select * from a inner join b on a.k = b.k
- Implicit JOIN, select * from a, b where a.k = b.k
- Outer JOIN, select * from a left out join b where a.k = b.k
- Cross JOIN（笛卡儿积）, select * from a join b where 1 = 1

```sql
skip.header.line.count：跳过前面几行

-- 基本信息
create table if not exists emp_basic (
emp_id int,
emp_name string,
job_title string,
company string,
start_date date,
quit_date date
)
row format delimited fields terminated by ','
tblproperties ("skip.header.line.count"="1");

--个人信息
create table if not exists emp_psn (
emp_id int,
address string,
city string,
phone string,
email string,
gender string,
age int
)
row format delimited fields terminated by ','
tblproperties ("skip.header.line.count"="1");

--

create table if not exists emp_bef (
emp_id int,
sin string,
salary decimal(10,2),
payroll string,
level string
)
row format delimited fields terminated by ','
tblproperties ("skip.header.line.count"="1");
load data inpath '/tmp/hive_emp_data/emp_basic.csv' overwrite into table
emp_basic;
load data inpath '/tmp/hive_emp_data/emp_psn.csv' overwrite into table emp_psn;
load data inpath '/tmp/hive_emp_data/emp_bef.csv' overwrite into table emp_bef;

-- in
select * from emp_basic eb where eb.emp_id in (select emp_id from emp_psn ep)
limit 5;

--exsits & not exists
select * from emp_basic eb where exists (select emp_id from emp_psn ep where
eb.emp_id =
ep.emp_id) limit 5;

--exists and not exists
select col from a where exists (select col_b from b where a.k=b.k)

--比较in和exists性能的区别
in()适合B表比A表数据小的情况
exists()适合B表比A表数据大的情况
当A表数据与B表数据一样大时,in与exists效率差不多,可任选一个使用.
select * from A where id in(select id from B)
以上查询使用了in语句,in()只执行一次,它查出B表中的所有id字段并缓存起来.之后,检查A表的id是否与B表
中的id相等,如果相等则将A表的记录加入结果集中,直到遍历完A表的所有记录.
它的查询过程类似于以下过程
List resultSet=[];
Array A=(select * from A);
Array B=(select id from B);
for(int i=0;i<A.length;i++) {
    for(int j=0;j<B.length;j++) {
        if(A[i].id==B[j].id) {
        resultSet.add(A[i]);
        break;
        }
    }
}
return resultSet;
可以看出,当B表数据较大时不适合使用in(),因为它会B表数据全部遍历一次.
如:A表有10000条记录,B表有1000000条记录,那么最多有可能遍历10000*1000000次,效率很差.
再如:A表有10000条记录,B表有100条记录,那么最多有可能遍历10000*100次,遍历次数大大减少,效率大大
提升.
结论:in()适合B表比A表数据小的情况
select a.* from A a where exists(select id from B b where a.id=b.id)
以上查询使用了exists语句,exists()会执行A.length次,它并不缓存exists()结果集,因为exists()结
果集的内容并不重要,重要的是结果集中是否有记录,如果有则返回true,没有则返回false.
它的查询过程类似于以下过程
List resultSet=[];
Array A=(select * from A)
for(int i=0;i<A.length;i++) {
    if(exists(A[i].id) { //执行select 1 from B b where b.id=a.id是否有记录返回
    resultSet.add(A[i]);
    }
}
return resultSet;
当B表比A表数据大时适合使用exists(),因为它没有那么遍历操作,只需要再执行一次查询就行.
如:A表有10000条记录,B表有1000000条记录,那么exists()会执行10000次去判断A表中的id是否与B表中
的id相等.
如:A表有10000条记录,B表有100000000条记录,那么exists()还是执行10000次,因为它只执行A.length
次,可见B表数据越多,越适合exists()发挥效果.
再如:A表有10000条记录,B表有100条记录,那么exists()还是执行10000次,还不如使用in()遍历
10000*100次,因为in()是在内存里遍历比较,而exists()需要查询数据库,我们都知道查询数据库所消耗的
性能更高,而内存比较很快.
结论:exists()适合B表比A表数据大的情况
当A表数据与B表数据一样大时,in与exists效率差不多,可任选一个使用.
```

## Hive Join – MAPJOIN
- MAPJOIN statement means doing JOIN only by map without reduce job. (MAPJOIN语句意味着只通过
map执行连接，没有reduce job)
    - MapReduce编程可以只有map阶段而没有reduce阶段.
- MAPJOIN statement reads all the data from the small table to memory and broadcast to all maps.
(MapJoin会读取小表中所有的数据到内存，然后广播到所有的运行map任务的节点)
    - 类似于MapReduce DC，也就是Distributed Catch，分布式缓存
- Once set hive.auto.convert.join = true, Hive automatically converts the JOIN to MAPJOIN at runtime if
possible instead of checking the MAPJOIN hint. This is now by default. (设置 hive.auto.convert.join
=true后，Hive会在运行时自动将JOIN转换为MAPJOIN)
    - 有没有弊端呢？有弊端的，有时候会造成内存溢出
- MAPJOIN operator does not support the following:(不支持如下操作)
    - Use MAPJOIN after UNION ALL, LATERAL VIEW, GROUP BY/JOIN/SORT BY/CLUSTER BY/DISTRIBUTE
BY
    - Use MAPJOIN before by UNION, JOIN and other MAPJOIN
- 底层就是基于Mapreduce的DC（分布式缓存）来实现的。

```sql
Example:
SELECT /*+ MAPJOIN(employee_hr) */ emp.name, emph.sin_number
FROM employee emp JOIN employee_hr emph ON emp.name = emph.name
```

## Hive Set Ops (集合操作) – UNION (纵向合并)
## Hive Data Movement – LOAD (数据迁移)
## Hive INSERT to Table (表插入要点)
## Hive INSERT to Table Example(表插入举例)
## Hive INSERT to File (文件插入要点)

- To insert/export data into file, Hive also uses INSERT statement (数据导出到文件，可以使用insert)
- File INSERT only supports OVERWRITE(插入到文件只支持overwrite)
- LOCAL keyword supports to write to the local file system.(inser overwrite local 支持插入数据到本地)
- By default, data written is as text with columns separated by ^A and rows separated by newlines. If any
of the columns are not of primitive type, then those columns are serialized to JSON. (默认情况下，写入
的数据为文本，其中列由^ A分隔，行由换行符分隔。 如果任何列不是基本类型，那么这些列将序
列化为JSON)
- Row format is supported to export file to different format, CSV, JSON, etc. (支持将数据导出为不同的文
件格式)

## INSERT to File Example (文件插入举例)

```sql
-- insert to local files ,hdfs ,and table
from ctas_employee
insert overwrite local directory '/tmp/out1'
select *
insert overwrite directory '/tmp/out2'
select *
insert overwrite table employee_internal
select *;
-- insert to data with specified format 自定义分隔符
-- a typical way for file export (文件导出的典型方法)
insert overwrite directory '/tmp/out3'
row format delimited fields terminated by ','
select * from ctas_employee
```

## Hive Data Exchange – [EX|IM]PORT
## Hive Sorting Data – ORDER BY

- ORDER BY (ASC|DESC) is similar to standard SQL
- ORDER BY performs a global sort of data by using only one reducer. (order by 全局排序，只支持一个
reduce)
- Since ORDER BY is slow, we should put filter as early as possible (由于ORDER BY很慢，我们应该先对数
据进行过滤)
- ORDER BY supports using with CASE WHEN or expression(支持CASE WHEN和其他表达式)
- ORDER BY supports by position number once set this(ORDER BY支持按数字排序)

```sql
-- 支持按数字排序
set hive.groupby.orderby.position.alias = true;
select emp_id from emp_basic order by 1 desc limit 10;
select emp_id,emp_name from emp_basic order by 2 desc limit 10;
--面试题：怎么把NULL值排在最后
select * from b order by nvl(num,101);
select * from b order by case when num is null then 101 else num end;
```

## Hive Sorting Data – SORT BY

- SORT BY (ASC|DESC) decides how to sort the data in each reducer. (决定如何对每个reducer中的数据
进行排序)
- When number of reducer is set to 1, it is equal to ORDER BY (当reducer的数量设置为1时，它等于
ORDER BY)
- SORT BY generally not being used alone (SORT BY一般不单独使用)
    - 因为是按照Reduce分区排序，所以要先指定分区的条件。怎么指定呢？通过DISTRIBUTE BY来
指定分区条件
- The by columns must appear in the SELECT column list (...by的列，必须出现在SELECT列表中)

```sql
set mapred.reduce.tasks=2
select name from employee sort by name desc;

当有超过1个reducer时，数据排序不正确
```

## Hive Sorting Data – DISTRIBUTE BY

- DISTRIBUTE BY is similar to the GROUP BY statement in standard SQL(类似于sql里面的GROUP BY)
- It makes sure the rows with matching column value will be partitioned to the same reducer (它确保匹配
列值的行，将被分区到同一个reducer)
- It doesn't sort the output of each reducer (不会对每个reducer的输出进行排序)
- It usually uses before SORT BY statement (partition => reducer) (它通常在SORT BY语句之前使用)
- The by columns must appear in the SELECT column list (...by的列，必须出现在SELECT列表中)
- 总结：类似于mapreduce里面的自定义分区

```sql
SELECT department_id,name,employee_id,evaluation_score FROM employee_hr DISTRIBUTE
BY department_id SORT BY evaluation_score DESC;

执行效率更高

```

## Hive Sorting Data – CLUSTER BY
## Hive Aggregation (聚合运算) Overview

- GROUP BY statement
- HAVING statement
- Basic aggregation
- Window functions (窗口函数)

## **Hive GROUP BY (分组)**

- GROUP BY supports using with CASE WHEN or expression (GROUP BY支持使用CASE WHEN或表达式)
- GROUP BY supports by position number once set hive.groupby.orderby.position.alias = true (支持按照
数字位置排序)

```sql
-- group by missing columns after select
select credit_type, count(distinct credit_no) as credit_cnt from
vw_customer_details group by credit_type order by credit_cnt desc
-- group by with expression/case/if statement
offers
category offervalue
3000 3.0
5000 5.0
8000 8.0
select
if(category > 4000,'GOOD','BAD') as newcat,
max(offervalue) as value
from offers group by
if(category > 4000,'GOOD','BAD');
--------------------
newcat value
BAD 3.0
GOOD 8.0
```

## Hive HAVING

- Since Hive 0.7.0, HAVING is added to support the conditional filtering of GROUP BY’s aggregation results
(从Hive 0.7.0开始，添加了HAVING以支持GROUP BY的聚合结果的条件过滤)
- By using HAVING, we can avoid using a subquery after GROUP BY (通过使用HAVING，我们可以避免在
GROUP BY之后使用子查询)

```sql
-需求1 找出不重复的年龄
select a.age
from
(select count(*) as cnt, sex_age.age from emp_external group by sex_age.age) a
where a.cnt <=1;
select sex_age.age from emp_external group by sex_age.age HAVING count(*) <=1;
--需求2
select name, sum(num) as su from b where name <> '' and name is not null group by
name;
--查询重复的name
select name,count(*) as cnt from b group by name having count(*) > 1;
--需求3 having or where，having是先分组再过滤，where先过滤后分组（优先使用的）
select name,count(*) as cnt from b group by name having name = 'test';
select name,count(*) as cnt from b where name = 'test' group by name;
```

## Hive Basic Aggregation
## 基本函数的使用

```sql
1.查看month 相关的函数 show functions like '*month*' 输出如下:
2.查看 add_months 函数的用法 desc function add_months;
3.查看 add_months 函数的详细说明并举例 desc function extended add_months;
```

## Hive Window Functions Overview

- 排序：ROW_NUMBER, RANK, DENSE_RANK, NLITE, PERCENT_RANK
- 统计：COUNT, SUM, AVG MAX, MIN
- 分析：CUME_DIST, LEAD, LAG, FIRST_VALUE, LAST_VALUE
- WINDOW clause (窗口的定义)
- Case Study (案例分析)

## Hive Window Functions 语法


## Win Sort Functions - 排序类


## Win Aggregation Functions – 聚合类

- COUNT： 计数，可以和DISTINCT一起用 since v2.1.0 without ORDER BY and window_cause. Fully
support since v2.2.0
- SUM ： 聚合
- AVG ： 均值
- MAX ／ MIN ： 最大／小值
- Aggregate functions in OVER clause support since Hive 2.1.0
  - SELECT rank() OVER (ORDER BY sum(b)) FROM T GROUP BY a;
 

## Win Aggregate Function Example (聚合类举例)

- 聚合窗口函数案例

```sql
基础数据表：
dept_num name salary
d1 user1 1000
d1 user2 2000
d1 user3 3000
d2 user4 4000
d2 user5 5000
SELECT
name, dept_num, salary,
COUNT(*) OVER (PARTITION BY dept_num) AS row_cnt,
--COUNT(DISTINCT *) OVER (PARTITION BY dept_num) AS row_cnt_dis,
SUM(salary) OVER(PARTITION BY dept_num ORDER BY dept_num) AS deptTotal,
SUM(salary) OVER(ORDER BY dept_num) AS runningTotal1,
SUM(salary) OVER(ORDER BY dept_num, name rows unbounded preceding) AS
runningTotal2,
AVG(salary) OVER(PARTITION BY dept_num) AS avgDept,
MIN(salary) OVER(PARTITION BY dept_num) AS minDept,
MAX(salary) OVER(PARTITION BY dept_num) AS maxDept
FROM employee_contract
ORDER BY dept_num, name;
dept_num name salary row_cnt
d1 user1 1000 3
d1 user2 2000 3
d1 user3 3000 3
d2 user4 4000 2
d2 user5 5000 2
```

## 总结 (Assignment)

- 掌握hive视图的使用
- 如何使用 lateral view
- 掌握hive的数据join方式
- 完成Hive中的数据排序
- 完成窗口案例的实践操作

