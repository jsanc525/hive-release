set hive.explain.user=false;
set hive.auto.convert.join=true;
set hive.auto.convert.join.noconditionaltask=true;
set hive.auto.convert.join.noconditionaltask.size=10000;

CREATE TABLE srcbucket_mapjoin(key int, value string) partitioned by (ds string) CLUSTERED BY (key) INTO 2 BUCKETS STORED AS TEXTFILE;
CREATE TABLE tab_part (key int, value string) PARTITIONED BY(ds STRING) CLUSTERED BY (key) INTO 4 BUCKETS STORED AS TEXTFILE;
CREATE TABLE srcbucket_mapjoin_part (key int, value string) partitioned by (ds string) CLUSTERED BY (key) INTO 4 BUCKETS STORED AS TEXTFILE;

load data local inpath '../../data/files/srcbucket20.txt' INTO TABLE srcbucket_mapjoin partition(ds='2008-04-08');
load data local inpath '../../data/files/srcbucket22.txt' INTO TABLE srcbucket_mapjoin partition(ds='2008-04-08');

load data local inpath '../../data/files/srcbucket20.txt' INTO TABLE srcbucket_mapjoin_part partition(ds='2008-04-08');
load data local inpath '../../data/files/srcbucket21.txt' INTO TABLE srcbucket_mapjoin_part partition(ds='2008-04-08');
load data local inpath '../../data/files/srcbucket22.txt' INTO TABLE srcbucket_mapjoin_part partition(ds='2008-04-08');
load data local inpath '../../data/files/srcbucket23.txt' INTO TABLE srcbucket_mapjoin_part partition(ds='2008-04-08');

set hive.enforce.bucketing=true;
set hive.enforce.sorting = true;
set hive.optimize.bucketingsorting=false;
insert overwrite table tab_part partition (ds='2008-04-08')
select key,value from srcbucket_mapjoin_part;

CREATE TABLE tab(key int, value string) PARTITIONED BY(ds STRING) CLUSTERED BY (key) INTO 2 BUCKETS STORED AS TEXTFILE;
insert overwrite table tab partition (ds='2008-04-08')
select key,value from srcbucket_mapjoin;

set hive.convert.join.bucket.mapjoin.tez = true;

explain select a.key, b.key from tab_part a join tab_part c on a.key = c.key join tab_part b on a.value = b.value;

CREATE TABLE tab1(key int, value string) CLUSTERED BY (key) INTO 2 BUCKETS STORED AS TEXTFILE;
insert overwrite table tab1
select key,value from srcbucket_mapjoin;

explain
select a.key, a.value, b.value
from tab1 a join src b on a.key = b.key;

explain
select a.key, b.key from (select key from tab_part where key > 1) a join (select key from tab_part where key > 2) b on a.key = b.key;

explain
select a.key, b.key from (select key from tab_part where key > 1) a left outer join (select key from tab_part where key > 2) b on a.key = b.key;

explain
select a.key, b.key from (select key from tab_part where key > 1) a right outer join (select key from tab_part where key > 2) b on a.key = b.key;

explain select a.key, b.key from (select distinct key from tab) a join tab b on b.key = a.key;

explain select a.value, b.value from (select distinct value from tab) a join tab b on b.key = a.value;


--HIVE-17939
create table small (i int) stored as ORC;
create table big (i int) partitioned by (k int) clustered by (i) into 10 buckets stored as ORC;

insert into small values (1),(2),(3),(4),(5),(6);
insert into big partition(k=1) values(1),(3),(5),(7),(9);
insert into big partition(k=2) values(0),(2),(4),(6),(8);
explain select small.i, big.i from small,big where small.i=big.i;
select small.i, big.i from small,big where small.i=big.i order by small.i, big.i;
