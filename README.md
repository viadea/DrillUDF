# DrillUDF
This Drill UDF sample code contains 2 functions: myaddints() and dup_string()

They are tested in Drill 1.4

To make Drill UDF work in Drill, please follow this blog:

http://www.openkb.info/

## a. How to build the jar

```shell
mvn package
```

##b. Test UDF in Apache Drill

```sql
>select myaddints(1.2, 3.2) from sys.version;
+---------+
| EXPR$0  |
+---------+
| 4.4     |
+---------+
1 row selected (0.409 seconds)

> select dup_string('abc',3) from sys.version;
+------------+
|   EXPR$0   |
+------------+
| abcabcabc  |
+------------+
1 row selected (2.839 seconds)
```
