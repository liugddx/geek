# 第一题

- 1、通过修改 SqlBase.g4 文件添加自定义命令，需要在原生 SqlBase.g4 文件中添加下面 4 处

1)

    statement
    : query    
    | SHOW VERSION                                 #showVersion

2)

    nonReserved
    
    //--DEFAULT-NON-RESERVED-START
    : ADD
    | VERSION

3)

    ### ansiNonReserved
    
    //--ANSI-NON-RESERVED-START
    : ADD
    
    | VERSION



4)

```
//============================

// Start of the keywords list

//============================

//--SPARK-KEYWORD-LIST-START

VERSION: 'VERSION';
```

- 2、执行 spark-catalyst maven module 的 anltr4:anltr4 goal

- 3、修改 SparkSqlParser.scala 文件, 重写 visitShowVersion 方法

 ```
  override def visitShowVersion(ctx: ShowVersionContext): LogicalPlan = withOrigin(ctx) {
 
 ​    ShowVersionCommand()
 
   }
 ```

- 4、在 commands.scala 类中增加实现类 ShowVersionCommand，代码如下，

  ```
  case class ShowVersionCommand() extends RunnableCommand {  override def run(sparkSession: SparkSession): Seq[Row] = {    Seq(Row(util.Properties.versionNumberString, System.getProperty("java.version")))  } }
  
  - 
  ```

- 5、在 spark 源码根目录下执行命令"./build/sbt package -DskipTests -Phive -Phive-thriftserver"
- 6、将 SPARK_HOME 环境变量设置成 spark 源码根目录，然后执行./bin/spark-sql，进入 spark-sql 控制台之后，执行 show version 命令。

# 第二题

### 第一条sql

```
select a.address 

from (

	select name, address,age 

	from customers 

	where 1="1" and age > 5

) a 

where a.age<30
```

### 第二条 sql 

```
(select a.address , a.age + (100 + 80) , Now() z 

from (

	select distinct name, age , address  

	from customers

) a 

where a.age>10 order by z)  

except 

(select a.address , a.age + (100 + 80), Now() z 

from (

	select distinct name, age , address  

	from customers

) a 

where a.name="saya");
```

