# Spark Join 笔记

当前SparkSQL支持三种Join算法－shuffle hash join、broadcast hash join以及sort merge join

## Hash Join

1、 确定Build Table以及Probe Table：这个概念比较重要，Build Table使用join key构建Hash Table，而Probe Table使用join key进行探测，探测成功就可以join在一起。通常情况下，小表会作为Build Table，大表作为Probe Table。

2、 构建Hash Table：依次读取Build Table（item）的数据，对于每一行数据根据join key（item.id）进行hash，hash到对应的Bucket，生成hash table中的一条记录。

3、探测：再依次扫描Probe Table（order）的数据，使用相同的hash函数映射Hash Table中的记录，映射成功之后再检查join条件（item.id = order.i_id），如果匹配成功就可以将两者join在一起。

基本流程可以参考上图，这里有两个小问题需要关注：
1、 hash join性能如何？很显然，hash join基本都只扫描两表一次，可以认为o(a+b)，较之最极端的笛卡尔集运算a*b，不知甩了多少条街。
2、为什么Build Table选择小表？道理很简单，因为构建的Hash Table最好能全部加载在内存，效率最高；这也决定了hash join算法只适合至少一个小表的join场景，对于两个大表的join场景并不适用；
上文说过，hash join是传统数据库中的单机join算法，在分布式环境下需要经过一定的分布式改造，说到底就是尽可能利用分布式计算资源进行并行化计算，提高总体效率。hash join分布式改造一般有两种经典方案：
1、broadcast hash join：
    将其中一张小表广播分发到另一张大表所在的分区节点上，分别并发地与其上的分区记录进行hash join。broadcast适用于小表很小，可以直接广播的场景。
2、shuffle hash join：
    一旦小表数据量较大，此时就不再适合进行广播分发。这种情况下，可以根据join key相同必然分区相同的原理，将两张表分别按照join key进行重新组织分区，这样就可以将join分而治之，划分为很多小join，充分利用集群资源并行化。（相当于在map端将大小按照key进行拆分重新组织分区，然后根据key分发到reduce端进行分别大小表的处理，最终再将结果进行汇总。）

### Broadcast Hash Join

1、broadcast阶段：将小表广播分发到大表所在的所有主机。广播算法可以有很多，最简单的是先发给driver，driver再统一分发给所有executor；要不就是基于bittorrete的p2p思路；
基于bittorrete的p2p思路可参考：
https://zhidao.baidu.com/question/9782615.html
https://baike.baidu.com/item/BitTorrent/142795?fr=aladdin

2、hash join阶段：在每个executor上执行单机版hash join，小表映射，大表试探；


### Shuffle Hash Join
在大数据条件下如果一张表很小，执行join操作最优的选择无疑是broadcast hash join，效率最高。但是一旦小表数据量增大，广播所需内存、带宽等资源必然就会太大，broadcast hash join就不再是最优方案。此时可以按照join key进行分区，根据key相同必然分区相同的原理，就可以将大表join分而治之，划分为很多小表的join，充分利用集群资源并行化。如下图所示，shuffle hash join也可以分为两步：
1、shuffle阶段：分别将两个表按照join key进行分区，将相同join key的记录重分布到同一节点，两张表的数据会被重分布到集群中所有节点。这个过程称为shuffle
2、hash join阶段：每个分区节点上的数据单独执行单机hash join算法。（最后应该还要做一个union all的操作将之前处理的内容进行合并）


### Sort-Merge Join
1、shuffle阶段：将两张大表根据join key进行重新分区，两张表数据会分布到整个集群，以便分布式并行处理
2、sort阶段：对单个分区节点的两表数据，分别进行排序

3、merge阶段：对排好序的两张分区表数据执行join操作。join操作很简单，分别遍历两个有序序列，碰到相同join key就merge输出，否则取更小一边（两张分区表进行join的过程中，会不断的比较索引的大小，一直以索较小的索引值遍历分区表数据。），见下图示意：
![merge](https://img-blog.csdnimg.cn/20200402213923158.png?x-oss-process=image/watermark,type_ZmFuZ3poZW5naGVpdGk,shadow_10,text_aHR0cHM6Ly9ibG9nLmNzZG4ubmV0L3FxXzE2NjMzNDA1,size_16,color_FFFFFF,t_70)


仔细分析的话会发现，sort-merge join的代价并不比shuffle hash join小，反而是多了很多。那为什么SparkSQL还会在两张大表的场景下选择使用sort-merge join算法呢？这和Spark的shuffle实现有关，目前spark的shuffle实现都适用sort-based shuffle算法，因此在经过shuffle之后partition数据都是按照key排序的。因此理论上可以认为数据经过shuffle之后是不需要sort的，可以直接merge（也就是说sort-merge-join实际只需要执行shuffle和merge阶段，而shuffle-hash-join需要执行shuffle和hash-join阶段。而对于大表join大表来说，merge阶段比hash-join阶段更优！
为什么更优：hash-join的复杂度O(a+b)；而merge小于O(a+b)。a,b代表数组的长度）。
