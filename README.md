# bigclamSpark-distribute
This project is sparked by https://github.com/thangdnsf/BigCLAM-ApacheSpark

which also implements BigCLAM models proposed by Yang and Leskovec (2013),

I change most of the collectasmap and broadcast code into rdd join to make it more resource-efficent and robust.


I use this code to detect the communities of network which has tens of millions of nodes in my work,and it worked.


Important Notices of the code:

1.Make sure kvalue<=S.size,where S denotes the local minimal sets,and kvalue means the number of the communities
2.In Bigclam.scala,the graphpath file need to contain paires of edges in network whose lines are delimited by "\n",and whose node is delimited by "\t" like:

1\t2\n
3\t4\n

3.In Bigclam.scala,the nodeid need to be in range(0~max(num_nodes)-1),where num_nodes means the number of distinct nodes in the graph file 




