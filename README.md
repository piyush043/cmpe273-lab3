# cmpe273-lab3
Chronicle Hash Map

Purpose

The purpose of this lab is to understand how to implement a RESTful persistent key-value data store using Chronicle Map.
Steps

You will be using the same three cache servers from the lab 2. However, you must use the assignment 3’s either consistent hashing or endezvous hashing implementation on the client side for this assignment to shard the data across the below three servers:
A: http://localhost:3000
B: http://localhost:3001
C: http://localhost:3002
{Key => Value}
1 => a
2 => b
3 => c
4 => d
5 => e
6 => f
7 => g
8 => h
9 => i
10 => j
The limitation of the current implementation is the data will be lost whenever a node is goes down. To locally persist the data, you will use Chronicle Map to store the key-value hashmap into a file system. You are going to implement a new class called ChronicleMapCache using the same interface: CacheInterface and then replace the existing implementation with the new ChronicleMapCache.java. Finally, delete InMemoryCache.java from the code base.
