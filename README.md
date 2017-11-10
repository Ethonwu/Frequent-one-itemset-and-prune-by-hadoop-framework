# Frequent one-itemset and prune by hadoop framework

## Introduction

Our Algorithm have two jobs 

First Job is **Counting** items

Second Job is **Prune** infrequent items from Transcation list

## First Job
 
First Job splits every input transcation list by offset

First Job format is Text , IntWritable

Map -> Class TokenizerMapper 

Reduce -> Class IntSumReducer

Map site example:
	
	Input: T1 {a,b,c}

	Output(<Key, Value>): (a,1) , (b,1) , (c,1) ....

Reduce site example:

	Input(Key, Value): (a,1) , (b,1) , (c,1) ...	

	Output: (a,Sum of Values)

Store frequent items to **frequent_one_itemset** array list

## Second Job

Second Job read orginal file and read **frequent_one_itemset** to **Prune** infrequent items

Second Job format is Text , Text

Map -> Class PruneMapper

Reduce -> Class PruneReducer

Map site example:

	Input: T1 {a,b,c}

	Read frequent_one_itemset array list and compare

	Output(<Key, Value>): (Pruned transcation length,Pruned transcation)

Reduce site example:

	Input: Map Output 

	Due to Key is length from every transcation, the output would sort

	Output: (Pruned transcation , null)


