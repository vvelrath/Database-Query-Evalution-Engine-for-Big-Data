Database Query Evaluation Engine for Big Data 
==============================

### Team members

Vivekanandh Vel Rathinam (vvelrath@buffalo.edu), Amitha Narasimha Murthy (amithana@buffalo.edu), 
Neeti Narayan (neetinar@buffalo.edu)

### Description

This project is, in effect, a more rigorous form of the [Simple SQL Query Evaluator](https://github.com/vvelrath/Simple-SPJUA-Query-Evaluator). 
The requirements are identical to the first version of the project: query and some data will be given which evaluates the query on the data and
give us a response as quickly as possible.

Code is evaluated on a broader range of queries selected from TPC-H benchmark, which exercises a broader range of SQL features than the Project 1 test cases did.

Second, performance constraints will be tighter than the first version, which means that this project is expected to perform more efficiently, and to handle data that does not fit into main memory.

### Blocking Operators and Memory

Blocking operators (e.g., joins other than Merge Join, the Sort operator) are generally blocking because they need to materialize instances of a relation. This project assumes that there is not have enough memory available to materialize a full relation, to say nothing of join results. To successfully process these queries, we have implemented out-of core equivalents of these operators: 

1) External Join (e.g., Hash, and Sort/Merge Join) 
2) Sort Algorithm (e.g., External Sort).

This project has been evaluated on machines with 2GB of memory, and Java is configured for a 1GB heap.

### Program execution

Run this program use the following syntax:

	java -Xmx1024m -cp build:jsqlparser.jar edu.buffalo.cse562.Main --data [data] --swap [swap] [sqlfile1] [sqlfile2] ... This example uses the following directories and files
	
	• [data]: Table data stored in '|' separated files. As before, table names match the names provided in the matching CREATE TABLE with the .dat suffix.
	• swap: A temporary directory for an individual run. This directory will be emptied after every trial.
	• [sqlfileX]: A file containing CREATE TABLE and SELECT statements, defining the schema of the dataset and the query to process