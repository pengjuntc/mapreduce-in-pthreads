#mapreduce-in-pthreads

In this folder, it contains 
   1) README.txt, which is this file. It explains the code.
   2) mapreduce.c, which is the source code.
   3) uthash.h, which is written by Troy D. Hanson (http://troydhanson.github.io/uthash/userguide.html). It provides hash table for the source code.
   4) Makefile, which builds an executable named "index".
   5) file1.txt, file2.txt... file24.txt, which are provided by instructor to test the program.
   6) output.txt, which is an exmaple output by running the source code.



In this file, I introduce the design motivation behind the source code, mainly talking the strategy to modify the producer-consumer pattern to meet our requirements for MapReduce. At the end of the file, the instruction to run the source code is given.

1.    Design motivation
The target of the program is to simulate MapReduce in pthreads. In a nutshell, there are m Map threads and n Reduce threads which are specified by the user. Then Map thread i reads file file(i).txt line by line. Assume that each file contains exactly one word per line. SO each Map thread reads its corresponding file. Then each Map thread hashes the word and computes an integer from 1 to n (the number of Reduce threads). The hash function I use is taken from webpage(http://www.cse.yorku.ca/~oz/hash.html). Then Map thread sends the word and its file name and line number (struct Word is used in the source code) to the Reduce thread. Reduce thread gets the word and stores it into a hash table. SO this is a typical producer and consumer problem in multithread programming.


---------------------Version 1----------------------- 
Typical producer pattern in multithreads programming:
//producer
for(;;) {
        lock(&mutex);
        while(count == SIZE) {
                wait(&empty, &mutex);
        }
        put(item);
        signal(&fill);
        unlock(&mutex);
}

Typical consumer pattern in multithreads programming:
//consumer
for(;;) {
        lock(&mutex);
        while(count == 0) {                
                wait(&fill, &mutex);
        }        
        item = get();
        signal(&empty);
        unlock(&mutex);
}


The above producer and consumer pattern is typically used for multiple producers and consumers sharing one bounded buffer. The producer acquires the lock and checks if the buffer is full or not. If the buffer is full, it waits for consumer consuming items. Otherwise it puts the item into the bounded buffer, signals the consumer and releases the lock. In consumer thread, the consumer acquires the lock and checks if the buffer is empty or not. If the buffer is empty, it waits for producer putting items into the buffer. Otherwise it consumes the item, signals the producer and releases the lock.



In the MapReduce, each consumer has one bounded buffer sharing with multiple producers. Producers put items to corresponding consumer buffer according to the hash of item. So the pattern above should be modified as shown below.

---------------------Version 2----------------------- 
/producer
for(;;) {
        num = hash(item)
        lock(&mutex[num]);
        while(count[num] == SIZE) {
                wait(&empty[num], &mutex[num]);
        }
        put(item, num);
        signal(&fill[num]);
        unlock(&mutex[num);
}


//consumer
for(;;) {
        lock(&mutex[num]);   //num -- consumer id
        while(count[num] == 0) {                
                wait(&fill[num], &mutex[num]);
        }        
        item = get(num);
        signal(&empty[num]);
        unlock(&mutex[num]);
}




Consider that the producer reads word from file and terminates when it reaches the EOF, the above pseudocode is modified as shown below.

---------------------Version 3----------------------- 
/producer
while(item = readline()) {
        num = hash(item)
        lock(&mutex[num]);
        while(count[num] == SIZE) {
                wait(&empty[num], &mutex[num]);
        }
        put(item, num);
        signal(&fill[num]);
        unlock(&mutex[num);
}


//consumer
for(;;) {
        lock(&mutex[num]);   //num -- consumer id
        while(count[num] == 0) {                
                wait(&fill[num], &mutex[num]);
        }
        item = get(num);
        signal(&empty[num]);
        unlock(&mutex[num]);
}



However, it is likely that the consumer threads wait forever. Considering a scenario that all producers terminate and the counts are 0, the consumer threads would just fall into the while(count == 0){} and can't be waked up by any producer since all producers ternimate. So the producers have to broadcast to all consumers that they have terminated. Thus consumers can be waked up. The modified pseudocode is shown below.

---------------------Version 4----------------------- 
/producer
while(item = readline()) {
        num = hash(item)
        lock(&mutex[num]);
        while(count[num] == SIZE) {
                wait(&empty[num], &mutex[num]);
        }
        put(item, num);
        signal(&fill[num]);
        unlock(&mutex[num);
}

//consumer
//signal is set to 0 initially
while(!signal) { 
        lock(&mutex[num]);   //num -- consumer id
        while(count[num] == 0) {                
                wait(&fill[num], &mutex[num]);
                if(signal) {
                        unlock(&mutex[num]); //release the lock
                        Exit the while(!signal) loop
                }        
        }
        item = get(num);
        signal(&empty[num]);
        unlock(&mutex[num]);
}

while(count[num] != 0) {
        item = get(num);   //clean up the buffer
}


//in main thread
if all producers terminate; then
   signal = 1
   broadcast(&fill[num]) for all num



If we add the hashtable data structure in the consumer, the below code illustrates the whole idea behind the source code (mapreduce.c).

---------------------Version 5----------------------- 
/producer
while(item = readline()) {
        num = hash(item)
        lock(&mutex[num]);
        while(count[num] == SIZE) {
                wait(&empty[num], &mutex[num]);
        }
        put(item, num);
        signal(&fill[num]);
        unlock(&mutex[num);
}

//consumer
//signal is set to 0 initially
while(!signal) { 
        lock(&mutex[num]);   //num -- consumer id
        while(count[num] == 0) {                
                wait(&fill[num], &mutex[num]);
                if(signal) {
                        unlock(&mutex[num]); //release the lock
                        Exit the while(!signal) loop
                }        
        }
        item = get(num);
        signal(&empty[num]);
        unlock(&mutex[num]);
        HASH_ADD(item);   //add item to the local hashtable
}

while(count[num] != 0) {
        item = get(num);   //clean up the buffer
        HASH_ADD(item); //add item to the local hashtable
}


//in main 
if all producers terminate; then
   signal = 1
   broadcast(&fill[num]) for all num


2.    Usage
The instruction for running the source code:
1) open terminal
2) run make, then you'll see an executable named "index"
3) you can either use command: ./index -p 24 -c 10
       24 and 10 are the number of map and reduce threads (-p for producer and -c for consumer)
   or just use command: ./index
       then the pogram will prompt for the number of map and reduce threads.
4) run command: make clean
       to delete all objects and executables

The output.txt records the output for running program with command: ./index -p 24 -c 10 > output.txt
Note that the file name for Map threads is strict. It should be named as file1.txt, file2.txt, etc.


3.    Reference
In the source code, I use hash function which is taken from http://www.cse.yorku.ca/~oz/hash.html and hash table from http://troydhanson.github.io/uthash/userguide.html. The rest of code it totally written by myself.

//END OF README
