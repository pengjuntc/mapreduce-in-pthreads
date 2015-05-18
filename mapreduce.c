/**************

This program is written by Jun Peng, which is a program assignment of parallel programming. The target of the program is to simulate MapReduce in pthreads. In a nutshell, there are m Map threads and n Reduce threads which are specified by the user. Then map thread i read file file(i).txt line by line. Assume that each file contains exactly one word per line. SO each map thread read its corresponding file. Then each map thread hashes the word and compute an integer from 1 to n (the number of reduce threads). Then map thread sends the word and its file name and line number to the reduce thread. Reduce thread gets the word and store it into a hashtable. SO this is a typical producer and consumer problem in multithread programming.

 *************/



#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <pthread.h>
#include <assert.h>
#include "uthash.h"   //credit to Troy D. Hanson http://troydhanson.github.io/uthash/userguide.html


//Macros defined here 
#define MAXLEN 50
#define SIZE 10     // the size of bounded buffer
#define ALLOCSIZE 100


//struct to store word and its corresponding file name and line num
typedef struct Word {
        char word[MAXLEN];
        char filename[MAXLEN];
        int linenum;
} Word;


//struct to be used in hashtable  "uthash.h"
struct my_struct {
        char name[50];    //key
        char *value;      //value
        UT_hash_handle hh;  //hashtable handler
};


//global varaibles which are shared by threads
int NP, NC;   // number of producers, number of consumers
Word **buffer; 
int *end;     // the end position of bounded buffer
int *start;   // the start position of bounded buffer
int *count;   // the count of bounded buffer
int signal=0; //used to indicate if all producers terminate.



void tolowercase(char *p) {
        while(*p != '\0') {
                *p ++ = tolower(*p);
        }
}

void trimString(char *p) {
        while((*p >= 'a' && *p <= 'z') || (*p >= '0' && *p <= '9')) {
                p++;
        }
        *p = '\0';
}

//hash function is taken from webpage: http://www.cse.yorku.ca/~oz/hash.html
unsigned long hash(unsigned char *str) {
        unsigned long hash = 5381;
        int c;

        while (c = *str++) {
                hash = ((hash << 5) + hash) + c; /* hash * 33 + c */
        }
        return hash;
}

//put the Word w into buffer[num]
void put(Word w, int num) {
        buffer[num][end[num]] = w;
        end[num] = (end[num] + 1) % SIZE;
        count[num] ++;
}

//get the current word from buffer[num]
Word get(int num) {
        Word w = buffer[num][start[num]];
        start[num] = (start[num] + 1) % SIZE;
        count[num] --;
        return w;
}


//all mutex and conditional variable are defined here
pthread_cond_t *empty, *fill;
pthread_mutex_t *mutex;
pthread_mutex_t printmutex;


//producer thread
void *producer(void *arg) {
        char *filename = (char *)arg;
        FILE *fp;
        if((fp = fopen(filename, "r")) == NULL) {
                printf("no such file:%s\n", filename);
                return;
        }
        
        char line[MAXLEN];
        int linenum = 0;
        while(fgets(line, MAXLEN, fp) != NULL) {
     
                linenum++;
                tolowercase(line);
                trimString(line);
                if(strlen(line) == 0) {  //if line is empty string like space, skip it
                        continue;         
                }

                int cnum = hash(line) % NC; //hash the line to the corresponding consumer
                Word w;
                strcpy(w.word, line);
                w.linenum = linenum;
                strcpy(w.filename, filename);

                
                //producer pattern in multithreads
                pthread_mutex_lock(&mutex[cnum]);
                while(count[cnum] == SIZE) {
                        pthread_cond_wait(&empty[cnum], &mutex[cnum]);
                }
                put(w, cnum);
                pthread_cond_signal(&fill[cnum]);
                pthread_mutex_unlock(&mutex[cnum]);
                //end of producer pattern
        }

        fclose(fp);
        return NULL;
}

//consumer thread
void *consumer(void *arg) {
        int num = (int)arg;
        int flag = 0;
        struct my_struct *s, *tmp, *users = NULL;  // for hashtable to use 
        while(!signal) {


                //consumer pattern in multithreads
                pthread_mutex_lock(&mutex[num]);
                while(count[num] == 0) {
                        pthread_cond_wait(&fill[num], &mutex[num]);
                        if(signal) {  //if all producers terminates and count is 0, the consumer should be waked up
                                flag = 1;
                                pthread_mutex_unlock(&mutex[num]);
                                break;
                        }
                }
                if(flag)
                        break;
                Word w = get(num);
                pthread_cond_signal(&empty[num]);
                pthread_mutex_unlock(&mutex[num]);
                //end of consumer pattern
                


                //store w into hashtable
                HASH_FIND_STR(users, w.word, s);
                if(s) {
                        char *str = (char *)malloc(sizeof(char) * (strlen(s->value) + ALLOCSIZE));
                        sprintf(str, "%s, (%s: %d)", s->value, w.filename, w.linenum);
                        free(s->value);
                        s->value = (char *)malloc(sizeof(char) * (strlen(str) + 1));
                        strcpy(s->value, str);
                        free(str);
                } else {
                        char *str = (char *)malloc(sizeof(char) * 20);
                        s = (struct my_struct*)malloc(sizeof(struct my_struct));
                        strcpy(s->name, w.word);
                        sprintf(str, "(%s: %d)", w.filename, w.linenum);
                        s->value = (char *)malloc(sizeof(char) * (strlen(str) + 1));
                        strcpy(s->value, str);
                        free(str);
                        HASH_ADD_STR( users, name, s );
                }

        }


        //clean up if buffer still have words
        while(count[num] != 0) {
                Word w = get(num);

                HASH_FIND_STR(users, w.word, s);
                if(s) {
                        char *str = (char *)malloc(sizeof(char) * (strlen(s->value) + ALLOCSIZE));
                        sprintf(str, "%s, (%s: %d)", s->value, w.filename, w.linenum);
                        free(s->value);
                        s->value = (char *)malloc(sizeof(char) * (strlen(str)+1));
                        strcpy(s->value, str);
                        free(str);
                } else {
                        char *str = (char *)malloc(sizeof(char) * 20);
                        s = (struct my_struct*)malloc(sizeof(struct my_struct));
                        strcpy(s->name, w.word);
                        sprintf(str, "(%s: %d)", w.filename, w.linenum);
                        s->value = (char *)malloc(sizeof(char) * (strlen(str) + 1));
                        strcpy(s->value, str);
                        free(str);
                        HASH_ADD_STR( users, name, s );
                }
        }


        //iterate through hashtable, add mutex to avoid interleaving of printing.
        pthread_mutex_lock(&printmutex);
        HASH_ITER(hh, users, s, tmp) {
                printf("%s: %s\n", s->name, s->value); //print the key and value 
                HASH_DEL(users, s);
                free(s);
        }
        pthread_mutex_unlock(&printmutex);


        return NULL;
}

//initialize all global variabls
void initialization() {
        end = (int *)malloc(sizeof(int) * NC);
        start = (int *)malloc(sizeof(int) * NC);
        count = (int *)malloc(sizeof(int) * NC);
        memset(end, 0, NC);
        memset(start, 0, NC);
        memset(count, 0, NC);

        empty = (pthread_cond_t *)malloc(sizeof(pthread_cond_t) * NC);
        fill = (pthread_cond_t *)malloc(sizeof(pthread_cond_t) * NC);
        mutex = (pthread_mutex_t *)malloc(sizeof(pthread_mutex_t) * NC);
  
        
        buffer = (Word **)malloc(sizeof(Word *) * NC);
        int i;
        for(i = 0; i < NC; i++) {
                buffer[i] = (Word *)malloc(sizeof(Word) * SIZE);
                pthread_mutex_init(&mutex[i], NULL);
                pthread_cond_init(&empty[i], NULL);
                pthread_cond_init(&fill[i], NULL);
        }

        pthread_mutex_init(&printmutex, NULL);

}

//release all memory
void freeAllMemory() {
        free(empty);
        free(fill);
        free(mutex);
        free(count);
        free(start);
        free(end);
        int i;
        for(i = 0; i < NC; i++)
                free(buffer[i]);
        free(buffer);
}


int main(int argc, char *argv[]) {
        //get  -producer n -consumer m, try to use getopts func
        int opt, i;
        int rc;
        int nproducer=0, nconsumer=0;
        
        //Pass the arguments via terminal like "-p 20 -c 10"
        while((opt = getopt(argc, argv, "p:c:")) != -1) {
                switch(opt) {
                case 'p':
                        nproducer = atoi(optarg);
                        break;
                case 'c':
                        nconsumer = atoi(optarg);
                        break;
                }
        }
       
        //Or prompt for user to input
        if(nproducer == 0) {
                printf("Number of Map threads: ");
                scanf("%d", &nproducer);
        }

        if(nconsumer == 0) {
                printf("Number of Reduce threads: ");
                scanf("%d", &nconsumer);
        }

        NP = nproducer;
        NC = nconsumer;
        
        initialization();
       
        pthread_t ptid[nproducer], ctid[nconsumer];

        //spawn m produecers, m is taken from the terminal
        char str[nproducer][MAXLEN];
        for(i = 0; i < nproducer; i++) {
                sprintf(str[i], "file%d.txt", i+1);
                rc = pthread_create(&ptid[i], NULL, producer, str[i]); //producer threads
                assert(rc == 0);
        }
        
        //spawn n consumers, n is taken from the terminal
        for(i = 0; i < nconsumer; i++) {
                rc = pthread_create(&ctid[i], NULL, consumer, (void *)i); //consumer threads
                assert(rc == 0);
        }
             
        for(i = 0; i < nproducer; i++) {
                rc = pthread_join(ptid[i], NULL); //join producer threads
                assert(rc == 0);
        }
        
        signal = 1;  // set signal after all producer terminates
        
        //broadcast
        for(i = 0; i < nconsumer; i++) {
                pthread_mutex_lock(&mutex[i]);
                pthread_cond_broadcast(&fill[i]);
                pthread_mutex_unlock(&mutex[i]);
        }


        for(i = 0; i < nconsumer; i++) {
                rc = pthread_join(ctid[i], NULL); //join consumer threads
                assert(rc == 0);
        }

        //printf("EXIT NORMALLY\n");

        freeAllMemory();
        return 0;
}
