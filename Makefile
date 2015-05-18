CC = gcc 
CFLAGS +=
LDLIBS = -lpthread

targets = index
objects = mapreduce.o

.PHONY : default
default : all

.PHONY : all
all : $(targets)

index : mapreduce.o 
	$(CC) $(OPT) -o $@ $^ $(LDLIBS)

%.o : %.c uthash.h
	$(CC) -c $(CFLAGS) $<

.PHONY : clean
clean:
	rm -f $(targets) $(objects) *~
