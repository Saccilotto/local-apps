# Author: Gabriele Mencagli
# Date: 13/01/2020

INCLUDE_DIR		= ../includes
OUT_DIR			= ../bin

CXX 			= g++
CXXFLAGS		= -std=c++17
INCLUDES		= -I $(INCLUDE_DIR)
MACRO           = -DFF_BOUNDED_BUFFER -DDEFAULT_BUFFER_CAPACITY=32786 -DNDEBUG -DN_CAMPAIGNS=100
OPTFLAGS		= -g -O3 -finline-functions
LDFLAGS			= -pthread

all: ysb

ysb.o: ysb.cpp
	$(CXX) $(CXXFLAGS) $(INCLUDES) $(MACRO) $(OPTFLAGS) $< -c

ysb: ysb.o
	$(CXX) ysb.o -o ../bin/ysb $(LDFLAGS)

clean:
	rm -f $(OUT_DIR)/ysb
	rm -f *.o

.DEFAULT_GOAL := all
.PHONY: all clean
