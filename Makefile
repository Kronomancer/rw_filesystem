OS=$(shell uname -s)
ifeq ("$(OS)", "Darwin")
	OS_DEF=-DMACOSX -D__FreeBSD__=10
	FUSE_LINK=-lfuse
else
	OS_DEF=-DLINUX
	FUSE_LINK=-lfuse -lpthread
endif

CFLAGS=-Wall -Werror -DDEBUG -g -D_FILE_OFFSET_BITS=64 $(OS_DEF)
CXXFLAGS=$(CFLAGS)

all: hello_cpe453fs cpe453fs

cpe453fs: cpe453fs_main.o implementation.o
	$(CXX) $(CXXFLAGS) cpe453fs_main.o implementation.o -o $@ $(FUSE_LINK)

hello_cpe453fs: cpe453fs_main.o hello_fs.o
	$(CXX) $(CXXFLAGS) cpe453fs_main.o hello_fs.o -o $@ $(FUSE_LINK)

cpe453fs_main.o: cpe453fs_main.c cpe453fs.h
hello_fs.o: hello_fs.cpp cpe453fs.h
implementation.o: implementation.c cpe453fs.h

clean:
	rm -f cpe453fs_main.o implementation.o hello_fs.o cpe453fs hello_cpe453fs
