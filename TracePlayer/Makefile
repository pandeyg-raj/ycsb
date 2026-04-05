CC      = gcc
CFLAGS  = -O2 -Wall -Wextra -std=c11 -D_POSIX_C_SOURCE=200809L
LIBS    = -lcassandra -lpthread -lm

TARGET  = trace_driver
SRC     = trace_driver.c

.PHONY: all clean install-deps

all: $(TARGET)

$(TARGET): $(SRC)
	$(CC) $(CFLAGS) -o $@ $< $(LIBS)

# Install DataStax C driver on Ubuntu/Debian
install-deps:
	apt-get update
	apt-get install -y libcassandra-dev libssl-dev libuv1-dev cmake
	@echo "DataStax C driver installed."

clean:
	rm -f $(TARGET)
