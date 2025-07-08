CC = gcc
CFLAGS = -Wall -Wextra -O2
LDFLAGS = -lpthread -lrt
TARGET = build
SRC = main.c

$(TARGET): $(SRC)
	$(CC) $(CFLAGS) -o $@ $^ $(LDFLAGS) -g

.PHONY: clean
clean:
	rm -f $(TARGET)
