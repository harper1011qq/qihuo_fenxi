#!/usr/bin/env python
from redis_connection import RedisConnection


def read_file(redis_client):
    index = 1
    with open('baicha1.txt', 'r') as data_file:
        while True:
            line = data_file.readline()
            index += 1
            if line:
                data_elements = line.split()
                for each in data_elements:
                    print('File: \u%r' % each),
                redis_client.write_data(str(index), line)
                print('\n------------------------------------------------------------------')
            else:
                print('End of File')
                break


def main():
    redis_client = RedisConnection()
    read_file(redis_client)


if __name__ == '__main__':
    main()
