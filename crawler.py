#!/usr/bin/env python
# -*- coding: utf-8 -*-


import time
import pymysql
import requests
import pymysql.cursors
from gevent.pool import Pool
from gevent import monkey, Timeout
monkey.patch_all()


url_base = 'http://www.amazon.com/dp/'
conn = pymysql.connect(host='localhost',
                             user='root',
                             password='',
                             db='test',
                             charset='utf8mb4',
                             cursorclass=pymysql.cursors.DictCursor)
headers = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/45.0.2454.101 Safari/537.36'
        }
proxies = {
        # 'http': 'http://127.0.0.1:1080',
        # 'https': 'https://127.0.0.1:1080',
        }


def print_info(info):
    """TODO: Docstring for print_info.
    :info: TODO
    :returns: TODO
    """
    print '\033[;32m%s\033[0m' % (info)


def print_warning(warning):
    """TODO: Docstring for print_warning.
    :warning: TODO
    :returns: TODO
    """
    print '\033[;33m%s\033[0m' % (warning)


def print_error(error_type):
    """TODO: Docstring for print_error.
    :errorname: TODO
    :returns: TODO
    """
    print '\033[;31m%s\033[0m' % (error_type)


def id2content(idname):
    """TODO: Docstring for id2content.
    :returns: TODO

    """
    content = ''
    timeout = Timeout(10)
    timeout.start()
    try:
        content = requests.get(url_base+idname, headers=headers, proxies=proxies)
        status = 0
        if len(content.content) < 10000:
            print 'Blocked: %s, length of content: %d' % (idname, len(content.content))
            status = 4
    except requests.exceptions.ConnectionError:
        print_error('Caught ConnectionError@' + idname)
        status = 1
    except Timeout:
        print_warning('Greenlet Timeout@' + idname)
        status = 2
    except requests.exceptions.ReadTimeout:
        print_warning('Caught ReadTimeout@' + idname)
        status = 3
    finally:
        timeout.cancel()
        return idname, content, status


def flush2db(datum, cur):
    """TODO: Docstring for flush2db.
    :returns: TODO

    """
    counter = 0
    status_map = {
            0: 'successful',
            1: 'ConnectionError',
            2: 'G Timeout',
            3: 'R Timeout',
            4: 'Blocked',
            }
    print 'flushing...'
    for idname, content, status in datum:
        if status == 0:
            sql = "UPDATE `web` SET `content`=%s, `status`=%s WHERE `id`=%s"
            cur.execute(sql, (content.content, 1, idname))
            conn.commit()
            counter = counter + 1
        else:
            print 'download failure: %s: %s' % (idname, status_map[status])

    return counter


def buileup():
    """TODO: Docstring for buileup.
    :returns: TODO

    """
    with conn.cursor() as cursor:
        with open('filtered_sort_uniq.txt', 'r') as fp:
            for idname in fp:
                sql = "INSERT INTO `web` (`id`, `content`, `status`, `title`) VALUES (%s, %s, %s, %s)"
                cursor.execute(sql, (idname, '', 0, ''))
                conn.commit()



def fiill_queue(fp, qsize, cur):
    """TODO: Docstring for fiill_queue.
    :returns: TODO

    """
    queue, idname = [] , 'begin'
    while len(queue) < qsize and idname:
        idname = fp.readline()
        sql = "SELECT `status` FROM `web` WHERE `id`=%s"
        cur.execute(sql, (idname,))
        if cur.fetchone()['status'] == 1:
            print '%s have been already downloaded' % (idname)
            continue
        idname and queue.append(idname)

    return idname, queue


def main():
    """TODO: Docstring for main.

    :arg1: TODO
    :returns: TODO

    """
    greenlet_pool = Pool(22)

    try:
        # buileup()
        # return
        with open('filtered_sort_uniq.txt', 'r') as fp, conn.cursor() as cur:
            while True:
                start = time.time()
                idname, queue = fiill_queue(fp, 200, cur)
                datum = greenlet_pool.map(id2content, queue)
                greenlet_pool.join()
                counter = flush2db(datum, cur)
                end = time.time()
                print 'Time elapsed: %f, page per second: %f'\
                        % (end-start, (end-start)/counter)
                if not idname:
                    break
    finally:
        conn.close()


if __name__ == "__main__":
    main()
