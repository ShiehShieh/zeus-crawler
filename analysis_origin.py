#!/usr/bin/env python
# -*- coding: utf-8 -*-


import time
import pymysql
import pymysql.cursors
from bs4 import BeautifulSoup


conn = pymysql.connect(host='localhost',
                             user='root',
                             password='',
                             db='test',
                             charset='utf8mb4',
                             cursorclass=pymysql.cursors.DictCursor)


def process_entry(entry, cursor):
    """TODO: Docstring for process_entry.
    :returns: TODO

    """
    title = get_title(entry['content'])
    sql = "UPDATE `web` SET `title`=%s WHERE `id`=%s"
    cursor.execute(sql, (title, entry['id']))
    # print 'updating... %s' % (entry['id'])
    # conn.commit()


def get_title(content):
    """TODO: Docstring for get_title.
    :returns: TODO

    """
    title1 = BeautifulSoup(content, "html.parser").find(id='productTitle')
    if title1:
        return title1.get_text()

    title2 = BeautifulSoup(content, "html.parser").find(id='aiv-content-title')
    if title2:
        return title2.get_text()

    return ''


def count_title(cursor):
    """TODO: Docstring for count_title.
    :returns: TODO

    """
    sql = "SELECT COUNT(DISTINCT `title`) FROM `web` WHERE `status`=%s AND `title`!=''"
    cursor.execute(sql, (1,))
    unique_title = cursor.fetchone()
    return unique_title['COUNT(DISTINCT `title`)']


def count_missing_title_id(cursor):
    """TODO: Docstring for count_missing_title_id.
    :returns: TODO

    """
    sql = "SELECT COUNT(DISTINCT `id`) FROM `web` WHERE `status`=%s AND `title`=''"
    cursor.execute(sql, (1,))
    unique_id = cursor.fetchone()
    return unique_id['COUNT(DISTINCT `id`)']


def fiill_queue(qcursor, qsize):
    """TODO: Docstring for fiill_queue.
    :returns: TODO

    """
    queue, entry = [] , True
    while len(queue) < qsize and entry:
        entry = qcursor.fetchone()
        entry and queue.append(entry)
    
    return entry, queue


def main():
    """TODO: Docstring for main.
    :returns: TODO

    """
    try:
        with conn.cursor() as qcursor, conn.cursor() as ucursor:
            sql = "SELECT `id`, `content` FROM `web` WHERE `status`=%s AND `title`=''"
            qcursor.execute(sql, (1,))
            while True:
                start = time.time()
                entry, queue = fiill_queue(qcursor, 200)
                for en in queue:
                    process_entry(en, ucursor)
                conn.commit()
                end = time.time()
                print 'Time elapsed: %f, query per second: %f'\
                        % (end-start, (end-start)/200)
                if not entry:
                    break
            unique_title = count_title(ucursor)
            unique_id = count_missing_title_id(ucursor)
            print 'unique_id + unique_title: %d + %d = %d'\
                    % (unique_id, unique_title, unique_id + unique_title)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
