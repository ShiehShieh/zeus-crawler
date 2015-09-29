#!/usr/bin/env python
# -*- coding: utf-8 -*-


import re
import json
import string
import HTMLParser
import pymysql
import pymysql.cursors

conn = pymysql.connect(host='localhost',
                             user='root',
                             password='',
                             db='test',
                             charset='utf8mb4',
                             cursorclass=pymysql.cursors.DictCursor)


def update_title(id_title, cur):
    """TODO: Docstring for update_title.
    :returns: TODO

    """
    sql = "INSERT INTO `id_title` (`id`, `title`) VALUES (%s, %s)"
    cur.execute(sql, (id_title['id'], id_title['title']))


def count_title(cursor):
    """TODO: Docstring for count_title.
    :returns: TODO

    """
    sql = "SELECT COUNT(DISTINCT `title`) FROM `id_title`"
    cursor.execute(sql, ())
    unique_title = cursor.fetchone()
    return unique_title['COUNT(DISTINCT `title`)']


def filter_str(strs, regex):
    """TODO: Docstring for filter_str.

    :arg1: TODO
    :returns: TODO

    """
    # tmp = strs.lower()
    # strs = re.sub(r'(\ \(.*vol.*\))|(\ volume.*\d+)|(\ \(.*volumn.*\))|(\d+$)|(\ disc.*\d+)|(\ -\ season\ \d+.*)|((,|:|-)\ (set|disc|vol|volume|book|box\ set).*\d+.*)', r'', strs)
    tmp = re.sub(r'\ +', ' ', strs.lower())


    tmp = re.sub(r'(\ \[vhs\]*.*)|(\ \[blu-ray\]*.*)|(\ \[.*(dvd|ultraviolet|bluray).*\]*)|(\ \(.*(dvd|ultraviolet|bluray).*\))|(\ \(.*pal.*\))|(\ \(.*disc\ set.*\))|(\ \(dk\ eyewitness\ dvd\))|(\ \(.*edition.*\))|(\ \(.*box\ set.*\))|(\ \(director\'s\ cut\))|(\ -\ the\ director\'s\ cut)|(\ dvd$)|(\ -\ dvd$)|(\ vhs$)|(\ \(widescreen\))|(/dvd/.*)|(\ dvd\ collection)|(\ \(.*collection.*\))|(\ \(collector\'s\ series\))|(/collectors\ edition.*)|((,|:|-)\ (edition).*\d+.*)|(\ \dth.*)|(^the$)|(\(.*\ *bd\ *.*\))|(\ \(movie\ 3\))|(-)|(\'s)|(\ +$)', r'', tmp)
    tmp = re.sub(r'(\ \[vhs\]*.*)|(\ \[blu-ray\]*.*)|(\ \[.*(dvd|ultraviolet|bluray).*\]*)|(\ \(.*(dvd|ultraviolet|bluray).*\))|(-)|(\'s)|(:)|(,)|(\ +$)', r'', tmp)


    # tmp = regex.sub(' ', tmp)
    tmp = re.sub(r'\ +', ' ', tmp)
    print tmp
    if len(tmp) == 0:
        return strs
    else:
        return tmp


def get_unique(cursor):
    """TODO: Docstring for filter_title.
    :returns: TODO

    """
    h = HTMLParser.HTMLParser()
    regex = re.compile('[%s]' % re.escape(string.punctuation))
    sql = "SELECT DISTINCT `title` FROM `id_title`"
    cursor.execute(sql, ())
    unique_title = cursor.fetchall()
    uni = set((filter_str(h.unescape(ii['title']), regex) for ii in unique_title))

    return uni


def main():
    """TODO: Docstring for main.
    :returns: TODO

    """
    # with open('titles.json', 'r') as tmp, conn.cursor() as cur:
    #     obj = json.load(tmp)
    #     for o in obj:
    #         update_title(o, cur)
    #     conn.commit()
    try:
        with conn.cursor() as ucursor:
            # unique_title = count_title(ucursor)
            # print 'unique_title: %d' % (unique_title)
            print len(get_unique(ucursor))
    finally:
        conn.close()


if __name__ == "__main__":
    main()
