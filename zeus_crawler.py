#!/usr/bin/env python
# encoding: utf-8

##
# @file zeus_crawler.py
# @brief Crawler
# @author 谢志杰/Shieh
# @version v1.0.0.0
# @date 2014-10-07


import os
import time
import requests
import optparse
from os.path import splitext, dirname, isdir, exists
from functools import wraps
from urlparse import urlparse, urljoin
from bs4 import BeautifulSoup
from gevent.pool import Pool
from gevent import monkey, Timeout
monkey.patch_all()


VERSION = 'v1.0.0'
USAGE = "usage: %prog [options] arg1 arg2"


def get_options():
    """TODO: Docstring for get_options.
    :returns: TODO

    """
    parser = optparse.OptionParser(usage=USAGE, version=VERSION)

    parser.add_option('-m', '--main', action='store', type='string',
                      help='Main Page.', dest='url')
    parser.add_option('-p', '--pool', action='store', type='int',
                      help='Size of pool.', dest='pool')
    parser.add_option('-t', '--test', action='store_true', dest='test',
                      help='Test the difference between sync and async.')
    parser.add_option('-v', '--verbose', action='store_true', dest='verbose',
                      help='Output verbosely.')

    return parser.parse_args()


def log_time(fn):
    """TODO: Docstring for log_time.

    :fn: TODO
    :returns: TODO

    """
    @wraps(fn)
    def wrapper(*args, **kwargs):
        start_time = time.time()

        result = fn(*args, **kwargs)

        end_time = time.time()
        print_info('Time used : {0}'.format(end_time - start_time))

        return result

    return wrapper


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


def download_url(url):
    """TODO: Docstring for download_url.

    :url: TODO
    :returns: TODO

    """
    url_data = requests.get(url)

    print '%s has been downloaded.' % (url)

    return url_data


@log_time
def synchronous_download():
    """TODO: Docstring for synchronous_download.
    :returns: TODO

    """
    with open('url_list.txt', 'r') as url_list:
        for url in url_list:
            download_url(url.strip('\n'))

    return True


@log_time
def asynchronous_download():
    """TODO: Docstring for asynchronous_download.
    :returns: TODO

    """
    greenlets = Pool(20)
    with open('url_list.txt', 'r') as url_list:
        greenlets.map(download_url, [x.strip('\n') for x in url_list])

    return True


def url_to_path(url, default='index.html'):
    """TODO: Docstring for url_to_path.

    :url: TODO
    :returns: TODO

    """
    parsed_url = urlparse(url, scheme='http')
    path = parsed_url[1] + parsed_url[2]
    ext = splitext(path)

    if ext[1] == '':
        if path[-1] == '/':
            path += default
        else:
            path += '/' + default

    if os.sep != '/':
        path = path.replace('/', os.sep)

    return path


def is_file_exists(url, default='index.html'):
    """TODO: Docstring for is_exists.

    :url: TODO
    :returns: TODO

    """
    path = url_to_path(url, default)

    return os.path.exists(path)


def make_file(url, default='index.html'):
    """TODO: Docstring for make_file.

    :url: TODO
    :returns: TODO

    """
    path = url_to_path(url, default)
    ldir = dirname(path)

    if not isdir(ldir):
        exists(ldir) and os.unlink(ldir)
        os.makedirs(ldir)

    return path


def complete_url(curr_path, subdomain):
    """TODO: Docstring for complete_url.

    :domain: TODO
    :subdomain: TODO
    :returns: TODO

    """
    if subdomain is None or \
            urlparse(curr_path).netloc == urlparse(subdomain).netloc:
        url = subdomain
    elif urlparse(subdomain).netloc == '':
        url = urljoin(curr_path, urlparse(subdomain).path)
    else:
        url = None

    return url


def get_whole_website(url, greenlet_pool):
    """TODO: Docstring for get_whole_website.

    :url: TODO
    :returns: TODO

    """
    Timeout(120).start()
    if is_file_exists(url) or url is None:
        print_warning(url + ' already exists or invalid.')
        return True

    try:
        content = requests.get(url)
        with open(make_file(url), 'w') as temp:
            temp.write(content.text)
            print '%s has been write into file.' % (url)

        all_href = BeautifulSoup(content.text).find_all(href=True)
        links = set(map(complete_url,
                        (url for i in all_href),
                        (i.get('href') for i in all_href)))

        map(greenlet_pool.spawn,
            (get_whole_website for i in links),
            links,
            (greenlet_pool for i in links))
    except requests.exceptions.ConnectionError:
        print_error('Caught ConnectionError@' + url)
    except Timeout:
        print_warning('Greenlet Timeout@' + url)
    except requests.exceptions.ReadTimeout:
        print_warning('Caught ReadTimeout@' + url)
    finally:
        return True


@log_time
def main():
    """TODO: Docstring for main.
    :returns: TODO

    """
    (options, args) = get_options()
    greenlet_pool = Pool(500)

    options.url and get_whole_website(options.url, greenlet_pool) or \
        options.test and synchronous_download() and asynchronous_download()

    greenlet_pool.join()


if __name__ == '__main__':
    main()
