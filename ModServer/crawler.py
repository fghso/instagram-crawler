#!/usr/bin/python
# -*- coding: iso-8859-1 -*-

import os
from instagram.client import InstagramAPI
from datetime import datetime, timedelta
import logging, json, random, sys, time
from mysql.connector.errors import Error
from mysql.connector import errorcode
import mysql.connector
import calendar
import socket


'''
OBS: salvar quem gerou o user para no caso de ser privado
conseguir pegar o username dps!
'''


class Crawler:
    # Retorna o nome que identifica o coletor
    def getName(self):
        #return os.getpid()
        return socket.gethostname()

    def crawl(self, resourceID, noLogging, **params):

        api_user = params["application"]["@name"]
        uid = int(resourceID)

    # CONFS
        max_time = datetime(2014, 10, 1, 12, 00)
        min_time = datetime(2014, 6, 1, 12, 00)
        #min_ts = min_time.strftime('%s')
        #max_ts = max_time.strftime('%s')
        min_ts = calendar.timegm(min_time.timetuple())
        max_ts = calendar.timegm(max_time.timetuple())
        
        user_path =     'data/users/'
        feed_path =     'data/feeds/'
        comment_path =  'data/comments/'
        log_path =      'data/logs/'
        
        if not os.path.exists(user_path): os.makedirs(user_path)
        if not os.path.exists(feed_path): os.makedirs(feed_path)
        if not os.path.exists(comment_path): os.makedirs(comment_path)
        if not os.path.exists(log_path): os.makedirs(log_path)

    # API object
        api = InstagramAPI(client_id=params["application"]["clientid"],\
                client_secret=params["application"]["clientsecret"])

    # Start logging
        logging.basicConfig(format='%(asctime)s %(levelname)s: %(message)s',\
                datefmt="%d/%m/%Y %H:%M:%S",\
                filename='%s/%d_%s.log'%(log_path, uid, api_user),\
                filemode='w', level=logging.DEBUG)

        logging.info('Starting to crawl Uid [%d] on api_user [%s].'%(uid, api_user))


    # Crawling Uid
        idx = 0
        retry = 20
        while idx < retry:
            try:
                user = api.user(uid, return_json=True)
                logging.debug('Uid [%d] crawled successfully.'%(uid))
                break
            except Exception ,e:
                logging.info('Exception crawling Uid [%d]: [%s].'%(uid, str(e)))
                try:
                    if e.status_code == '429': # API rate remaining = 0
                        time.sleep(300)
                    elif e.error_type == 'APINotAllowedError': # PRIVATE USER
                        logging.info('Uid [%d] is private, returning.'%(uid))
                        return (-2,0) # ERROR
                    elif e.error_type == 'APINotFoundError': # DELETED USER USER
                        logging.info('Uid [%d] has been deleted, returning.'%(uid))
                        return (-3,0) # ERROR

                except Exception, ex:
                    logging.error('Exception++ crawling Uid [%d]: [%s].'%(uid, str(ex)))
            idx += 1
        if idx >= retry: 
            logging.critical('MAX RETRY on user crawling Uid [%d]'%(uid))
            raise

    # Saving Uid
        with open('%s%d.info'%(user_path, uid), 'w') as user_output:
            user_output.write(json.dumps(user))
        logging.info('Finished crawling Uid [%d], reqs [%d].'%(uid, idx))


    # Crawling feed
        logging.info('Started crawling Uid [%d] feed.'%(uid))
        feed_outfile = open('%s%d.feed'%(feed_path, uid), 'w')
        feed_outfile.write('[')

        nxt = ''
        feed = []
        retry = 20
        feed_round = 0
        while nxt is not None:
            idx = 0
            while idx < retry:
                try:
                    nxt = nxt.split('&')
                    maxID = ''
                    if len(nxt) > 1: maxID = nxt[2].split('=')[1]
                    medias, nxt = api.user_recent_media(user_id=uid,return_json=True,\
                            min_timestamp = min_ts, count = 35, max_id = maxID);
                    feed += [json.dumps(media) for media in medias]
                    logging.debug('Uid [%d], feed [%d], len [%d] crawled successfully.'%\
                            (uid, feed_round, len(medias)))
                    feed_round += 1
                    break
                except Exception, e:
                    logging.info('Exception crawling Uid [%d], feed [%d]: [%s].'\
                            %(uid, feed_round, str(e)))
                    try:
                        if e.status_code == '429': # API rate remaining = 0
                                time.sleep(300)
                    except Exception, ex:
                        logging.error('Exception++ crawling Uid [%d], feed [%d]: [%s].'\
                                %(uid, feed_round, str(ex)))
                idx += 1
            if idx >= retry:
                logging.critical('MAX RETRY on feed [%d], crawling Uid [%d].'%(feed_round, uid))
                raise


    # Saving feeds
        feed_outfile.write(',\n'.join(feed)+']')
        feed_outfile.close()
        logging.info('Finished crawling Uid [%d] feed.'%(uid))


    # Crawling comments
        medias = [json.loads(x)['id'] for x in feed]
        logging.info('Started crawling Uid [%d] medias comments, len=[%d]'%(uid,len(medias)))

        retry = 20
        all_comments = []
        for i, mid in enumerate(medias):
            comment_outfile = open('%s%s.comments'%(comment_path, mid), 'w')
            comment_outfile.write('[')
            logging.info("Crawling comments from Mid[%s], i (%d)."%(str(mid), i))
            comments = []
            idx = 0
            while idx < retry:
                try:
                    cmts = api.media_comments(media_id=mid,return_json=True)
                    # waiting API suport for pagination..
                    comments += [json.dumps(x) for x in cmts]
                    all_comments += comments
                    logging.debug('Uid [%d], Mid [%s], len[%d] comments crawled successfully.'%\
                            (uid, mid, len(cmts)))
                    break
                except Exception, e:
                    logging.info('Exception crawling Uid [%d], Mid [%s]: [%s].'\
                            %(uid, mid, str(e)))
                    try:
                        if e.status_code == '429': # API rate remaining = 0
                                time.sleep(300)
                    except Exception, ex:
                        logging.error('Exception++ crawling Uid [%d], Mid [%s]: [%s].'\
                                %(uid, mid, str(ex)))
                idx += 1

            if idx >= retry:
                logging.critical('MAX RETRY comments crawling, mid [%s], i [%d]'%(mid, i))
                raise

            # Saving comments
            comment_outfile.write(',\n'.join(comments)+']')
            comment_outfile.close()
        
        
    # Dumping on DB
        if (feed):
            logging.info('Dumping Uids on database from Uid_base=%d, len=[%d]'%(uid, len(all_comments)))
            mysqlConnection = mysql.connector.connect(user=params['database']['user'],\
                password=params['database']['password'], host=params['database']['host'],\
                database=params['database']['name'])
            cursor = mysqlConnection.cursor()

            for comment in all_comments:
                commentDict = json.loads(comment)
                who = commentDict['from']
                userid = who['id']
                username = who['username']
                '''
                query = "INSERT INTO `%s` (resource_id,annotation) VALUES (%s,'%s') ON DUPLICATE KEY UPDATE resources_pk=resources_pk;"%\
                                            (params['database']['table'],userid, username)
                try:
                    cursor.execute(query)
                except Exception, e:
                    logging.error('Error while executing query [%s]: [%s]'%(query,str(e)))
                    raise
                '''
                
                query = 'insert INTO ' + params['database']['table'] + ' (resource_id, annotation) values (%s,%s)'
                try:
                    cursor.execute(query, (uid, username))
                except mysql.connector.Error as err:
                    if err.errno != errorcode.ER_DUP_ENTRY:
                        logging.error('Error while inserting user into database: [%s]'%(str(err.msg)))
                        raise

            try:    
                mysqlConnection.commit()
            except Exception, e:
                logging.error('Error while dumping users to database: [%s]'%(str(e)))
                raise

            logging.info('[DONE] Dumping Uids on database from Uid_base=%s'%(uid))
        return (2,0)
