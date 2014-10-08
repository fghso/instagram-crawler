# -*- coding: utf-8 -*-
from instagram.client import InstagramAPI
from datetime import datetime, timedelta
import logging, json, random, sys, time
import mysql.connector
import app

'''
OBS: salvar quem gerou o user para no caso de ser privado
conseguir pegar o username dps!
'''


class Crawler:
    # Retorna o nome que identifica o coletor
    def getName(self):
        return os.getpid()

    def crawl(self, resourceID, noLogging, **params):

# INPUTS
    if len(sys.argv) < 3:
        print 'Api_key and Uid missing.'
        sys.exit()

    api_user = int(params['clientid'])
    uid = int(resourceID)

# CONFS
    max_time = datetime(2014, 10, 1, 12, 00)
    min_time = datetime(2014, 6, 1, 12, 00)
    min_ts = min_time.strftime('%s')
    max_ts = max_time.strftime('%s')

    user_path =     'data/users/'
    feed_path =     'data/feeds/'
    comment_path =  'data/comments/'
    log_path =      'data/logs/'

# API object
    api = InstagramAPI(client_id=app.accounts[api_user][1],\
            client_secret=app.accounts[api_user][2])

# Start logging
    logging.basicConfig(format='%(asctime)s %(levelname)s: %(message)s',\
            datefmt="%d/%m/%Y %H:%M:%S",\
            filename='%s/%d_%d.log'%(log_path, uid, api_user),\
            filemode='w', level=logging.DEBUG)

    logging.info('Starting to crawl Uid [%d] on api_user [%d].'%(uid, api_user))


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
    logging.info('Started crawling Uid [%d] medias comments.'%(uid))

    retry = 20
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
    
    logging.info('Dumping Uids on database from Uid_base=%d'%(uid))
    # Dumping on DB
    self.mysqlConnection = mysql.connector.connect(user=params['database']['user'],\
        password=params['database']['password'], host=params['database']['host'],\
        database=params['database']['name'])
    cursor = self.mysqlConnection.cursor()

    for comment in comments:
        who = comment['from']
        uid = str(who['id'])
        username = str(who['username'])
        query = 'insert INTO %s (resource_id,kargs) values (%s,%s)'%(uid, username)
        cursor.execute(query)
    cursor.commit()

    logging.info('[DONE] Dumping Uids on database from Uid_base=%d'%(uid))
    return (2,0)
