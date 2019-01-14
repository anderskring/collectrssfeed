# -*- coding: utf-8 -*-
from datetime import datetime
import pandas as pd
import feedparser
import sqlalchemy
import logging
import yaml
import uuid

conf = yaml.load(open('app.yaml'))
db_host = conf['database']['host']
db_port = conf['database']['port']
db_user = conf['database']['user']
db_pass = conf['database']['password']

connection_string = 'mysql+pymysql://' + db_user + ':' + db_pass + '@' + db_host + ':' + str(db_port) + '/rss_feed_items'
engine = sqlalchemy.create_engine(connection_string)


def get_feeds():
    rss_sources = conf['rss_feed_sources']
    feeds = pd.DataFrame()
    for rss_source in rss_sources:
        logging.info('Checking : ' + str(rss_source['source'].strip().encode('utf-8')) + ' : ' + str(rss_source['rss_url']))
        data = feedparser.parse(rss_source['rss_url'])

        for entry in data.entries:
            item = pd.Series()
            if entry.get("guid") is not None:
                item['guid'] = entry.guid.strip().encode('utf-8')
            else:
                item['guid'] = str(uuid.uuid4())
            item['link'] = entry.link.strip().encode('utf-8')
            item['pubDate'] = entry.published
            item['title'] = entry.title.strip().encode('utf-8')
            item['source'] = rss_source['source'].strip().encode('utf-8')
            feeds = feeds.append(item, ignore_index=True)

    feeds['pubDate'] = pd.to_datetime(feeds['pubDate'])
    return feeds


def store_items(data_frame):
    # logging.info(data_frame)
    data_frame.to_sql(name='feed_items', con=engine, index=False, if_exists='append')


def get_last_updated_article():
    result = engine.execute("SELECT pubDate FROM rss_feed_items.feed_items ORDER BY pubDate DESC LIMIT 1")
    return_value = datetime.strptime('Jan 1 2018', '%b %d %Y')
    data_value = return_value

    for row in result:
        data_value = row[0]

    if data_value > return_value:
        return_value = data_value

    return return_value


def collect_feed():
    logging.info("----------------------Collecting RSS Feeds %s ----------------------" % datetime.now().strftime("%Y-%m-%d %H:%M"))
    last_update_time = get_last_updated_article()
    feeds = get_feeds()
    mask1 = last_update_time < feeds['pubDate']
    mask2 = feeds['pubDate'] < datetime.now()
    feeds = feeds[mask1 & mask2]
    logging.info('We have ' + str(len(feeds)) + ' new articles since ' + last_update_time.strftime("%Y-%m-%d %H:%M"))
    store_items(feeds)
