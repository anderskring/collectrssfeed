# -*- coding: utf-8 -*-

from datetime import datetime
import pandas as pd
import feedparser
import sys
import time
import sqlalchemy
import yaml
import uuid

conf = yaml.load(open('app.yaml'))
db_host = conf['database']['host']
db_port = conf['database']['port']
db_user = conf['database']['user']
db_pass = conf['database']['password']

connection_string = 'mysql+pymysql://' + db_user + ':' + db_pass + '@' + db_host + ':' + str(db_port) + '/rss_feed_items'
# print(connection_string)
engine = sqlalchemy.create_engine(connection_string)


def get_feeds():
    rss_sources = pd.read_csv('rss_sources.csv')
    feeds = pd.DataFrame()
    for index, row in rss_sources.iterrows():
        print('Checking : ' + str(row['source'].strip().encode('utf-8')) + ' : ' + str(row['rss_url']))
        data = feedparser.parse(row['rss_url'])

        for entry in data.entries:
            item = pd.Series()
            """
            if entry.get("description") is not None:
                desc = entry.description.strip().encode('utf-8')
                item['description'] = desc[:255] if len(desc) > 256 else desc
            """
            # item['description'] = 'Removed by Code'
            if entry.get("guid") is not None:
                item['guid'] = entry.guid.strip().encode('utf-8')
            else:
                item['guid'] = str(uuid.uuid4())
            item['link'] = entry.link.strip().encode('utf-8')
            item['pubDate'] = entry.published
            item['title'] = entry.title.strip().encode('utf-8')
            item['source'] = row['source'].strip().encode('utf-8')
            feeds = feeds.append(item, ignore_index=True)

    feeds['pubDate'] = pd.to_datetime(feeds['pubDate'])

    return feeds


def get_stored_items():
    # engine = sqlalchemy.create_engine('mysql+pymysql://root:root@db:3306/rss_feed_items')
    # stored_items = pd.read_sql_table('feed_items', engine)
    # print(stored_items)
    stored_items = pd.DataFrame()
    return stored_items


def store_items(data_frame):
    # print(data_frame)
    data_frame.to_sql(name='feed_items', con=engine, index=False, if_exists='append')


def countdown(t):  # in seconds
    print('Update done, now sleeping for {} seconds'.format(t))
    time.sleep(t)


def get_last_updated_article():
    result = engine.execute("SELECT pubDate FROM feed_items ORDER BY pubDate DESC LIMIT 1")
    for row in result:
        data_value = row[0]
    return_value = datetime.strptime('Jan 1 2018', '%b %d %Y')

    if 'data_value' in locals():
        if data_value > return_value:
            return_value = data_value

    print(return_value)
    return return_value


def main(agv):
    interval = 10 * 60   # interval between runs in seconds
    run_times = 0
    last_update_time = get_last_updated_article()

    run_counter = 1

    for argument in agv:
        arg_list = argument.split("=")
        if arg_list[0].lower() == "run_times" or arg_list[0].lower() == "rt":
                run_times = int(arg_list[1])
        if arg_list[0].lower() == "interval" or arg_list[0].lower() == "i":
                interval = int(arg_list[1])

    while run_counter <= run_times or run_times == 0:
        print("---------------------- %s ----------------------" % datetime.now().strftime("%Y-%m-%d %H:%M"))
        print("Running cycle number %i of %i" % (run_counter, run_times))
        feeds = get_feeds()
        mask1 = last_update_time < feeds['pubDate']
        mask2 = feeds['pubDate'] < datetime.now()
        feeds = feeds[mask1 & mask2]
        print('New articles since ' + last_update_time.strftime("%Y-%m-%d %H:%M"))
        print(feeds.to_string())
        stored_items = get_stored_items()
        result = pd.concat([feeds, stored_items]).drop_duplicates(['guid']).reset_index(drop=True)
        store_items(result)
        last_update_time = datetime.now()

        run_counter += 1
        if run_counter <= run_times or run_times == 0:
            countdown(interval)
    pass


if __name__ == "__main__":
    main(sys.argv)
