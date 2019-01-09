import collect_rss_feed
import collect_twitter_feed

import schedule
import time

schedule.every(10).minutes.do(collect_rss_feed.collect_feed)
schedule.every().minutes.do(collect_twitter_feed.update_tweets, limit=20)
schedule.every().day.at('00:01').do(collect_twitter_feed.update_list_users)

while True:
    schedule.run_pending()
    time.sleep(1)
