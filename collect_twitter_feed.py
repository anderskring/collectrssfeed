from datetime import datetime
import sqlalchemy
import yaml
import twitter
import logging
from twitter import TwitterError

conf = yaml.load(open('app.yaml'))
db_host = conf['database']['host']
db_port = conf['database']['port']
db_user = conf['database']['user']
db_pass = conf['database']['password']

twitter_consumer_key = conf['twitter']['consumer_key']
twitter_consumer_secret = conf['twitter']['consumer_secret']
twitter_access_token_key = conf['twitter']['access_token_key']
twitter_access_token_secret = conf['twitter']['access_token_secret']

connection_string = 'mysql+pymysql://' + db_user + ':' + db_pass + '@' + db_host + ':' + str(db_port) + '/twitter_feeds'
sql_engine = sqlalchemy.create_engine(connection_string)

twitter_api = twitter.Api(consumer_key=twitter_consumer_key,
                          consumer_secret=twitter_consumer_secret,
                          access_token_key=twitter_access_token_key,
                          access_token_secret=twitter_access_token_secret,
                          tweet_mode='extended')


def update_list_users():
    logging.info("----------------------Updating Twitter Users %s ------------------" % datetime.now().strftime("%Y-%m-%d %H:%M"))
    for twitter_list in conf['twitter_user_lists']:
        list_id = twitter_list['list_id']
        list_type = twitter_list['type']
        limit = twitter_list['limit']

        if limit > 0:
            sql = "SELECT * FROM twitter_feeds.table_users WHERE type = '%s' LIMIT 1" % list_type
            result = sql_engine.execute(sql)
            new_category = True
            if result.rowcount > 0:
                new_category = False

            if not new_category:
                sql = "SELECT last_updated_timestamp FROM twitter_feeds.table_users WHERE type = '%s' AND DATE(last_updated_timestamp) < DATE(now()) ORDER BY last_updated_timestamp DESC LIMIT 1" % list_type
                result = sql_engine.execute(sql)
                must_update = False
                if result.rowcount > 0:
                    must_update = True
            else:
                must_update = True

            if must_update:
                logging.info("Getting members of list : " + list_type + "(" + str(list_id) + ")")
                members = twitter_api.GetListMembers(list_id=list_id)
                members.sort(key=lambda x: x.followers_count, reverse=True)
                members = members[0:limit]
                for member in members:
                    logging.info(member.name)
                    sql = "INSERT INTO twitter_feeds.table_users (user_id, name, screen_name, description, profile_image_url, get_tweets, last_updated_timestamp, type) VALUES ('%s', '%s', '%s', '%s', '%s', %i, DATE(now()) , '%s') " % (member.id, member.name.replace("'", ""), member.screen_name, member.description.replace("'", "´").replace("%", "%%"), member.profile_image_url, 1, list_type)
                    sql += "ON DUPLICATE KEY UPDATE name = '%s', screen_name = '%s', description = '%s', profile_image_url = '%s', type = '%s', get_tweets = 1, last_updated_timestamp = DATE(now())" % (member.name.replace("'", ""), member.screen_name, member.description.replace("'", "´").replace("%", "%%"), member.profile_image_url, list_type)
                    sql_engine.execute(sql)

                    sql = "INSERT INTO twitter_feeds.table_follower (user_id, follower_count, following, update_timestamp) VALUES (%s, %s, %s, DATE(now())) " % (member.id, member.followers_count, member.following)
                    sql += "ON DUPLICATE KEY UPDATE follower_count = %s, following = %s " % (member.followers_count, member.following)
                    sql_engine.execute(sql)


def update_tweets(limit=20):
    logging.info("----------------------Updating Twitter Feeds %s ------------------" % datetime.now().strftime("%Y-%m-%d %H:%M"))
    user_list = []
    sql = "SELECT user_id, screen_name, latest_tweet_id FROM twitter_feeds.table_users WHERE get_tweets = 1 ORDER BY last_tweet_fetch LIMIT %s" % limit
    result = sql_engine.execute(sql)
    for row in result:
        this_user = {"user_id": row['user_id'], "latest_tweet_id": row['latest_tweet_id'], "screen_name": row['screen_name']}
        user_list.append(this_user)
    for user in user_list:
        try:
            if user['latest_tweet_id']:
                tweets = twitter_api.GetUserTimeline(user_id=user['user_id'], since_id=user['latest_tweet_id'])
            else:
                tweets = twitter_api.GetUserTimeline(user_id=user['user_id'], count=200, trim_user=True)
            latest_tweet_id = -1
            added_tweet_count = 0
            added_retweets_count = 0
            added_new_user = 0
            for tweet in tweets:
                created_at = datetime.strptime(tweet.created_at, '%a %b %d %H:%M:%S %z %Y')
                min_created_at = datetime.strptime("31-12-2018 +0000", '%d-%m-%Y %z')
                if created_at > min_created_at:
                    added_tweet_count += 1
                    if tweet.id > latest_tweet_id:
                        latest_tweet_id = tweet.id
                    this_tweet = {'tweet_id': tweet.id, 'user_id': user['user_id'], 'created_timestamp': str(created_at).split('+')[0], 'text': tweet.full_text}
                    sql = "REPLACE INTO twitter_feeds.table_tweets (tweet_id, user_id, created_timestamp, text) VALUES (%s, %s, '%s', '%s') " % (this_tweet['tweet_id'], this_tweet['user_id'], this_tweet['created_timestamp'], this_tweet['text'].replace("'", "´").replace("%", "%%"))
                    sql_engine.execute(sql)
                    if tweet.retweeted_status:
                        added_retweets_count += 1
                        sql = "SELECT * FROM twitter_feeds.table_users WHERE user_id = %s" % tweet.retweeted_status.user.id
                        result = sql_engine.execute(sql)
                        if result.rowcount > 0:
                            have_user = True
                        else:
                            have_user = False
                        if not have_user:
                            new_user = twitter_api.GetUser(user_id=tweet.retweeted_status.user.id)
                            # print("       We do NOT have User : " +  new_user.screen_name + ' (' + str(new_user.id) + ')')
                            added_new_user += 1

                            sql = "INSERT INTO twitter_feeds.table_users " \
                                  "(user_id, name, screen_name, description, profile_image_url, get_tweets, last_updated_timestamp, type) " \
                                  "VALUES " \
                                  "('%s', '%s', '%s', '%s', '%s', %i, DATE(now()) , '%s') " % (new_user.id, new_user.name.replace("'", "").replace("%", "%%"), new_user.screen_name, new_user.description.replace("'", "´").replace("%", "%%"), new_user.profile_image_url, 0, '')
                            sql_engine.execute(sql)
                        sql = "REPLACE INTO twitter_feeds.table_tweet_link (org_user_id, target_user_id, type, org_tweet_id, tweet_id) VALUES (%s, %s, 'Retweet', %s, %s)" % (tweet.retweeted_status.user.id, user['user_id'], tweet.retweeted_status.id, tweet.id)
                        sql_engine.execute(sql)
            sql = "UPDATE twitter_feeds.table_users SET last_tweet_fetch = now()"
            if latest_tweet_id > 0:
                sql += " , latest_tweet_id = " + str(latest_tweet_id)
            sql += " WHERE user_id =" + str(user['user_id'])
            sql_engine.execute(sql)

            logging.info(str(added_tweet_count) + '/' + str(added_retweets_count) + '/' + str(added_new_user) + ' User : ' + user['screen_name'] + ' (' + str(user['user_id']) + ')')

        except TwitterError:
            logging.info('TwitterError : User is Private... Ignoring Feed')
            sql = "UPDATE twitter_feeds.table_users SET get_tweets = 0 WHERE user_id = %i" % user['user_id']
            sql_engine.execute(sql)
