# -*- coding: utf-8 -*-

import random
from datetime import datetime, timedelta

import MySQLdb
from MySQLdb.cursors import DictCursor
import redis

db = 'kuaikan'
user = 'exp_imp_user'
passwd = 'G6_y7P9f5DpS8'
host = '10.9.5.43'

count_redis_host = 'b-cache01'
store_redis_host = '10.9.105.75'


# db = 'kuaikan'
# user = 'test'
# passwd = 'test_kuaikan'
# host = 'db01'


# def stat():
    # con = MySQLdb.connect(host=host, user=user, passwd=passwd, db=db, charset='utf8', cursorclass=DictCursor)
    # cur = con.cursor()
    # print u'日期,评论量,点赞量'
    # for month in range(1, 13):
    #     n_year = 2015 if month < 12 else 2016
    #     n_month = (month + 1) if month < 12 else 1
    #     start_date = datetime(2015, month, 1)
    #     end_date = datetime(n_year, n_month, 1)
    #     print 'python manage.py export_update_topic_info -s %s -e %s' % (start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d'))

        # start_date = datetime(2015, month, 1) - timedelta(hours=8)
        # end_date = datetime(n_year, n_month, 1) - timedelta(hours=8)
        # comment_sql = ''' select count(*) as cnt from comment where created_at>='%s' and created_at<'%s' and status=0 ''' % (start_date, end_date)
        # cur.execute(comment_sql)
        # comment_count = cur.fetchone()['cnt']
        # comic_like_sql = ''' select count(*) as cnt from comic_like where created_at>='%s' and created_at<'%s' and status=0 ''' % (start_date, end_date)
        # # print comic_like_sql
        # cur.execute(comic_like_sql)
        # print '%s,%d,%d' % ((start_date + timedelta(hours=8)).strftime('%Y-%m'), comment_count, cur.fetchone()['cnt'])
    # cur.close()
    # con.close()


def stat():
    con = MySQLdb.connect(host=host, user=user, passwd=passwd, db=db, charset='utf8', cursorclass=DictCursor)
    store_client = redis.Redis(host=store_redis_host, port=6382)
    count_client = redis.Redis(host=count_redis_host)
    cur = con.cursor()
    cur.execute(''' select id, nickname from user where role='author' ''')
    users = {item['id']: item['nickname'].replace(",", "") for item in cur.fetchall()}
    cur.execute(''' select id, title from tag ''')
    tags = {tag['id']: tag['title'] for tag in cur.fetchall()}
    cur.execute('''select id, title, user_id from topic where status='published' order by id asc''')
    topics = [{'id': item['id'], 'title': item['title'], 'user_id': item['user_id']} for item in cur.fetchall()]
    out_file = open('result_1.csv', 'w')
    out_file.write(u'id,标题,漫画数量,连载时间,漫画浏览量,专题浏览量,总浏览量,粉丝数(关注人数),题材类型,作者名\n'.encode('utf8'))
    out_file.flush()
    for topic in topics:
        cur.execute(''' select id, created_at from comic where topic_id=%d and status='published' order by created_at asc''' % topic['id'])
        view_count = 0
        comic_num = 0
        min_time = None
        max_time = None
        for item in cur.fetchall():
            if min_time is None:
                min_time = item['created_at']
            max_time = item['created_at']
            t_sum = long(store_client.get('kuaikan-store:comics:%s:webviews_count' % item['id']))
            #if t_sum <= 0:
            #    raise
            view_count += t_sum
            comic_num += 1
        fav_count = long(count_client.get('kuaikan-cache:%s:fav:count' % topic['id']))
        topic_pv = long(store_client.get('kuaikan-store:topics:%s:views_count' % topic['id']))
        cur.execute(''' select distinct(tag_id) as tag_id from topic_tag where topic_id=%d ''' % topic['id'])
        topic_tag = '|'.join([tags[item['tag_id']] for item in cur.fetchall()])
        topic_user = users[topic['user_id']]
        out_file.write('%d,%s,%d,%s,%d,%d,%d,%d,%s,%s\n' % (
            topic['id'],
            topic['title'].replace(",", "").encode('utf8'),
            comic_num,
            '%s~%s' % ((min_time + timedelta(hours=8)).strftime('%Y-%m-%d %H:%M:%S'),
                       (max_time + timedelta(hours=8)).strftime('%Y-%m-%d %H:%M:%S')) if comic_num else '',
            view_count,
            topic_pv,
            view_count * 12 + topic_pv,
            fav_count,
            topic_tag.encode('utf8'),
            topic_user.encode('utf8')
        ))
        out_file.flush()
    # topics.sort(key=lambda t: t['count'], reverse=True)
    # out_file.write(u'id,标题,关注量'.encode('utf8'))
    # for i, topic in enumerate(topics):
    #     out_file.write('%d,%s,%d\n' % (topic['id'], topic['title'].encode('utf8'), topic['count']))
    # out_file.flush()
    out_file.close()
    cur.close()
    con.close()


# def stat_1():
#     client = redis.Redis(host=redis_host)
#     con = MySQLdb.connect(host=host, user=user, passwd=passwd, db=db, charset='utf8', cursorclass=DictCursor)
#     cur = con.cursor()
#     cur.execute('''select id from topic where status='published' order by id asc''')
#     for topic in cur.fetchall():
#         cur1 = con.cursor()
#         cur1.execute(''' select count(id) as cnt from favourite where target_id=%d and target_type=1 and status=0 ''' % topic['id'])
#         key = 'kuaikan-cache:%s:fav:count' % topic['id']
#         value = cur1.fetchone()['cnt'] or 0
#         print key, value
#         client.set(key, value)
#         cur1.close()
#     cur.close()
#     con.close()

def stat1():
    con = MySQLdb.connect(host, user, passwd, db, charset='utf8', cursorclass=DictCursor)
    cur = con.cursor()
    store_client = redis.Redis(host=store_redis_host, port=6382)
    count_client = redis.Redis(host=count_redis_host)
    print u'作品名,单篇数量,所有话PV和,专题页PV,所有话PV和*12+专题页PV,单篇PV,作品点赞数,评论点赞数,总点赞数,单篇点赞数,作品评论数' \
          u',单篇评论数,关注数'.encode('utf8')
    cur.execute("select id, title from topic where status='published' order by id asc")
    for topic in cur.fetchall():
        cur1 = con.cursor()
        cur1.execute("select id, title from comic where topic_id=%s and status='published'" % topic['id'])
        ret = 0
        num = 0
        t_like_count_sum = 0
        t_comment_like_count_sum = 0
        for item in cur1.fetchall():
            comic_id = item['id']
            t_sum = long(store_client.get('kuaikan-store:comics:%s:webviews_count' % comic_id))
            #if t_sum <= 0:
            #    raise
            ret += t_sum
            num += 1
            t_like_count_sum += long(count_client.get('kuaikan-cache:likes:count:comic:%s' % comic_id))
            cur2 = con.cursor()
            cur2.execute("select IFNULL(sum(score), 0) as count from comment where comic_id=%s and status=0" % comic_id)
            t_comment_like_count_sum += cur2.fetchone()['count'] or 0
            cur2.close()
        if num == 0:
            continue
        all_like = t_like_count_sum + t_comment_like_count_sum
        topic_pv = long(store_client.get('kuaikan-store:topics:%s:views_count' % topic['id']))
        topic_fav = long(count_client.get('kuaikan-cache:%s:fav:count' % topic['id']))
        all_pv = ret * 12 + topic_pv
        comment_count = long(count_client.get('kuaikan-cache:topic:%s:comments_count' % topic['id']))
        print '%s,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d' % (
            topic['title'].encode('utf8'),
            num,
            ret,
            topic_pv,
            all_pv,
            int(all_pv / num),
            t_like_count_sum,
            t_comment_like_count_sum,
            all_like,
            int(all_like / num),
            comment_count,
            int(comment_count / num),
            topic_fav
        )
        cur1.close()
    cur.close()
    con.close()

if __name__ == '__main__':
    stat1()
