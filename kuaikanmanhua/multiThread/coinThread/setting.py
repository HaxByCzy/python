# -*- coding: utf-8 -*-

import sys
defaultencoding = 'utf-8'
if sys.getdefaultencoding() != defaultencoding:
    reload(sys)
    sys.setdefaultencoding(defaultencoding)

hdsId = 1073
cqytId = 1080
bdwzId = 1089

# mysql链接设置

#测试
kkb_host = '10.9.33.70'
kkb_port = 3316
kkb_db = 'kkb'
auth_db = 'comic_auth'
kkb_user = 'user_read_pay'
kkb_passwd = 'vwgvqefr57'

timer_host = '10.9.33.70'
timer_port = 3316
timer_db = 'kbtimer'
timer_user = 'user_kbtimer'
timer_passwd = 'vfavefv3r'


#线上
kkb_host = '10.9.98.143'
kkb_port = 3309
kkb_db = 'kkb'
auth_db = 'comic_auth'
kkb_user = 'user_read_pay'
kkb_passwd = 'vwgvqefr57'

timer_host = '10.9.108.186'
timer_port = 3309
timer_db = 'kbtimer'
timer_user = 'user_kbtimer'
timer_passwd = 'vdav45!3VDQOY'

redis_host = '10.9.106.116'
redis_port = 6395
