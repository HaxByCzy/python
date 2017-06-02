# -*- coding: utf-8 -*-

import sys
defaultencoding = 'utf-8'
if sys.getdefaultencoding() != defaultencoding:
    reload(sys)
    sys.setdefaultencoding(defaultencoding)

# mysql数据库链接设置
host = '10.9.33.70'
port = 3316
db = 'data_platform'
username = 'data_platform'
pwd = '123456'