# ！/usr/bin/env python
# —*— coding: utf-8 —*—

from operations import DB
import json, time, datetime
from flask import Flask
from flask_cors import CORS

# 初始化flask
app = Flask(__name__)
CORS(app, resources=r'/*')  # 让本服务器所有的URL都允许跨域请求
MY_URL = '/2-204/'
# =========================================================================
# --------------------------------设置配置信息--------------------------------
usr = 'xxxxxxxxxxxxxx'
passwd = 'xxxxxxxxxxxx'
ip = 'xxx.xxx.xxx.xxx'
port = xxxxxxxx

# 连接数据库
DB_local = DB(usr, passwd, ip)

# 重定义datetime格式的编码方法
# 如果没有这一段，则会出现错误：TypeError: Object of type datetime is not JSON serializable
class DateEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime.datetime):
            return obj.strftime("%Y-%m-%d %H:%M:%S")
        else:
            return json.JSONEncoder.default(self, obj)

# 获取：城市数据，职业数据，等等等等
@app.route(MY_URL + 'request-all', endpoint='request-all', methods=['GET'])
def request_all():
    print('request received')
    if DB_local.using_db == 1:
        return json.dumps({'msg': '数据库更新中，请稍后再试'}, ensure_ascii=False)
    else:
        time_start = time.time()
        return_dict = DB_local.get_second_layer_db_data()
        print('%s seconds used for checking data'%str(time.time() - time_start))
        if not bool(return_dict):
            return json.dumps({'msg': '数据爬取中，请稍后再试'}, ensure_ascii=False)
        else:
            return json.dumps(return_dict, cls=DateEncoder, ensure_ascii=False)

if __name__ == "__main__":
    app.run(threaded=True, processes=True, host='0.0.0.0', port=port, debug=True, use_reloader=False)
