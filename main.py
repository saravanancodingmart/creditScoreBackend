from flask import Flask, request, jsonify
import time
from datetime import datetime
import pymysql

app = Flask(__name__)
app.config['WTF_CSRF_CHECK_DEFAULT'] = False

zeus_db = pymysql.connect(host='127.0.0.1',
                         user='root',
                         password='root',
                         db='grab',
                         autocommit=True)


def get_rows_from_grab(sql):
   print(sql)
   try:
       with zeus_db.cursor() as curr:
           curr.execute(sql)
           rows = curr.fetchall()
       return rows
   except Exception as e:
       return ()


curr_seconds = time.time()


@app.route("/transaction", methods=["POST"])
def do_transaction():
   global curr_seconds
   data = request.json
   print(request.json)
   count = data['count']
   time_period = data['period']/count
   sql = "select user_id, amount from user where amount>0 order by RAND() limit 1"
   data = get_rows_from_grab(sql)
   user_id = data[0][0]
   amount = data[0][1]
   sql = "select coin_id from coin where user_id={} order by coin_values".format(user_id)
   user_c = get_rows_from_grab(sql)
   user_coins = [str(row[0]) for row in user_c]
   
   transact_amount = amount / count
   for i in range(0, count):
       curr_seconds = curr_seconds + time_period
       insert_date = datetime.fromtimestamp(curr_seconds).strftime('%Y-%m-%d %H:%M:%S')
       sql = "select user_id, amount from user where user_id <> {} order by RAND() limit 1".format(user_id)
       to_user_id = get_rows_from_grab(sql)[0][0]
       to_user_amount = get_rows_from_grab(sql)[0][1]
       coin_to_send = []
       for _ in range(0, transact_amount):
           if len(user_coins) == 0:
              break
           coin_to_send.append(user_coins.pop(0))
       s = [str(i) for i in coin_to_send] 
       sql = "update coin set user_id = {} where coin_id in ({})".format(to_user_id, ",".join(s))
       get_rows_from_grab(sql)
       amount = amount-transact_amount

       with open("temp", "w+") as fd:
           fd.write("{}".format(",".join(s)))

       with open("temp", 'rb') as fd:
           blob_data = fd.read()

       sql = "insert into transaction(transaction_date, from_user, to_user, amount, blob_data)" \
             " values ('%s', %d, %d, %d, '%s')" % (insert_date, user_id, to_user_id, transact_amount, str(blob_data))
       get_rows_from_grab(sql)
       to_user_amount = to_user_amount + transact_amount
       sql = "update user set amount={} where user_id={}".format(transact_amount, to_user_id)

       get_rows_from_grab(sql)

       # Todo : add record to transaction db and insert stimulated date

   sql = "update user set amount={} where user_id={}".format(amount, user_id)

   get_rows_from_grab(sql)


@app.route('/getCustomerCreditValue/<int:user_id>', methods=["POST"])
def get_customer_credit_value(user_id):
   sql = "select name, credit_value from user where user_id = {}".format(user_id)
   data = get_rows_from_grab(sql)
   response = {}
   response['name'] = data[0]
   response['credit_value'] = data[1]
   return jsonify(data)


@app.route('/customerBasedOnLocation', methods=["GET"])
def customer_based_on_location():

   location_list = request.args.get('location').split()
   add_sql = ""
   if len(location_list) > 0:
       add_sql = "where address in ({}) or state in ({}) or country in ({})".format(",".join(location_list), ",".join(location_list),
                                                                                 ",".join(location_list))
   sql = "select address_id from address {}".format(add_sql)
   address_id = get_rows_from_grab(sql)
   sql = "select name, credit_value from user where address_id in ({})".format(",".join(address_id))
   rows = get_rows_from_grab(sql)
   resp = {}
   for row in rows:
       resp[row[0]] = resp[row[1]]
   return jsonify(resp)


@app.route('/get_customer_coin_values/<int:user_id>', methods=["GET"])
def get_customer_coin_values(user_id):
   sql = "select coin_id, coin_values from coins where user_id = {}".format(user_id)
   rows = get_rows_from_grab(sql)
   resp = []
   i = 0
   for row in rows:
       temp = {
           'coin_id': row[0],
           'value': row[1]
       }
       resp.append(temp)

   return jsonify({"coin_values" : resp})


@app.route('/getCoinsValueBasedOnLocation')
def get_coins_value_based_on_location():
   location_list = request.args.get('location').split()
   add_sql = ""
   if len(location_list) > 0:
       add_sql = "where address in ({}) or state in ({}) or country ({})".format(",".join(location_list),
                                                                                 ",".join(location_list),
                                                                                 ",".join(location_list))
   sql = "select address_id from address {}".format(add_sql)
   address_id = get_rows_from_grab(sql)
   sql = "select user_id from user where address_id in ({})".format(",".join(address_id))
   rows = get_rows_from_grab(sql)
   user_ids = []
   for row in rows:
       user_ids.append(row[0])

   sql = "select coin_id, coin_value from coin where user_id in ({})".format(",".join(user_ids))
   rows = get_rows_from_grab(sql)
   resp = []
   i = 0
   for row in rows:
       temp = {
           'coin_id': row[0],
           'value': row[1]
       }
       resp.append(temp)

   return jsonify({"coin_values": resp})


@app.route('/getCustomerCreditHistory/<int:user_id>', methods=["GET"])
def get_customer_credit_history(user_id):
   sql = 'select month, credit_score from customer_credit_history where user_id = {}'.format(user_id)

   rows = get_rows_from_grab(sql)

   res = []
   for row in rows:
       temp = {
           'month' : row[0],
           'credit_score' : row[1]
       }
       res.append(temp)

   return jsonify({"credit_history" : res})

@app.route('/getCustomersCreditHistoryByLocation', methods=["GET"])
def get_customers_credit_history():
   location_list = request.args.get('location').split()
   add_sql = ""
   if len(location_list) > 0:
       add_sql = "where address in ({}) or state in ({}) or country ({})".format(",".join(location_list),
                                                                                 ",".join(location_list),
                                                                                 ",".join(location_list))
   sql = "select address_id from address {}".format(add_sql)
   address_id = get_rows_from_grab(sql)
   sql = "select user_id from user where address_id in ({})".format(",".join(address_id))
   rows = get_rows_from_grab(sql)
   user_ids = []
   for row in rows:
       user_ids.append(row[0])

   sql = 'select month, credit_score, user_id from customer_credit_history where user_id in ({})'.format(",".format(user_ids))

   rows = get_rows_from_grab(sql)

   res = []
   for row in rows:
       temp = {
           'month': row[0],
           'credit_score': row[1],
           'user_id' : row[2]
       }
       res.append(temp)

   return jsonify({"credit_history": res})


if __name__ == '__main__':
   app.run(host='0.0.0.0')

