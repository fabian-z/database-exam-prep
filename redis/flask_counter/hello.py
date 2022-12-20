from flask import Flask
import redis
import sys

app = Flask(__name__)

r = redis.Redis(host='localhost', port=6379, db=0)

@app.route('/reset')
def reset():
    r.set('python', 0)
    r.set('c', 0)
    r.set('go', 0)
    r.set('total_votes', 0)

reset()

@app.route('/get/python')
def get_python():
    if r.get('total_votes') == 0:
       return '0'
    return r.get('python')

    print(r.get('python'), file=sys.stderr)
    print('test!!!', file=sys.stderr)

    #percent = int.from_bytes(r.get('python'), "little") / int.from_bytes(r.get('total_votes'), "little")
    #return str(percent)

@app.route('/get/c')
def get_c():
    if r.get('total_votes') == 0:
       return '0'
    return r.get('c')

    #percent = int.from_bytes(r.get('c'), "little") / int.from_bytes(r.get('total_votes'), "little")
    #return str(percent)

@app.route('/get/go')
def get_go():
    if r.get('total_votes') == 0:
       return '0'
    return r.get('go')

    #percent = int.from_bytes(r.get('go'), "little") / int.from_bytes(r.get('total_votes'), "little")
    #return str(percent)

@app.route('/vote/python')
def vote_python():
    r.incr('python')
    r.incr('total_votes')
    return get_python()

@app.route('/vote/c')
def vote_c():
    r.incr('c')
    r.incr('total_votes')
    return get_c()

@app.route('/vote/go')
def vote_go():
    r.incr('go')
    r.incr('total_votes')
    return get_go()

