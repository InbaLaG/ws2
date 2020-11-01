from flask import Flask, request, jsonify, render_template, session, flash, redirect, url_for
import redis
import urlcanon
import metadata_parser
import json
from datetime import datetime
from celery import Celery

# Broker URL for RabbitMQ task queue
# broker_url = 'amqp://guest@localhost'
url_db = redis.Redis(host='localhost', port=6379, db=3)


app = Flask(__name__)
app.config["DEBUG"] = True
# Celery configuration
# run celery worker, using redis to manage tasks:
# celery -A webapp.celery worker --loglevel=info
# need to start redis server before
app.config['CELERY_BROKER_URL'] = 'redis://localhost:6379/0'
app.config['CELERY_RESULT_BACKEND'] = 'redis://localhost:6379/0'
celery = Celery(app.name, broker=app.config['CELERY_BROKER_URL'])
celery.conf.update(app.config)


@celery.task()
def get_ogp_info(msg_id=None, url=None):
    print("in get!!!")
    print(msg_id)
    print(url)
    if msg_id == None or url == None:
        return None

    rec = {}
    form = {}
    rec['url'] = url
    rec['status'] = 'pending'
    url_db.set(msg_id, json.dumps(rec))
    print('done????')
    data = {}
    try:
        page = metadata_parser.MetadataParser(url=url)
        form['id'] = msg_id
        form['url'] = page.get_discrete_url(require_public_global=False)
        form['title'] = page.get_metadatas('title')
        form['type'] = page.get_metadatas('type') or 'Website'
        form['site-name'] = page.get_metadatas('site_name', strategy=['og', ])
        form['updated_time'] = datetime.now().strftime("%dd/%mm/%Y_%H:%M:%S")
        form['images'] = []
        for i in page.get_metadatas('image', strategy=['og', ]):
            img = {}
            img['url'] = i
            form['images'].append(img)

        # rec['og_obg'] = og
        rec['valid'] = 'ok'
        # TODO crape form here
        rec['form'] = form
        rec['status'] = 'done'
        url_db.set(msg_id, json.dumps(rec))
        return True
    except:
        rec['valid'] = 'error'
        rec['status'] = 'error'
        url_db.set(msg_id, json.dumps(rec))
        return False


def get_canonized_url(url):
    if url.startswith('http://www.'):
        return 'http://' + url[len('http://www.'):]
    if url.startswith('www.'):
        return 'http://' + url[len('www.'):]
    if not url.startswith('http://'):
        return 'http://' + url
    return url


def scrape_page(og_obg):
    return {}


def get_dict_from_db(k):
    data = url_db.get(k)
    if data == None:
        return None
    try:
        return pickle.loads(data)
    except:
        return eval(data.decode())


def get_url_record_by_url(url):
    if len(url_db.keys()) == 0:
        return None
    for k in url_db.keys():
        v = get_dict_from_db(k)
        if v["url"] == url:
            return [k, v]
    return None


@app.route('/stories', methods=['GET'])
def get_stories(msg_id=None):
    if msg_id == None:
        return ('no msg id')
    url_record = get_dict_from_db(msg_id)
    if url_record == None:
        return ('not found ')
    elif url_record['status'] != 'done':
        return("status: "+url_record['status'])
    else:
        return jsonify(url_record['form'])


@app.route('/stories', methods=['POST'])
def set_stories():
    url = request.args.get('url')
    print('url %s' % url)
    if url == None:
        return "error"
    else:
        c_url = get_canonized_url(url)
        print(c_url)
        exist_rec = get_url_record_by_url(c_url)
        if exist_rec:
            print("exist????")
            print(exist_rec[1]['status'])
            return str(exist_rec[0])
        else:
            if len(url_db.keys()) == 0:
                print("no db")
                msg_id = 1111
            else:
                msg_id = max([int(_) for _ in url_db.keys()]) + 1
            print(msg_id)
            get_ogp_info.delay(msg_id, c_url)
            return str(msg_id)


if __name__ == "__main__":
    print(app.name)
    app.run(host='0.0.0.0', port=5002)
