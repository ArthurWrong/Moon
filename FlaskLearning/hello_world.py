from flask import Flask
app = Flask(__name__)

@app.route('/')
def index():
    return '<h1>Hello World</h1>'

@app.route('/hello/<name>')
def hello(name):
    return 'Hello %s' % name

@app.route('/user/<int:user_id>')
def get_user(user_id):
    return 'User ID: %d' % user_id

if __name__ == '__main__':
    app.run()
