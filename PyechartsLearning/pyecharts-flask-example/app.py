# import sys
# sys.path.append(r'D:\Code\Moon')

from flask import Flask, render_template
from pyecharts.globals import CurrentConfig

from pyecharts import options as opts
from pyecharts.charts import Bar


app = Flask(__name__, static_folder="templates")


def bar_base() -> Bar:
    c = (
        Bar()
        .add_xaxis(["衬衫", "羊毛衫", "雪纺衫", "裤子", "高跟鞋", "袜子"])
        .add_yaxis("商家A", [5, 20, 36, 10, 75, 90])
        .add_yaxis("商家B", [15, 25, 16, 55, 48, 8])
        .set_global_opts(title_opts=opts.TitleOpts(title="Bar-基本示例", subtitle="我是副标题"))
    )
    return c


@app.route("/barChart")
def bar_chart():
    c = bar_base()
    return c.dump_options_with_quotes()

@app.route("/")
def index():
    return render_template(r'index.html')


if __name__ == "__main__":
    app.run()
