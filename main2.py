import inspect
from functools import cache, partial, wraps

import polars as pl
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from great_tables import GT, html
from great_tables.data import sza

app = FastAPI()

templates = Jinja2Templates(directory="templates")


@cache
def get_sza():
    return pl.from_pandas(sza)


def gt2fastapi(func=None):
    """
    https://pybit.es/articles/decorator-optional-argument/
    """

    def _get_template_response(resp):
        context = resp.context
        request = context.pop("request")
        name = resp.template.name
        new_context = {}
        for key, value in context.items():
            if isinstance(value, GT):
                value = value.as_raw_html()
            new_context[key] = value
        return templates.TemplateResponse(
            request=request, name=name, context=new_context
        )

    if func is None:
        return partial(gt2fastapi)

    @wraps(func)
    async def async_wrapper(*args, **kwargs):
        resp = await func(*args, **kwargs)
        return _get_template_response(resp)

    @wraps(func)
    def wrapper(*args, **kwargs):
        resp = func(*args, **kwargs)
        return _get_template_response(resp)

    return async_wrapper if inspect.iscoroutinefunction(func) else wrapper


@app.get("/", response_class=HTMLResponse)
@gt2fastapi
def index(request: Request):
    sza_pivot = (
        get_sza()
        .filter((pl.col("latitude") == "20") & (pl.col("tst") <= "1200"))
        .select(pl.col("*").exclude("latitude"))
        .drop_nulls()
        .pivot(values="sza", index="month", on="tst", sort_columns=True)
    )

    sza_gt = (
        GT(sza_pivot, rowname_col="month")
        .data_color(
            domain=[90, 0],
            palette=["rebeccapurple", "white", "orange"],
            na_color="white",
        )
        .tab_header(
            title="Solar Zenith Angles from 05:30 to 12:00",
            subtitle=html("Average monthly values at latitude of 20&deg;N."),
        )
        .sub_missing(missing_text="")
    )

    context = {"sza_gt": sza_gt}

    return templates.TemplateResponse(
        request=request, name="index.html", context=context
    )


@app.get("/async", response_class=HTMLResponse)
@gt2fastapi
async def async_index(request: Request):
    sza_pivot = (
        get_sza()
        .filter((pl.col("latitude") == "20") & (pl.col("tst") <= "1200"))
        .select(pl.col("*").exclude("latitude"))
        .drop_nulls()
        .pivot(values="sza", index="month", on="tst", sort_columns=True)
    )

    sza_gt = (
        GT(sza_pivot, rowname_col="month")
        .data_color(
            domain=[90, 0],
            palette=["orange", "white", "rebeccapurple"],
            na_color="white",
        )
        .tab_header(
            title="Solar Zenith Angles from 05:30 to 12:00",
            subtitle=html("Average monthly values at latitude of 20&deg;N."),
        )
        .sub_missing(missing_text="")
    )

    context = {"sza_gt": sza_gt}

    return templates.TemplateResponse(
        request=request, name="index.html", context=context
    )
