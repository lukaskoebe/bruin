"""@bruin

name: hello_python

@bruin"""

import pandas as pd


def materialize():
    return pd.DataFrame(
        [
            {"customer_id": 1, "customer_name": "Ada Lovelace"},
            {"customer_id": 2, "customer_name": "Grace Hopper"},
            {"customer_id": 3, "customer_name": "Katherine Johnson"},
        ]
    )
