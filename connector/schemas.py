import pyarrow as pa

SCHEMAS = {
    "trade": {
        "schema": pa.schema([
            # ("e", pa.string()),     # Event type
            ("E", pa.int64()),      # Event time
            # ("T", pa.int64()),      # Trade time
            # ("s", pa.string()),     # Symbol
            ("t", pa.int64()),      # Trade ID
            ("p", pa.string()),     # Price
            ("q", pa.string()),     # Quantity
            # ("X", pa.string()),     # Order type (MARKET/LIMIT)
            ("m", pa.bool_()),      # Is buyer maker
        ]),
        "fields": ("E", "t", "p", "q", "m"),
        "name": "trades",
    },
    "depthUpdate": {
        "schema": pa.schema([
            ("E", pa.int64()),      # Event time
            # ("T", pa.int64()),      # Transaction time
            ("s", pa.string()),     # Symbol
            ("U", pa.int64()),      # First update ID
            ("u", pa.int64()),      # Final update ID
            # ("pu", pa.int64()),     # Final update ID of last stream
            ("b", pa.string()),     # Bids (JSON строка)
            ("a", pa.string()),     # Asks (JSON строка)
        ]),
        "fields": ("E", "s", "U", "u", "b", "a"),
        "name": "depth",
    },
}