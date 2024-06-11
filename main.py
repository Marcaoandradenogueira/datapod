import polars as pl
from datetime import datetime
from deltalake import DeltaTable

df = pl.DataFrame(
    {
        "id": [1, 2, 3],
        "data_aniversario": [
            datetime(2025, 1, 1),
            datetime(1993, 9, 19),
            datetime(1960, 5, 31),
        ],
        "nota": [4.0, 5.0, 6.0],
        "pessoa": ["ludmila", "joao", "deise"],
    }
)

pl.Config.set_tbl_rows(100)

df.write_parquet("data/output.parquet")

df = pl.read_delta('data/data_parquet/')

print(df)


table_path = "data/data_parquet/"
df.write_delta(table_path)

df_new = pl.DataFrame(
    {
        "id": [1],
        "data_aniversario": [
            datetime(1993, 8, 19),
        ],
        "nota": [4.2],
        "pessoa": ["igor"]
    }
)


primary_keys = ['id', 'pessoa']

string_exp = ""

for pk in primary_keys:
    string_exp += f"s.{pk} = t.{pk}" if string_exp == "" else f" AND s.{pk} = t.{pk}"

(
    df_new.write_delta(
        "data/data_parquet/",
        mode="merge",
        delta_merge_options={
            "predicate": string_exp,
            "source_alias": "s",
            "target_alias": "t",
        },
    )
    .when_matched_update_all()
    .when_not_matched_insert_all()
    .execute()
)
