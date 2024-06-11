import polars as pl
from datetime import datetime
import os


def is_directory_empty(directory):
    # Lista os arquivos no diretório
    files = os.listdir(directory)
    # Verifica se a lista está vazia
    print(len(files) == 0)
    return len(files) == 0


def upsert_delta(primary_keys, data_path, df):

    if is_directory_empty(data_path):
        df.write_delta(data_path)
    else:

        string_exp = ""

        for pk in primary_keys:
            string_exp += (
                f"s.{pk} = t.{pk}" if string_exp == "" else f" AND s.{pk} = t.{pk}"
            )

        (
            df.write_delta(
                data_path,
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


def main():

    pl.Config.set_tbl_rows(100)

    df = pl.read_delta("data/data_parquet/")

    df_new = pl.DataFrame(
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

    pk = ["id", "pessoa"]

    upsert_delta(primary_keys=pk, data_path="data/data_parquet", df=df_new)

    df = pl.read_delta("data/data_parquet/")

    print(df)

if __name__ == "__main__":
    main()
