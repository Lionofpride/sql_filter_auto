import pandas as pd
import re
from sqlalchemy import create_engine, text
from tqdm import tqdm
import os
from config import INPUT_WP_POSTS_PATH, INPUT_WP_POSTMETA_PATH, OUTPUT_FILTERED_PATH, OUTPUT_UNIQUE_AUTHORS_PATH


def extract_insert_statements(filepath):
    print(f"Extraindo INSERTs de {filepath}...")
    with open(filepath, 'r', encoding='utf-8') as f:
        content = f.read()
        insert_statements = re.findall(r"INSERT INTO.*?VALUES (.*?);", content, re.DOTALL)
    return insert_statements


def convert_insert_to_dataframe(insert_statements, columns):
    all_values = []
    for stmt in tqdm(insert_statements, desc="Convertendo INSERTs em DataFrame"):
        values = re.findall(r"\((.*?)\)", stmt)
        for val in values:
            all_values.append(tuple(val.split(',')))

    return pd.DataFrame(all_values, columns=columns)


def filter_and_order_records(wp_posts_path, wp_postmeta_path, output_path):
    print("Verificando arquivos...")
    if not os.path.exists(wp_posts_path) or not os.path.exists(wp_postmeta_path):
        raise FileNotFoundError(f"Files not found: {wp_posts_path} or {wp_postmeta_path}")

    wp_postmeta_inserts = extract_insert_statements(wp_postmeta_path)
    wp_posts_inserts = extract_insert_statements(wp_posts_path)

    print("Convertendo statements em DataFrames...")
    wp_postmeta_df = convert_insert_to_dataframe(wp_postmeta_inserts, ["meta_id", "post_id", "meta_key", "meta_value"])
    wp_posts_df = convert_insert_to_dataframe(wp_posts_inserts,
                                              ["ID", "post_author", "post_date", "post_date_gmt", "post_content",
                                               "post_title", "post_excerpt", "post_status", "comment_status",
                                               "ping_status", "post_password", "post_name", "to_ping", "pinged",
                                               "post_modified", "post_modified_gmt", "post_content_filtered",
                                               "post_parent", "guid", "menu_order", "post_type", "post_mime_type",
                                               "comment_count"])

    print("Filtrando e ordenando registros...")
    filtered_wp_posts_df = wp_posts_df[wp_posts_df["ID"].isin(wp_postmeta_df["post_id"])]
    filtered_wp_posts_df = filtered_wp_posts_df.sort_values(by="post_author")

    print(f"Salvando resultados em {output_path}...")
    engine = create_engine('sqlite:///:memory:')
    filtered_wp_posts_df.to_sql("wp_posts", engine, if_exists='replace', index=False)
    with open(output_path, "w", encoding="utf-8") as out_file:
        out_file.write(str(engine.execute(text("SELECT * FROM wp_posts")).fetchall()))


def ensure_unique_post_authors(input_path, output_path):
    print("Garantindo autores únicos...")
    engine = create_engine('sqlite:///:memory:')
    df = pd.read_sql(f"sqlite:///{input_path}", engine)
    df.drop_duplicates(subset="post_author", keep="first", inplace=True)
    df.to_sql("wp_posts", engine, if_exists='replace', index=False)
    with open(output_path, "w", encoding="utf-8") as out_file:
        out_file.write(str(engine.execute(text("SELECT * FROM wp_posts")).fetchall()))


if __name__ == "__main__":
    filter_and_order_records(INPUT_WP_POSTS_PATH, INPUT_WP_POSTMETA_PATH, OUTPUT_FILTERED_PATH)
    ensure_unique_post_authors(OUTPUT_FILTERED_PATH, OUTPUT_UNIQUE_AUTHORS_PATH)
