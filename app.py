import streamlit as st
import pandas as pd
import asyncio
from playwright.async_api import async_playwright
from notion_client import Client
from datetime import datetime, timezone

# ----- Playwright でページ情報（ステータス/タイトル/全文）を取得する非同期関数 -----
async def fetch_page_info(url: str):
    """
    - HTTP ステータスコード
    - ページタイトル
    - ページ全文 (document.body.innerText)
    を取得して返す。
    404 の場合は { 'status': 404 } のみを返す。
    """
    result = {"status": None, "title": "No Title", "body": ""}

    try:
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            context = await browser.new_context(
                user_agent=(
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/113.0.0.0 Safari/537.36"
                )
            )
            page = await context.new_page()

            try:
                response = await page.goto(url, timeout=15000)
            except Exception:
                result["status"] = None
                await browser.close()
                return result

            if response is None:
                result["status"] = None
                await browser.close()
                return result

            status = response.status
            result["status"] = status

            # 404 はスキップ
            if status == 404:
                await browser.close()
                return result

            try:
                await page.wait_for_load_state("networkidle", timeout=7000)
            except Exception:
                pass

            try:
                title = await page.title()
                result["title"] = title.strip() if title and title.strip() else "No Title"
            except Exception:
                result["title"] = "No Title"

            try:
                full_text = await page.evaluate("document.body.innerText")
                result["body"] = full_text.strip() if full_text and full_text.strip() else ""
            except Exception:
                result["body"] = ""

            await browser.close()
            return result

    except Exception:
        result["status"] = None
        result["title"] = "No Title"
        result["body"] = ""
        return result

def fetch_page_info_sync(url: str):
    return asyncio.run(fetch_page_info(url))


# ----- 本文を 2000 文字ごとに分割して Paragraph ブロックへ変換する関数 -----
def split_text_to_paragraph_blocks(text: str) -> list:
    """
    - 引数 text を 2000 文字ごとに区切り、
    - Notion の paragraph ブロック形式に変換してリストで返す。
    """
    max_length = 2000
    children = []
    start = 0
    text_length = len(text)

    while start < text_length:
        chunk = text[start:start + max_length]
        block = {
            "object": "block",
            "type": "paragraph",
            "paragraph": {
                "rich_text": [
                    {
                        "type": "text",
                        "text": {
                            "content": chunk
                        }
                    }
                ]
            }
        }
        children.append(block)
        start += max_length

    return children


# ----- Notion 用ユーティリティ -----
def build_title_property(col_value: str):
    return {
        "title": [
            {"text": {"content": col_value}}
        ]
    }

def build_url_property(col_value: str):
    return {
        "url": col_value
    }

def build_date_property(unix_ts: int):
    dt = datetime.fromtimestamp(unix_ts, timezone.utc)
    date_str = dt.strftime("%Y-%m-%d")
    return {
        "date": {"start": date_str}
    }

def build_multi_select_property(tag_str: str):
    if not tag_str:
        return {"multi_select": []}
    tags = [t.strip() for t in tag_str.split(",") if t.strip()]
    return {
        "multi_select": [{"name": tag} for tag in tags]
    }

def build_select_property(status_value: str):
    if not status_value:
        return {"select": None}
    return {
        "select": {"name": status_value}
    }

def create_notion_page(notion, database_id: str, property_json: dict, children_blocks: list):
    return notion.pages.create(
        parent={"database_id": database_id},
        properties=property_json,
        children=children_blocks
    )

@st.dialog("使い方")
def show_instructions():
    st.markdown(
        """
        ### 🔧 Notionの準備

        1. **Notion APIキー**：自分のIntegration Tokenを貼り付ける  
        2. **データベースID**：控えておいたデータベースIDを入力  
        3. **本文を登録するか**：チェックボックスでON/OFF（必要に応じて）  

        → 正しく設定すると「データベース情報を取得しました」と表示されます。

        ---

        ### 🔗 CSVとNotionの項目をつなぐ

        メインエリアのプルダウンから以下のように対応づけます：

        - `title` → Notionの「タイトル」  
        - `url` → Notionの「URL」  
        - `time_added` → Notionの日付列（datetime型）  
        - `tags` → Notionの「タグ」（マルチセレクト）  
        - `status` → Notionの「ステータス」（セレクト）  

        ※ 移行したい項目だけでOK。
        ※ `time_added` は Notion の「Created Time」にはマッピングできません。  
          事前に Notion 側で Date 型のカスタムプロパティ（例：`Added Date`）を作成しておいてください。

        ---

        ### 📁 CSVを選ぶ

        1. 「CSVを選択」ボタンをクリック  
        2. Pocket公式サイトなどからエクスポートしたCSVを選ぶ  
           → 表のプレビューと行数が表示されます

        ---

        ### 📝 Notionに登録

        1. マッピングを確認して  
        2. 「CSVをNotionに登録」ボタンをクリック！  
           - `title` が URL の場合はスクレイピングでページタイトル＋全文を取得  
           - HTTP ステータス404ならその行をスキップ  
           - `time_added` は日付型プロパティ（例：Added Date）に変換して登録  
           - 最後に「成功/失敗/スキップ数」が表示される
        """
    )
    if st.button("閉じる"):
        st.rerun()


# ----- Streamlit UI -----
st.set_page_config(
    page_title="Pocket to Notion",
    page_icon=":notebook_with_decorative_cover:",
    layout="wide"
)
st.title("Pocket to Notion")

# ------------------------------------------------------------
# サイドバー：認証情報＆フラグ＆「使い方を見る」ボタン
# ------------------------------------------------------------
st.sidebar.header("Notion 認証情報設定")
notion_token = st.sidebar.text_input("Notion API キー", type="password")
database_id  = st.sidebar.text_input("Notion データベース ID")
register_body = st.sidebar.checkbox("本文を Notion に登録する", value=True)

if st.sidebar.button("使い方を見る"):
    show_instructions()

# ------------------------------------------------------------
# メインエリア：データベース情報取得 → マッピング → CSV登録
# ------------------------------------------------------------

# 1. データベース情報取得
properties = {}
property_names = []
if notion_token and database_id:
    try:
        notion   = Client(auth=notion_token)
        database = notion.databases.retrieve(database_id=database_id)
        properties     = database.get("properties", {})
        property_names = list(properties.keys())
        st.success("データベース情報を取得しました。")
    except Exception as e:
        st.error(f"データベース取得エラー: {e}")
else:
    # この行は後ほどマッピング部分の直後に表示したいので、ここでは何もしない
    pass

# 2. プロパティのマッピング（５列横並びで表示）
mapping = {}
csv_columns = ["title", "url", "time_added", "tags", "status"]

st.markdown("### STEP1: CSV カラムと Notion プロパティの紐づけ設定")

if property_names:
    cols = st.columns(5)
    with cols[0]:
        choice_title = st.selectbox(
            "CSV の「title」→ Notion プロパティ選択",
            options=["― 未選択 ―"] + property_names,
            key="map_title"
        )
        mapping["title"] = choice_title

    with cols[1]:
        choice_url = st.selectbox(
            "CSV の「url」→ Notion プロパティ選択",
            options=["― 未選択 ―"] + property_names,
            key="map_url"
        )
        mapping["url"] = choice_url

    with cols[2]:
        choice_time = st.selectbox(
            "CSV の「time_added」→ Notion プロパティ選択",
            options=["― 未選択 ―"] + property_names,
            key="map_time"
        )
        mapping["time_added"] = choice_time

    with cols[3]:
        choice_tags = st.selectbox(
            "CSV の「tags」→ Notion プロパティ選択",
            options=["― 未選択 ―"] + property_names,
            key="map_tags"
        )
        mapping["tags"] = choice_tags

    with cols[4]:
        choice_status = st.selectbox(
            "CSV の「status」→ Notion プロパティ選択",
            options=["― 未選択 ―"] + property_names,
            key="map_status"
        )
        mapping["status"] = choice_status

    # st.write("現在のマッピング設定：", mapping)
    st.markdown(
        """
        **注意点**  
        - `time_added` は Notion の「Created Time」に直接マッピングできません。  
        - 必要に応じて Date 型のカスタムプロパティ（例：`Added Date`）を作成してください。  
        - `title` が URL の場合：Playwright でタイトル＋全文を取得し、404 はスキップします。  
        - 本文は 2000文字ごとに分割して複数の段落ブロックを生成します。  
        """
    )
else:
    # property_names が空の場合は、この行の下でメッセージを表示する
    mapping = {col: "― 未選択 ―" for col in csv_columns}
    st.info("まずはサイドバーで Notion API キーとデータベースIDを入力してください。")

# 3. CSV ファイルアップロード
st.markdown("### STEP2: CSV ファイルを選択して Notion に登録")
uploaded_file = st.file_uploader("Pocket からエクスポートした CSV を選択", type=["csv"])
df = None
total_rows = 0

if uploaded_file:
    try:
        df = pd.read_csv(uploaded_file)
        st.write("### CSV プレビュー")
        st.dataframe(df.head())
        total_rows = len(df)
        st.write(f"行数（ヘッダー除く）： {total_rows}")
    except Exception as e:
        st.error(f"CSV 読み込みエラー: {e}")

# 4. CSV 登録処理
if uploaded_file and notion_token and database_id and property_names:
    if st.button("CSV を Notion に登録"):
        progress_bar = st.progress(0)
        status_text = st.empty()
        success_count = 0
        failure_count = 0
        skipped_count = 0
        notion_client = Client(auth=notion_token)

        for idx, row in enumerate(df.itertuples(index=False), start=1):
            raw_title  = getattr(row, "title")
            url_val    = getattr(row, "url")
            time_val   = getattr(row, "time_added")
            tags_val   = getattr(row, "tags")
            status_val = getattr(row, "status")

            scraped_title = raw_title
            body_text     = ""
            skip_this     = False

            if isinstance(raw_title, str) and raw_title.startswith(("http://", "https://")):
                info        = fetch_page_info_sync(raw_title)
                status_code = info.get("status")

                if status_code == 404:
                    st.warning(f"行 {idx}: URL が 404 です。スキップ → {raw_title}")
                    skipped_count += 1
                    skip_this     = True
                else:
                    scraped_title = info.get("title") or "No Title"
                    if register_body:
                        body_text = info.get("body", "")

            if skip_this:
                status_text.text(f"処理済み: {idx} / {total_rows}")
                progress_bar.progress(min(idx / total_rows, 1.0))
                continue

            property_json = {}
            if mapping.get("title") and mapping["title"] != "― 未選択 ―":
                property_json[mapping["title"]] = build_title_property(scraped_title)
            if mapping.get("url") and mapping["url"] != "― 未選択 ―":
                property_json[mapping["url"]] = build_url_property(url_val)

            if mapping.get("time_added") and mapping["time_added"] != "― 未選択 ―":
                prop_info = properties.get(mapping["time_added"], {})
                if prop_info.get("type") == "date":
                    try:
                        unix_ts = int(time_val)
                        property_json[mapping["time_added"]] = build_date_property(unix_ts)
                    except:
                        property_json[mapping["time_added"]] = {"date": None}
                else:
                    st.warning(f"行 {idx}: 「{mapping['time_added']}」は Date 型ではありません。")
            if mapping.get("tags") and mapping["tags"] != "― 未選択 ―":
                property_json[mapping["tags"]] = build_multi_select_property(tags_val)
            if mapping.get("status") and mapping["status"] != "― 未選択 ―":
                property_json[mapping["status"]] = build_select_property(status_val)

            children_blocks = []
            if register_body and body_text:
                paragraph_blocks = split_text_to_paragraph_blocks(body_text)
                children_blocks.extend(paragraph_blocks)

            try:
                create_notion_page(notion_client, database_id, property_json, children_blocks)
                success_count += 1
            except Exception as e:
                failure_count += 1
                st.write(f"行 {idx} 登録エラー: {e}")

            status_text.text(f"処理済み: {idx} / {total_rows}")
            progress_bar.progress(min(idx / total_rows, 1.0))

        st.success(f"登録完了：成功 {success_count} 件、失敗 {failure_count} 件、スキップ {skipped_count} 件")
else:
    # ここでは特に何も表示せずに済みます
    pass