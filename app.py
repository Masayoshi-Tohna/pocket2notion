import streamlit as st
import pandas as pd
import asyncio
from playwright.async_api import async_playwright
from notion_client import Client
from datetime import datetime, timezone
from bs4 import BeautifulSoup # ★★★ BeautifulSoupをインポート ★★★

# ----- Playwright でページ情報（ステータス/タイトル/全文）を取得する非同期関数（修正・ログ追加版） -----
async def fetch_page_info(url: str):
    result = {"status": None, "title": "No Title", "body": ""}
    print(f"🚀 [Scraping Start] URL: {url}")

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
                print(f"  📄 [{url}] Navigating...")
                # タイムアウトを少し延長し、waitUntilをdomcontentloadedにしてみる（サイトによる）
                response = await page.goto(url, timeout=20000, wait_until="domcontentloaded")
            except Exception as e:
                print(f"  ❌ [{url}] Navigation error: {e}")
                result["status"] = f"Navigation Error: {type(e).__name__}"
                await browser.close()
                return result

            if response is None:
                print(f"  ❌ [{url}] No response object received.")
                result["status"] = "No Response"
                await browser.close()
                return result

            status = response.status
            result["status"] = status
            print(f"  ℹ️ [{url}] HTTP Status: {status}")

            if status == 404:
                print(f"  ⏭️ [{url}] Page returned 404, skipping further processing.")
                await browser.close()
                return result
            
            # 400以上のエラーだが404ではない場合もログに残す
            if status >= 400:
                print(f"  ⚠️ [{url}] Page returned status {status}. Attempting to get title anyway.")

            try:
                # networkidleは時間がかかることがあるので、DOMの準備完了を優先
                await page.wait_for_load_state("load", timeout=15000) # "load"イベントを待つ
                print(f"  ⏳ [{url}] Waited for load state. Adding small delay...")
                await asyncio.sleep(2.5)  # JS実行のための追加待機を少し延長
            except Exception as e_wait:
                print(f"  ⚠️ [{url}] Wait for load state timeout/error: {e_wait}. Proceeding.")
                pass

            try:
                title = await page.title()
                result["title"] = title.strip() if title and title.strip() else "No Title"
                print(f"  🏷️ [{url}] Title: {result['title']}")
            except Exception as e_title:
                print(f"  ❌ [{url}] Error getting title: {e_title}")
                result["title"] = "Error Getting Title"

            # 本文取得
            page_body_text = ""
            try:
                print(f"  💬 [{url}] Attempting to get body via page.evaluate (innerText/textContent)...")
                page_body_text = await page.evaluate("""
                    () => {
                        const body = document.body;
                        if (!body) return '';
                        return body.innerText || body.textContent || '';
                    }
                """)
                if page_body_text and page_body_text.strip():
                    result["body"] = page_body_text.strip()
                    print(f"  ✅ [{url}] Got body via page.evaluate. Length: {len(result['body'])}. Preview: '{result['body'][:100]}...'")
                else:
                    print(f"  ⚠️ [{url}] page.evaluate returned empty. Falling back to BeautifulSoup.")
                    raise ValueError("Empty innerText/textContent from page.evaluate")
            except Exception as e_evaluate:
                print(f"  ↪️ [{url}] Failed or empty from page.evaluate ({e_evaluate}). Trying BeautifulSoup.")
                try:
                    html_content = await page.content()
                    soup = BeautifulSoup(html_content, "html.parser")
                    
                    body_tag = soup.find('body')
                    if body_tag:
                        result["body"] = body_tag.get_text(separator="\n", strip=True)
                    else: 
                        result["body"] = soup.get_text(separator="\n", strip=True)

                    if result["body"]:
                        print(f"  ✅ [{url}] Got body via BeautifulSoup. Length: {len(result['body'])}. Preview: '{result['body'][:100]}...'")
                    else:
                        print(f"  ❌ [{url}] BeautifulSoup also resulted in empty body.")
                        
                except Exception as e_bs:
                    print(f"  ❌ [{url}] Error getting body with BeautifulSoup: {e_bs}")
                    result["body"] = ""

            await browser.close()
            print(f"🏁 [Scraping End] URL: {url}. Final body length: {len(result['body'])}")
            return result

    except Exception as e_outer:
        print(f"💥 [Scraping Fatal Error] URL: {url}, Error: {e_outer}")
        result["status"] = f"Outer Exception: {type(e_outer).__name__}"
        # browserが初期化されているか不明なため、ここではcloseを呼ばない（Playwrightが管理）
        return result

def fetch_page_info_sync(url: str):
    return asyncio.run(fetch_page_info(url))

# ----- 本文を 2000 文字ごとに分割して Paragraph ブロックへ変換する関数（超堅牢版） -----
def split_text_to_paragraph_blocks(text: str) -> list:
    # max_chunk_len_api = 2000  # Notion APIの制限
    max_chunk_len_api = 1900  # 安全を見た値（2000文字はギリギリなので、1900に設定）
    children = []
    
    if not text or not text.strip():
        print("  [Splitter] Input text is empty or whitespace. Returning no blocks.")
        return children

    print(f"  [Splitter] Original text length: {len(text)}")
    
    current_pos = 0
    text_len = len(text)
    block_num = 0

    while current_pos < text_len:
        block_num += 1
        # 1. このブロックで切り出す文字列の長さを決定
        #    残り文字数がmax_chunk_len_apiより少なければ、残り全部。そうでなければmax_chunk_len_api。
        chunk_len_to_slice = min(max_chunk_len_api, text_len - current_pos)
        
        # 2. 文字列をスライス
        #    current_pos から current_pos + chunk_len_to_slice まで
        chunk = text[current_pos : current_pos + chunk_len_to_slice]
        
        # 3. スライスしたチャンクの長さを検証 (Pythonのlen()で)
        actual_chunk_len = len(chunk)
        
        print(f"  [Splitter Block {block_num}] Attempting slice: text[{current_pos}:{current_pos + chunk_len_to_slice}]")
        print(f"  [Splitter Block {block_num}] Expected slice length: {chunk_len_to_slice}, Actual sliced chunk length: {actual_chunk_len}")

        # 4. 万が一、スライス結果が期待より長い場合 (またはAPI制限を超える場合) は強制トリム
        #    この状況は通常ありえないはずだが、APIエラーを防ぐために絶対的な保証を加える
        if actual_chunk_len > max_chunk_len_api:
            print(f"  [Splitter Block {block_num}] WARNING: Sliced chunk ({actual_chunk_len}) is longer than API limit ({max_chunk_len_api}). Forcibly trimming.")
            chunk = chunk[:max_chunk_len_api]
            actual_chunk_len = len(chunk) # 再度長さを取得
            print(f"  [Splitter Block {block_num}] Forcibly trimmed chunk length: {actual_chunk_len}")
        
        # 5. さらに念のため、それでもAPI制限を超えていたらエラーを出して少し短くする
        if actual_chunk_len > max_chunk_len_api:
            # このエラーログが出たら、Pythonの文字列スライスかlen()の挙動に未知の問題がある可能性が高い
            critical_error_msg = (
                f"  [Splitter Block {block_num}] CRITICAL ERROR: Chunk length ({actual_chunk_len}) "
                f"STILL EXCEEDS API limit ({max_chunk_len_api}) after forced trim. "
                f"This should NOT happen. Reducing length further as an emergency measure."
            )
            print(critical_error_msg)
            # st.error(critical_error_msg) # UIにも表示した方が良いかもしれない
            
            # 緊急避難的にAPI制限より少し短くする (例: 1990文字)
            safer_limit = max_chunk_len_api - 10 
            chunk = chunk[:safer_limit]
            actual_chunk_len = len(chunk)
            print(f"  [Splitter Block {block_num}] Emergency trimmed chunk length: {actual_chunk_len}")


        # 6. 空のチャンクや空白のみのチャンクは追加しない
        if not chunk.strip():
            print(f"  [Splitter Block {block_num}] Chunk is empty or whitespace. Skipping.")
            current_pos += chunk_len_to_slice # スライスしようとした分だけ進む
            continue

        # 7. Notionブロックを作成
        #    この時点で chunk の長さは絶対に max_chunk_len_api 以下のはず
        if len(chunk) > max_chunk_len_api : # ここで再度チェック
             print(f"   [Splitter Block {block_num}] FATAL PRE-COMMIT CHECK: Chunk len {len(chunk)} > {max_chunk_len_api}")
             # ここでエラーにするか、再度トリムするか
             chunk = chunk[:max_chunk_len_api]


        block = {
            "object": "block",
            "type": "paragraph",
            "paragraph": {
                "rich_text": [{"type": "text", "text": {"content": chunk}}]
            }
        }
        children.append(block)
        
        # 8. 次の開始位置を更新
        current_pos += actual_chunk_len # 実際に処理したチャンクの長さだけ進む
        
        print(f"  [Splitter Block {block_num}] Added block. Content length: {actual_chunk_len}. Next current_pos: {current_pos}")

    print(f"  [Splitter] Total blocks created: {len(children)}")
    # 関数を抜ける直前に全ブロックの長さを最終確認
    for i, child_block in enumerate(children):
        final_content_len = len(child_block['paragraph']['rich_text'][0]['text']['content'])
        print(f"    [Splitter Final Check - Block {i+1}] Content length: {final_content_len}")
        if final_content_len > max_chunk_len_api:
            print(f"    [Splitter Final Check - Block {i+1}] FATAL ERROR: CONTENT STILL TOO LONG ({final_content_len}) BEFORE RETURNING LIST!")
            # ここでエラーが発生する場合、この関数内のロジックに根本的な欠陥がある
            # もしくは、appendした後にchunkが何らかの形で変更されている（Pythonでは通常考えにくい）

    return children

# ----- Notion 用ユーティリティ -----
def build_title_property(col_value: str):
    return {"title": [{"text": {"content": col_value}}]}

def build_url_property(col_value: str):
    return {"url": col_value}

def build_date_property(unix_ts: int):
    dt = datetime.fromtimestamp(unix_ts, timezone.utc)
    date_str = dt.strftime("%Y-%m-%d")
    return {"date": {"start": date_str}}

def build_multi_select_property(tag_str: str):
    if not isinstance(tag_str, str) or not tag_str.strip(): # str型であること、空でないことを確認
        return {"multi_select": []}
    tags = [t.strip() for t in tag_str.split(",") if t.strip()]
    return {"multi_select": [{"name": tag} for tag in tags]}

def build_select_property(status_value: str):
    if not status_value: # status_valueがNoneや空文字の場合
        return {"select": None}
    return {"select": {"name": str(status_value)}} # 念のためstrにキャスト

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
        2. [Pocket公式サイト](https://getpocket.com/export)からエクスポートしたCSVを選ぶ  
           → 表のプレビューと行数が表示されます

        ---

        ### 📝 Notionに登録

        1. マッピングを確認して  
        2. 「CSVをNotionに登録」ボタンをクリック！  
           - Pocketの`title`列がURL形式の場合 **ではなく** 、Pocketの`url`列のURLを元にスクレイピングでページタイトル＋全文を取得します。
           - HTTP ステータス404ならその行をスキップします。  
           - `time_added` は日付型プロパティ（例：Added Date）に変換して登録します。  
           - 最後に「成功/失敗/スキップ数」が表示されます。
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

mapping = {}
csv_columns = ["title", "url", "time_added", "tags", "status"]

st.markdown("### STEP1: CSV カラムと Notion プロパティの紐づけ設定")

if property_names:
    cols = st.columns(5)
    with cols[0]:
        mapping["title"] = st.selectbox("Pocketの「title」", options=["― 未選択 ―"] + property_names, key="map_title")
    with cols[1]:
        mapping["url"] = st.selectbox("Pocketの「url」", options=["― 未選択 ―"] + property_names, key="map_url")
    with cols[2]:
        mapping["time_added"] = st.selectbox("Pocket の「time_added」", options=["― 未選択 ―"] + property_names, key="map_time")
    with cols[3]:
        mapping["tags"] = st.selectbox("Pocket の「tags」", options=["― 未選択 ―"] + property_names, key="map_tags")
    with cols[4]:
        mapping["status"] = st.selectbox("Pocket の「status」", options=["― 未選択 ―"] + property_names, key="map_status")
    st.markdown(
        """
        **注意点**  
        - `time_added` は Notion の「Created Time」に直接マッピングできません。  
        - 必要に応じて Date 型のカスタムプロパティ（例：`Added Date`）を作成してください。  
        - Pocketの `url` 列のURLを元にPlaywrightでタイトル＋全文を取得し、404はスキップします。
        - 本文は 2000文字ごとに分割して複数の段落ブロックを生成します。  
        """
    )
else:
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
            # getattrで列が存在しない場合に備え、デフォルト値を設定
            raw_title  = getattr(row, "title", "タイトル不明") # CSVのタイトル列
            url_val    = getattr(row, "url", None)      # CSVのURL列
            time_val   = getattr(row, "time_added", None)
            tags_val   = getattr(row, "tags", "")       # タグは空文字をデフォルトに
            status_val = str(getattr(row, "status", "")) # statusは文字列として扱う

            current_title_for_notion = raw_title # Notion登録用のタイトル。スクレイピング成功なら上書き
            body_text = ""
            skip_this = False
            
            print(f"\n--- Processing row {idx + 1} (CSV row {idx}) ---")
            print(f"  CSV Data: Title='{raw_title}', URL='{url_val}'")

            # URL列が存在し、それが有効なURL形式の場合にスクレイピングを試みる
            if url_val and isinstance(url_val, str) and url_val.startswith(("http://", "https://")):
                st.write(f"行 {idx}: スクレイピング開始 → {url_val}")
                print(f"  Scraping URL: {url_val}")
                
                info = fetch_page_info_sync(url_val) # ターミナルに詳細ログが出力される
                
                page_status_code = info.get("status")

                if page_status_code == 404:
                    st.warning(f"行 {idx}: URLが404です。スキップ → {url_val}")
                    print(f"  SKIPPING: URL returned 404.")
                    skipped_count += 1
                    skip_this = True
                elif isinstance(page_status_code, str) and ("Error" in page_status_code or "No Response" in page_status_code):
                    st.warning(f"行 {idx}: スクレイピングエラー ({page_status_code})。本文なし、CSVタイトルで続行 → {url_val}")
                    print(f"  WARNING: Scraping error ({page_status_code}). Proceeding without body, using CSV title.")
                    body_text = ""
                    # current_title_for_notion は raw_title のまま
                else: # 成功または404以外のエラーステータス
                    # スクレイピングで取得したタイトルがあればそれを使用
                    if info.get("title") and info.get("title") != "No Title" and info.get("title") != "Error Getting Title":
                        current_title_for_notion = info.get("title")
                        print(f"  Scraped Title: '{current_title_for_notion}'")
                    else:
                        print(f"  WARNING: Scraped title was '{info.get('title', 'N/A')}', using CSV title '{raw_title}'.")
                        # current_title_for_notion は raw_title のまま

                    if register_body:
                        body_text = info.get("body", "")
                        print(f"  Fetched body length: {len(body_text)}")
                        if not body_text:
                            st.warning(f"行 {idx}: 本文が取得できませんでした。URL: {url_val}")
                        # else: # Streamlit UIへの大量出力は避ける
                        #    st.write(f"行 {idx}: 取得本文（先頭50文字）: '{body_text[:50]}...'")
            elif not url_val or not isinstance(url_val, str) or not url_val.startswith(("http://", "https://")):
                st.warning(f"行 {idx}: 有効なURLがCSVのurl列にありません。スキップまたはCSVタイトルのみ使用。 URL: '{url_val}'")
                print(f"  WARNING: Invalid or missing URL in CSV 'url' column: '{url_val}'. Using CSV title, no scraping.")
                # current_title_for_notion は raw_title のまま、body_text は空のまま

            if skip_this:
                status_text.text(f"処理済み: {idx} / {total_rows}")
                progress_bar.progress(min(idx / total_rows, 1.0))
                continue

            # NotionプロパティJSONの構築
            property_json = {}
            if mapping.get("title") and mapping["title"] != "― 未選択 ―":
                # タイトルが長すぎる場合、Notionの制限(2000文字)を考慮 (APIレベルでの制限は不明だが念のため)
                final_title = current_title_for_notion if current_title_for_notion and current_title_for_notion.strip() else "タイトルなし"
                property_json[mapping["title"]] = build_title_property(final_title[:1990]) # 少し短めに
            
            if mapping.get("url") and mapping["url"] != "― 未選択 ―" and url_val:
                property_json[mapping["url"]] = build_url_property(url_val)

            if mapping.get("time_added") and mapping["time_added"] != "― 未選択 ―" and time_val is not None:
                prop_info = properties.get(mapping["time_added"], {})
                if prop_info.get("type") == "date":
                    try:
                        unix_ts = int(time_val)
                        property_json[mapping["time_added"]] = build_date_property(unix_ts)
                    except (ValueError, TypeError):
                        st.warning(f"行 {idx}: time_added ('{time_val}') をUnixタイムスタンプに変換できませんでした。")
                        property_json[mapping["time_added"]] = {"date": None}
                else:
                    st.warning(f"行 {idx}: 「{mapping['time_added']}」は Date 型ではありません。time_added の登録をスキップします。")
            
            if mapping.get("tags") and mapping["tags"] != "― 未選択 ―":
                property_json[mapping["tags"]] = build_multi_select_property(tags_val)
            
            if mapping.get("status") and mapping["status"] != "― 未選択 ―":
                property_json[mapping["status"]] = build_select_property(status_val)

            # 本文ブロックの準備
            children_blocks = []
            if register_body and body_text and body_text.strip():
                print(f"  Splitting body text (length: {len(body_text)}) into paragraph blocks.")
                paragraph_blocks = split_text_to_paragraph_blocks(body_text)
                children_blocks.extend(paragraph_blocks)
                print(f"  Number of paragraph blocks created: {len(children_blocks)}")
                if not children_blocks:
                     print(f"  WARNING: Body text existed, but no paragraph blocks were generated.")
            elif register_body:
                print(f"  Register body is ON, but body_text is empty or invalid. No children blocks will be created.")

            if len(children_blocks) > 3: # エラーが children[3] で起きているので
                problematic_block_content = children_blocks[3]['paragraph']['rich_text'][0]['text']['content']
                print(f"  [DEBUG] Content of children_blocks[3] (length {len(problematic_block_content)}):")
                print(f"    '{problematic_block_content}'") # 全文出力
                print(f"    Last 20 chars: '{problematic_block_content[-20:]}'")
                # 必要であれば、バイナリ表現も確認
                # print(f"    Last 20 chars (bytes): {problematic_block_content[-20:].encode('utf-8')}")

            # Notionページ作成
            try:
                print(f"  Attempting to create Notion page. Properties: {list(property_json.keys())}, Children blocks: {len(children_blocks)}")
                create_notion_page(notion_client, database_id, property_json, children_blocks)
                success_count += 1
                print(f"  SUCCESS: Notion page created for '{current_title_for_notion[:50]}...'")
            except Exception as e:
                failure_count += 1
                error_message = f"行 {idx} 登録エラー: {e}"
                st.error(error_message) # Streamlit UIにもエラー表示
                print(f"  FAILURE: Error creating Notion page: {e}")
                # デバッグ用に詳細情報をログに出力
                print(f"    Failed properties: {property_json}")
                # print(f"    Failed children (first block if any): {children_blocks[0] if children_blocks else 'No children'}")


            status_text.text(f"処理済み: {idx} / {total_rows}")
            progress_bar.progress(min(idx / total_rows, 1.0))

        st.success(f"登録完了：成功 {success_count} 件、失敗 {failure_count} 件、スキップ {skipped_count} 件")
        print(f"\n--- Processing Complete ---")
        print(f"Total: {total_rows}, Success: {success_count}, Failure: {failure_count}, Skipped: {skipped_count}")
else:
    if uploaded_file and not (notion_token and database_id and property_names):
        st.warning("Notion の認証情報またはデータベース情報が正しく設定されていないため、登録処理を開始できません。サイドバーを確認してください。")