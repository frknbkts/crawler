import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin
import datetime
import hashlib
from elasticsearch import Elasticsearch, exceptions as es_exceptions

TARGET_URL = "https://www.sozcu.com.tr/"
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
}
FETCH_CONTENT_LIMIT = None
ES_HOST = "http://localhost:9200"
ES_INDEX_NAME = "sozcu_articles"

def connect_elasticsearch():
    print(f" Elasticsearch baglantisi deneniyor: {ES_HOST}")
    try:
        es_client = Elasticsearch(ES_HOST, request_timeout=30, max_retries=3, retry_on_timeout=True)
        if es_client.ping():
            print(f" Elasticsearch baglantisi basarili!")
            return es_client
        else:
            print(f" Ping basarisiz. Sunucu {ES_HOST} calisiyor mu?")
            return None
    except ConnectionRefusedError:
        print(f" Baglanti reddedildi. Sunucu {ES_HOST} aktif degil veya erisilemiyor.")
    except es_exceptions.ConnectionError as e_conn:
        print(f" Elasticsearch Kutuphanesi Baglanti Hatasi: {e_conn}")
    except Exception as e:
        print(f" Elasticsearch baglantisi sirasinda genel bir hata olustu: {e}")
    return None

def create_index_if_not_exists(es_client, index_name):
    try:
        if not es_client.indices.exists(index=index_name):
            print(f" '{index_name}' indeksi bulunamadi. Olusturuluyor...")
            index_settings = {
                "settings": {"number_of_shards": 1, "number_of_replicas": 0},
                "mappings": {
                    "properties": {
                        "title": {"type": "text"},
                        "content": {"type": "text"},
                        "url": {"type": "keyword"},
                        "source": {"type": "keyword"},
                        "scraped_date_utc": {"type": "date"},
                        "indexed_at_utc": {"type": "date"}
                    }
                }
            }
            es_client.indices.create(index=index_name, body=index_settings)
            print(f" '{index_name}' indeksi olusturuldu.")
        else:
            print(f" '{index_name}' indeksi zaten var.")
    except Exception as e:
        print(f" Index kontrolu/olusturulmasi sirasinda hata: {e}")

def fetch_article_content(article_url):
    all_content_parts = []
    try:
        response = requests.get(article_url, headers=HEADERS, timeout=15)
        response.raise_for_status()
        soup = BeautifulSoup(response.content, 'html.parser')

        summary_h2_tag = soup.select_one('h2.description.mb-4.fw-medium.fs-5.lh-base')
        if summary_h2_tag:
            summary_text = summary_h2_tag.get_text(strip=True)
            if summary_text:
                all_content_parts.append(summary_text)

        content_container = soup.find('div', class_='article-body')
        if content_container:
            paragraphs = content_container.find_all('p')
            main_article_text_parts = []
            for p_tag in paragraphs:
                text = p_tag.get_text(strip=True)
                if not text or "İLGİNİZİ ÇEKEBİLİR" in text.upper() or p_tag.find('script'):
                    continue
                main_article_text_parts.append(text)
            if main_article_text_parts:
                all_content_parts.append("\n\n".join(main_article_text_parts))

        if all_content_parts:
            return "\n\n".join(all_content_parts)
        else:
            return ""
    except Exception as e: # Hem RequestException hem de diğer parse hatalarını yakalar
        print(f"         Icerik cekme/isleme hatasi ({article_url}): {e}")
        return ""

def fetch_sozcu_main_page_articles():
    print(f" Ana sayfadan haberler cekiliyor: {TARGET_URL}")
    try:
        response = requests.get(TARGET_URL, headers=HEADERS, timeout=15)
        response.raise_for_status()
        soup = BeautifulSoup(response.content, 'html.parser')
    except requests.exceptions.RequestException as e:
        print(f" Ana sayfa ({TARGET_URL}) cekme hatasi: {e}")
        return []

    articles_data = []
    news_blocks = soup.find_all('div', class_='news-card')
    if not news_blocks:
        print(" 'news-card' class'ina sahip haber blogu bulunamadi. Site yapisi degismis olabilir.")
        return []
    print(f" {len(news_blocks)} potansiyel haber blogu ('news-card') bulundu.")

    content_fetched_count = 0
    processed_urls = set()

    for block_index, block in enumerate(news_blocks):
        try:
            title, relative_url = None, None
            footer_link_tag = block.find('a', class_='news-card-footer', href=True)
            if footer_link_tag:
                title = footer_link_tag.get_text(strip=True)
                relative_url = footer_link_tag['href']
            else:
                img_holder_link_tag = block.find('a', class_='img-holder', href=True)
                if img_holder_link_tag:
                    img_tag = img_holder_link_tag.find('img', alt=True)
                    if img_tag and img_tag.get('alt'):
                        title = img_tag['alt'].strip()
                        relative_url = img_holder_link_tag['href']

            if title and relative_url:
                title = title.strip()
                absolute_url = urljoin(TARGET_URL, relative_url.strip())

                excluded_paths = ["/kategori/", "/yazarlar/", "/etiket/", "/foto-analiz/",
                                  "/foto-galeri/", "/video/", "javascript:void(0)"]
                if "sozcu.com.tr/" in absolute_url and \
                   not absolute_url.startswith("https://bit.ly") and \
                   not any(excluded in absolute_url for excluded in excluded_paths):

                    if absolute_url not in processed_urls:
                        processed_urls.add(absolute_url)
                        print(f"   H#{len(articles_data) + 1}: {title[:60]}...")

                        article_content = ""
                        if FETCH_CONTENT_LIMIT is None or content_fetched_count < FETCH_CONTENT_LIMIT:
                            article_content = fetch_article_content(absolute_url)
                            if article_content:
                                content_fetched_count += 1
                        elif content_fetched_count == FETCH_CONTENT_LIMIT and FETCH_CONTENT_LIMIT is not None:
                            print(f"   Icerik cekme limitine ({FETCH_CONTENT_LIMIT}) ulasildi.")
                            content_fetched_count += 1

                        current_time_utc = datetime.datetime.now(datetime.timezone.utc).isoformat()
                        articles_data.append({
                            'title': title,
                            'url': absolute_url,
                            'content': article_content if article_content else "Icerik bulunamadi veya cekilemedi.",
                            'source': 'sozcu.com.tr',
                            'scraped_date_utc': current_time_utc
                        })
        except Exception as e:
            print(f"   Haber blogu işleme hatasi (#{block_index+1}): {e}")
            continue

    if not articles_data: print(" Filtreleme sonrasi gecerli haber bulunamadi.")
    return articles_data


def index_articles_to_elasticsearch(es_client, articles_to_index, index_name):
    if not es_client:
        print(" Elasticsearch baglantisi aktif degil. Veriler kaydedilemedi.")
        return False

    print(f"\n Elasticsearch'e veri gönderiliyor (Indeks: {index_name})...")
    success_count, fail_count = 0, 0

    for article_document in articles_to_index:
        if not article_document.get('content') or article_document.get('content') == "Icerik bulunamadi veya cekilemedi.":
            continue

        try:
            doc_id = hashlib.sha256(article_document['url'].encode('utf-8')).hexdigest()
            document_payload = article_document.copy()
            document_payload['indexed_at_utc'] = datetime.datetime.now(datetime.timezone.utc).isoformat()
            resp = es_client.index(index=index_name, id=doc_id, document=document_payload)
            success_count += 1
        except Exception as e:
            print(f"   Kayit hatasi: '{article_document.get('title', 'Basliksiz')[:30]}...' -> {e}")
            fail_count += 1

    print(f" ES Kayit: {success_count} basarili, {fail_count} basarisiz.")
    return success_count > 0


if __name__ == "__main__":
    print(" Sozcu Haber Cekme Islemi Baslatiliyor")

    es_client_instance = connect_elasticsearch()

    if es_client_instance:
        create_index_if_not_exists(es_client_instance, ES_INDEX_NAME)

    retrieved_articles_list = fetch_sozcu_main_page_articles()

    if retrieved_articles_list:
        print(f"\n Cekme islemi tamamlandi. Toplam {len(retrieved_articles_list)} haber basligi/URL'i bulundu.")

        if es_client_instance:
            index_articles_to_elasticsearch(es_client_instance, retrieved_articles_list, ES_INDEX_NAME)
        else:
            print("\n Elasticsearch baglantisi kurulamadigi icin veriler kaydedilemedi.")
    else:
        print(" Ana sayfadan hic haber cekilemedi veya islenemedi.")

    print("\n İslem tamamlandi")