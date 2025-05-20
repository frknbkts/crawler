import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin
import datetime
import hashlib
from elasticsearch import Elasticsearch, exceptions as es_exceptions

# --- Global Ayarlar ---
TARGET_URL = "https://www.sozcu.com.tr/"
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
}
FETCH_CONTENT_LIMIT = None
ES_HOST = "http://localhost:9200"
ES_INDEX_NAME = "sozcu_articles_simple"


def connect_elasticsearch():
    """Elasticsearch'e (versiyon 8.x varsayımıyla) bağlanır."""
    print(f"ℹ️ Elasticsearch bağlantısı deneniyor: {ES_HOST}")
    try:
        es_client = Elasticsearch(ES_HOST, request_timeout=30, max_retries=3, retry_on_timeout=True)
        if es_client.ping():
            print(f"✅ Elasticsearch bağlantısı başarılı!")
            return es_client
        else:
            print(f"❌ Ping başarısız. Sunucu {ES_HOST} çalışıyor mu?")
            return None
    except ConnectionRefusedError:
        print(f"❌ Bağlantı reddedildi. Sunucu {ES_HOST} aktif değil veya erişilemiyor.")
    except es_exceptions.ConnectionError as e_conn:
        print(f"❌ Elasticsearch Kütüphanesi Bağlantı Hatası: {e_conn}")
    except Exception as e:
        print(f"❌ Elasticsearch bağlantısı sırasında genel bir hata oluştu: {e}")
    return None


def create_index_if_not_exists(es_client, index_name):
    """Belirtilen Elasticsearch indeksi yoksa, sadeleştirilmiş alan eşlemeleriyle oluşturur."""
    try:
        if not es_client.indices.exists(index=index_name):
            print(f"ℹ️ '{index_name}' indeksi bulunamadı. Oluşturuluyor...")
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
            print(f"✅ '{index_name}' indeksi oluşturuldu.")
        else:
            print(f"ℹ️ '{index_name}' indeksi zaten var.")
    except Exception as e:
        print(f"❌ Index kontrolü/oluşturulması sırasında hata: {e}")


def fetch_article_content(article_url):
    """Verilen URL'den haberin özetini ve ana metnini çeker."""
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
        print(f"        ❌ İçerik çekme/işleme hatası ({article_url}): {e}")
        return ""


def fetch_sozcu_main_page_articles():
    """Sözcü ana sayfasından haberleri çeker."""
    print(f"📰 Ana sayfadan haberler çekiliyor: {TARGET_URL}")
    try:
        response = requests.get(TARGET_URL, headers=HEADERS, timeout=15)
        response.raise_for_status()
        soup = BeautifulSoup(response.content, 'html.parser')
    except requests.exceptions.RequestException as e:
        print(f"❌ Ana sayfa ({TARGET_URL}) çekme hatası: {e}")
        return []

    articles_data = []
    news_blocks = soup.find_all('div', class_='news-card')
    if not news_blocks:
        print("❌ 'news-card' class'ına sahip haber bloğu bulunamadı. Site yapısı değişmiş olabilir.")
        return []
    print(f"🔍 {len(news_blocks)} potansiyel haber bloğu ('news-card') bulundu.")

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
                            print(f"  ℹ️ İçerik çekme limitine ({FETCH_CONTENT_LIMIT}) ulaşıldı.")
                            content_fetched_count += 1

                        current_time_utc = datetime.datetime.now(datetime.timezone.utc).isoformat()
                        articles_data.append({
                            'title': title,
                            'url': absolute_url,
                            'content': article_content if article_content else "İçerik bulunamadı veya çekilemedi.",
                            'source': 'sozcu.com.tr',
                            'scraped_date_utc': current_time_utc
                        })
        except Exception as e:
            print(f"  ⚠️ Haber bloğu işleme hatası (#{block_index+1}): {e}")
            continue

    if not articles_data: print("❌ Filtreleme sonrası geçerli haber bulunamadı.")
    return articles_data


def index_articles_to_elasticsearch(es_client, articles_to_index, index_name):
    """Toplanan haberleri Elasticsearch'e kaydeder."""
    if not es_client:
        print("❌ Elasticsearch bağlantısı aktif değil. Veriler kaydedilemedi.")
        return False

    print(f"\n📊 Elasticsearch'e veri gönderiliyor (İndeks: {index_name})...")
    success_count, fail_count = 0, 0

    for article_document in articles_to_index:
        if not article_document.get('content') or article_document.get('content') == "İçerik bulunamadı veya çekilemedi.":
            continue

        try:
            doc_id = hashlib.sha256(article_document['url'].encode('utf-8')).hexdigest()
            document_payload = article_document.copy()
            document_payload['indexed_at_utc'] = datetime.datetime.now(datetime.timezone.utc).isoformat()
            resp = es_client.index(index=index_name, id=doc_id, document=document_payload)
            success_count += 1
        except Exception as e:
            print(f"  ❌ Kayıt hatası: '{article_document.get('title', 'Başlıksız')[:30]}...' -> {e}")
            fail_count += 1

    print(f"📈 ES Kayıt: {success_count} başarılı, {fail_count} başarısız.")
    return success_count > 0


if __name__ == "__main__":
    print("🚀 Sözcü Haber Çekme Aracı Başlatılıyor")

    es_client_instance = connect_elasticsearch()

    if es_client_instance:
        create_index_if_not_exists(es_client_instance, ES_INDEX_NAME)

    retrieved_articles_list = fetch_sozcu_main_page_articles()

    if retrieved_articles_list:
        print(f"\n📰 Çekme işlemi tamamlandı. Toplam {len(retrieved_articles_list)} haber başlığı/URL'i bulundu.")

        if es_client_instance:
            index_articles_to_elasticsearch(es_client_instance, retrieved_articles_list, ES_INDEX_NAME)
        else:
            print("\n⚠️ Elasticsearch bağlantısı kurulamadığı için veriler kaydedilemedi.")
    else:
        print("❌ Ana sayfadan hiç haber çekilemedi veya işlenemedi.")

    print("\n🏁 İşlem tamamlandı")