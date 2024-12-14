import logging
import os
from dotenv import load_dotenv
import requests
import firebase_admin
from firebase_admin import credentials, storage, firestore
from PIL import Image
from urllib.parse import urlparse
import tempfile
import re

# Konfiguracja logowania
logging.basicConfig(
    level=logging.INFO,  # Możliwe poziomy: DEBUG, INFO, WARNING, ERROR, CRITICAL
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("nasa_image_loader.log"),  # Logi do pliku
        logging.StreamHandler()  # Logi na konsolę
    ]
)

# Załaduj zmienne z pliku .env
load_dotenv()

NASA_API_KEY = os.environ.get("NASA_API_KEY")
if not NASA_API_KEY:
    logging.error("Brak wartości dla NASA_API_KEY. Upewnij się, że plik .env jest poprawnie skonfigurowany.")
    raise ValueError("Brak wartości dla NASA_API_KEY. Upewnij się, że plik .env jest poprawnie skonfigurowany.")

FIREBASE_BUCKET = os.environ.get("FIREBASE_BUCKET")
if not FIREBASE_BUCKET:
    logging.error("Brak wartości dla FIREBASE_BUCKET. Dodaj FIREBASE_BUCKET do pliku .env.")
    raise ValueError("Brak wartości dla FIREBASE_BUCKET. Dodaj FIREBASE_BUCKET do pliku .env.")

SERVICE_ACCOUNT_PATH = "serviceAccountKey.json"

# Inicjalizacja Firebase Admin SDK
try:
    cred = credentials.Certificate(SERVICE_ACCOUNT_PATH)
    firebase_admin.initialize_app(cred, {
        'storageBucket': FIREBASE_BUCKET
    })
    logging.info("Firebase Admin SDK zainicjalizowane pomyślnie.")
except Exception as e:
    logging.critical(f"Nie udało się zainicjalizować Firebase Admin SDK: {e}")
    raise

db = firestore.client()

NASA_APOD_URL = f"https://api.nasa.gov/planetary/apod?api_key={NASA_API_KEY}"

def sanitize_filename(name):
    """
    Usuwa lub zastępuje niedozwolone znaki w nazwie pliku.
    """
    # Zamień spacje na podkreślenia
    name = name.replace(' ', '_')
    # Usuń znaki specjalne (pozostaw tylko alfanumeryczne, podkreślenia i kropki)
    name = re.sub(r'[^\w\.-]', '', name)
    return name

def fetch_nasa_image():
    logging.info("Rozpoczynanie pobierania obrazu z NASA APOD.")
    try:
        response = requests.get(NASA_APOD_URL)
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        logging.error(f"Żądanie do API NASA zakończone błędem: {e}")
        raise

    data = response.json()
    if 'url' not in data:
        logging.error("Brak URL do zdjęcia w odpowiedzi API NASA.")
        raise ValueError("Brak URL do zdjęcia w odpowiedzi API NASA.")

    image_url = data.get('hdurl') or data.get('url')
    image_title = data.get('title', 'nasa_image')
    image_title = sanitize_filename(image_title)  # Sanityzacja nazwy
    logging.debug(f"Sanityzowana nazwa obrazu: {image_title}")

    # Sprawdź, czy dokument o tej nazwie już istnieje
    doc_ref = db.collection('nasa_images').document(image_title)
    doc = doc_ref.get()
    if doc.exists:
        logging.info(f"Obraz '{image_title}' jest już w Firestore. Pomijam pobieranie.")
        return None  # Zwróć None, aby wskazać, że obraz został pominięty

    logging.info(f"Pobieranie obrazu z URL: {image_url}")
    try:
        img_response = requests.get(image_url)
        img_response.raise_for_status()
    except requests.exceptions.RequestException as e:
        logging.error(f"Nie udało się pobrać obrazu: {e}")
        raise

    img_data = img_response.content

    # Zapisz oryginał lokalnie (dowolne rozszerzenie)
    parsed_url = urlparse(image_url)
    original_ext = os.path.splitext(parsed_url.path)[1].lower()
    if not original_ext:
        original_ext = ".jpg"  # Domyślnie jpg, jeśli brak rozszerzenia

    temp_dir = tempfile.gettempdir()
    local_file = os.path.join(temp_dir, f"{image_title}{original_ext}")
    try:
        with open(local_file, 'wb') as f:
            f.write(img_data)
        logging.info(f"Zapisano obraz lokalnie: {local_file}")
    except Exception as e:
        logging.error(f"Nie udało się zapisać pliku lokalnie: {e}")
        raise

    # Otwórz obraz z PIL
    try:
        img = Image.open(local_file)
        logging.info(f"Otworzono obraz z PIL: {local_file}")
    except Exception as e:
        logging.error(f"Nie udało się otworzyć obrazu: {e}")
        raise

    # Konwertuj do PNG bezstratnie
    unified_file = os.path.join(temp_dir, f"{image_title}.png")
    try:
        img.save(unified_file, "PNG")
        logging.info(f"Obraz zapisany jako PNG: {unified_file}")
    except Exception as e:
        logging.error(f"Nie udało się zapisać obrazu jako PNG: {e}")
        raise

    # Załaduj ujednolicony plik PNG do Firebase Storage
    bucket = storage.bucket()
    blob = bucket.blob(f"images/{image_title}.png")
    try:
        blob.upload_from_filename(unified_file, content_type='image/png')
        logging.info(f"Plik {image_title}.png (bezstratny) załadowany do bucketu {FIREBASE_BUCKET}.")
    except Exception as e:
        logging.error(f"Nie udało się przesłać pliku do Firebase Storage: {e}")
        raise

    # Zapisz metadane w Firestore
    metadata = {
        'original_url': image_url,
        'title': data.get('title', 'nasa_image'),
        'date': data.get('date'),
        'explanation': data.get('explanation', ''),
        'media_type': data.get('media_type', ''),
        'storage_path': f"images/{image_title}.png",
        'public_url': blob.public_url
    }

    try:
        doc_ref.set(metadata)
        logging.info(f"Metadane obrazu zapisane w Firestore pod dokumentem '{image_title}'.")
    except Exception as e:
        logging.error(f"Nie udało się zapisać metadanych w Firestore: {e}")
        raise

    # Opcjonalnie: Usuń lokalne pliki tymczasowe
    try:
        os.remove(local_file)
        os.remove(unified_file)
        logging.info(f"Usunięto lokalne pliki: {local_file}, {unified_file}")
    except Exception as e:
        logging.warning(f"Nie udało się usunąć lokalnych plików: {e}")

    return metadata  # Zwróć metadane, jeśli obraz został pobrany i zapisany

if __name__ == "__main__":
    try:
        metadata = fetch_nasa_image()
        if metadata:
            logging.info("Pobrane metadane:")
            logging.info(metadata)
        else:
            logging.info("Obraz już istnieje w Firestore. Skrypt zakończony bez pobierania.")
    except Exception as e:
        logging.critical(f"Skrypt zakończył się błędem: {e}")
