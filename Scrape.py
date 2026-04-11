from bs4 import BeautifulSoup
import requests
from urllib.parse import urljoin
import pandas as pd
import time
import random
import concurrent.futures
from tqdm import tqdm

class SetlistScraper:
    def __init__(self, start_url):
        self.start_url = start_url
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.5",
            "Accept-Encoding": "gzip, deflate, br",
            "Connection": "keep-alive",
            "Upgrade-Insecure-Requests": "1",
            "Sec-Fetch-Dest": "document",
            "Sec-Fetch-Mode": "navigate",
            "Sec-Fetch-Site": "none",
            "Sec-Fetch-User": "?1"
        }
        self.session = requests.Session()
        self.base_soup = self.get_soup(self.start_url)

    def get_soup(self, url):
        try:
            response = self.session.get(url, headers=self.headers)
            response.raise_for_status()
            return BeautifulSoup(response.content, "html.parser")
        except requests.RequestException as e:
            print(f"Error fetching {url}: {e}")
            return None
    
    def get_qnt_pages(self, base_soup):
        last_page_link = base_soup.find("a", title="Go to last page")
        if last_page_link:
            return int(last_page_link.text.strip())
        
        page_nodes = base_soup.find_all(class_="pageLink")
        if not page_nodes:
            return 1
        
        numeros = [int(node.text.strip()) for node in page_nodes if node.text.strip().isdigit()]
        
        if numeros:
            return max(numeros)
        return 1
    
    def get_shows_per_page(self,base_soup):
        shows = []
        url_base = "https://www.setlist.fm" 
        
        for h2 in base_soup.find_all("h2"):

            a_tag = h2.find("a", title=True, href=True)
            
            if a_tag is not None:
                if "View this" in a_tag['title'] and "upcoming" not in a_tag['title'].lower():
                    
                    incomplete_url = a_tag['href']
                    full_url = urljoin(url_base, incomplete_url)
                    
                    if full_url not in shows:
                        shows.append(full_url)
        return shows
    

    def extract_show_details(self, show_url):
        soup = self.get_soup(show_url)

        if not soup:
            return None
        
        band = soup.find("a", href=lambda href: href and "/setlists/" in href).text.strip()
        songs = [tag.text for tag in soup.find_all("a", class_ = "songLabel")]
        venue = soup.find("a", href=lambda href: href and "/venue/" in href).text.strip()
        month = soup.find("span", class_ = "month").text.strip()
        day = soup.find("span", class_ = "day").text.strip()
        year = soup.find("span", class_ = "year").text.strip()
        date = f"{month} {day} {year}"

        return {"band": band, "venue": venue, "date": date, "setlist": songs, "url": show_url}
    
    def scrape(self):
        # --- O ESCUDO ANTI-CRASH ---
        if not self.base_soup:
            print("Página inicial bloqueada pelo site (Erro 503). O base_soup está vazio.")
            print("Seu IP está temporariamente suspenso. Abortando extração.")
            return None
        
        all_shows_data = []
        all_links_to_scrape = []

        for page in range(1, self.get_qnt_pages(self.base_soup) + 1):
            if "?" in self.start_url:
                new_url = self.start_url.replace("?", f"?page={page}&")
            else:
                new_url = f"{self.start_url}?page={page}"

            page_soup = self.get_soup(new_url)
            if page_soup:
                shows_links = self.get_shows_per_page(page_soup)
                all_links_to_scrape.extend(shows_links)
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        
            # Dispara todos os trabalhadores
            workers = [executor.submit(self.extract_show_details, link) for link in all_links_to_scrape]
            
            for workers in tqdm(concurrent.futures.as_completed(workers), total=len(workers), desc="Extraindo Shows"):
                
                time.sleep(random.uniform(1.0, 2.0))
                
                details = workers.result()
                if details:
                    all_shows_data.append(details)

        # Cria a tabela e remove duplicatas por segurança
        df = pd.DataFrame(all_shows_data)
        if not df.empty:
            df = df.drop_duplicates(subset=['url'])

        return df

url_digitada = input("Enter the URL you want to scrape: ")
scraper = SetlistScraper(url_digitada)
df = scraper.scrape()
print(df)   
