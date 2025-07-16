from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.action_chains import ActionChains
from bs4 import BeautifulSoup
import undetected_chromedriver as uc
import time
import pandas as pd
import re

# Configurações do navegador
options = uc.ChromeOptions()
options.add_argument('--no-sandbox')
options.add_argument('--disable-dev-shm-usage')
options.add_argument('--disable-blink-features=AutomationControlled')
options.add_argument('--start-maximized')

# Inicia o navegador
driver = uc.Chrome(options=options)
wait = WebDriverWait(driver, 10)
actions = ActionChains(driver)

# Acessa o site
url = "https://www.temu.com/br"
driver.get(url)
time.sleep(5)

# Fecha modal, se presente
try:
    modal = wait.until(EC.element_to_be_clickable((By.XPATH, "//div[@aria-label='Fechar']")))
    modal.click()
    print("Modal fechado.")
    time.sleep(2)
except TimeoutException:
    print("Modal não apareceu.")

# Clica repetidamente em "Ver mais"
while True:
    try:
        ver_mais = wait.until(EC.element_to_be_clickable((By.XPATH, "//div[contains(text(), 'Ver mais')]")))
        driver.execute_script("arguments[0].scrollIntoView();", ver_mais)
        time.sleep(1)
        ver_mais.click()
        print("Clique em 'Ver mais'")
        time.sleep(3)
    except Exception:
        print("Todos os produtos carregados.")
        break

# Faz o parsing da página
soup = BeautifulSoup(driver.page_source, "html.parser")
cards = soup.find_all("div", class_=re.compile("^_6q6qVUF5 "))
print(f"Cards encontrados: {len(cards)}")

produtos = []
for card in cards:
    try:
        # Descrição
        title_tag = card.find("h3", class_=re.compile("^_2BvQbnbN"))
        title = title_tag.get_text(strip=True) if title_tag else None

        # Preço
        price_tag = card.find("div", class_=re.compile("^_382YgpSF"))
        price = price_tag.get_text(strip=True) if price_tag else None

        # Imagem
        try:
            image = card.select_one("img")
            image_url = image["src"] if image and "src" in image.attrs else None
        except Exception as e:
            print(f"Erro ao extrair imagem: {e}")
            image_url = None

        if any([title, price, image_url]):
            produtos.append({
                "title": title,
                "price": price,
                "image": image_url
            })
    except Exception as e:
        print(f"Erro ao extrair card: {e}")

# Fecha navegador
driver.quit()

# Salva resultados
if produtos:
    df = pd.DataFrame(produtos)
    print(df.head())
    df.to_excel("ofertas_temu.xlsx", index=False)
    print("Arquivo 'ofertas_temu.xlsx' salvo com sucesso!")
else:
    print("Nenhum produto extraído. Verifique os seletores ou possíveis bloqueios.")
