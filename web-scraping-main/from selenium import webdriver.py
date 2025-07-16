from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException
import pandas as pd
import time

# Configuração do navegador
options = Options()
options.add_argument('--start-maximized')
options.add_argument('--disable-blink-features=AutomationControlled')
options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36")

driver = webdriver.Chrome(service=Service(), options=options)

produtos = []

try:
    for page in range(1, 18):  # De 1 até 17
        url = f"https://www.magazineluiza.com.br/selecao/ofertasdodia/?page={page}"
        driver.get(url)

        print(f"Acessando página {page}: {url}")

        try:
            WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "a[data-testid='product-card-container']"))
            )
            time.sleep(2)

            cards = driver.find_elements(By.CSS_SELECTOR, "a[data-testid='product-card-container']")
            print(f"  Produtos encontrados: {len(cards)}")

            for card in cards:
                try:
                    title = card.find_element(By.TAG_NAME, "h2").text.strip()
                    price = card.find_element(By.CSS_SELECTOR, "p[data-testid='price-value']").text.strip()
                    link = card.get_attribute("href")

                    produtos.append({
                        "title": title,
                        "price": price,
                        "url": link
                    })
                except Exception as e:
                    print("    Erro ao extrair card:", e)

        except TimeoutException:
            print(f"  Timeout ao carregar produtos na página {page}")

finally:
    driver.quit()

# Salvar como Excel
if produtos:
    df = pd.DataFrame(produtos)
    df.to_excel("ofertas_magalu_completo.xlsx", index=False)
    print(f"\nExtração completa! Total de produtos: {len(produtos)}")
    print("Arquivo 'ofertas_magalu_completo.xlsx' salvo com sucesso!")
else:
    print("Nenhum produto foi extraído.")
