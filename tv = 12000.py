from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
import pandas as pd
import time
import re

# Configura o navegador com user-agent de mobile
mobile_emulation = {
    "deviceMetrics": {"width": 375, "height": 812, "pixelRatio": 3.0},
    "userAgent": "Mozilla/5.0 (iPhone; CPU iPhone OS 13_5_1 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/13.1.1 Mobile/15E148 Safari/604.1"
}

options = webdriver.ChromeOptions()
options.add_experimental_option("mobileEmulation", mobile_emulation)
options.add_argument("--start-maximized")
driver = webdriver.Chrome(options=options)

# Remove cookies para iniciar da primeira página limpa
driver.delete_all_cookies()

# Lista para armazenar os dados extraídos
produtos = []

# URL inicial da categoria
url = "https://lista.mercadolivre.com.br/_Container_ms-lp_NoIndex_True"
driver.get(url)

# Tenta fechar o banner de cookies se existir
try:
    WebDriverWait(driver, 5).until(
        EC.presence_of_element_located((By.CLASS_NAME, "cookie-consent-banner-opt-out__container"))
    )
    driver.execute_script("document.querySelector('.cookie-consent-banner-opt-out__container').remove()")
    time.sleep(1)
except TimeoutException:
    pass

# Loop de paginação
while True:
    print(f"Extraindo dados da página: {driver.current_url}")
    time.sleep(2)

    cards = driver.find_elements(By.XPATH, "//li[contains(@class, 'ui-search-layout__item')]")
    for card in cards:
        try:
            raw_text = card.text
            titulo_match = re.search(r"^[^\n]+", raw_text)
            preco_match = re.search(r"R\$\s?[\d\.]+,\d{2}", raw_text)

            if not titulo_match:
                continue

            titulo = titulo_match.group().strip()
            preco = preco_match.group().strip() if preco_match else "Indisponível"
            link = card.find_element(By.TAG_NAME, "a").get_attribute("href")

            produtos.append({
                "title": titulo,
                "price": preco,
                "url": link
            })
        except Exception as e:
            print(f"Erro ao extrair card: {e}")

    # Tenta clicar no botão "Próxima"
    try:
        next_button = WebDriverWait(driver, 5).until(
            EC.element_to_be_clickable((By.XPATH, "//li[contains(@class, 'andes-pagination__button--next')]//a"))
        )
        driver.execute_script("arguments[0].click();", next_button)
    except TimeoutException:
        print("Fim da paginação ou botão 'Próxima' não encontrado.")
        break

# Encerra o navegador e exporta os dados
driver.quit()
df = pd.DataFrame(produtos)
df.to_excel("ofertas_mercadolivre.xlsx", index=False)
print("Arquivo 'ofertas_mercadolivre.xlsx' salvo com sucesso!")
