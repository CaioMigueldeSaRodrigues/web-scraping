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

produtos = []
url = "https://lista.mercadolivre.com.br/_Container_ms-lp_NoIndex_True?skipInApp=true"
driver.get(url)

# Fecha banner de cookies
try:
    WebDriverWait(driver, 5).until(
        EC.presence_of_element_located((By.CLASS_NAME, "cookie-consent-banner-opt-out__container"))
    )
    driver.execute_script("document.querySelector('.cookie-consent-banner-opt-out__container').remove()")
    time.sleep(1)
except TimeoutException:
    pass

# Fecha modal do app
try:
    fechar_modal = WebDriverWait(driver, 5).until(
        EC.element_to_be_clickable((By.XPATH, "//button[contains(text(), 'Agora não')]"))
    )
    fechar_modal.click()
    print("Modal 'Abrir app' fechado.")
    time.sleep(1)
except TimeoutException:
    pass

# Loop de paginação
while True:
    print(f"Extraindo dados da página: {driver.current_url}")
    time.sleep(2)

    cards = driver.find_elements(By.XPATH, "//li[contains(@class, 'ui-search-layout__item')]")
    if not cards:
        cards = driver.find_elements(By.XPATH, "//a[contains(@href, '/MLB-')]")

    for card in cards:
        try:
            raw_text = card.text
            titulo_match = re.search(r"^[^\n]+", raw_text)
            preco_match = re.search(r"R\$\s*\d+[.,]?\d*", raw_text)

            if not titulo_match:
                continue

            titulo = titulo_match.group().strip()
            preco = preco_match.group().replace('\n', '').strip() if preco_match else "Indisponível"

            try:
                link = card.find_element(By.TAG_NAME, "a").get_attribute("href")
            except NoSuchElementException:
                link = ""

            if link:
                produtos.append({
                    "title": titulo,
                    "price": preco,
                    "url": link
                })
        except Exception as e:
            print(f"Erro ao extrair card: {e}")

    driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
    time.sleep(2)

    # Remove banner completo do app se necessário
    try:
        wrapper = driver.find_element(By.CLASS_NAME, "download-app-top-banner-wrapper")
        driver.execute_script("arguments[0].remove();", wrapper)
        print("Banner do app removido.")
        time.sleep(1)
    except NoSuchElementException:
        pass

    # Remove botão de cookies se estiver bloqueando
    try:
        cookie_btn = driver.find_element(By.CLASS_NAME, "cookie-consent-banner-opt-out__action")
        driver.execute_script("arguments[0].remove();", cookie_btn)
        print("Botão de cookies removido.")
        time.sleep(1)
    except NoSuchElementException:
        pass

    # Botão "Próxima"
    try:
        next_button = WebDriverWait(driver, 5).until(
            EC.element_to_be_clickable((By.XPATH, "//li[contains(@class, 'andes-pagination__button--next')]//a"))
        )
        next_button.click()
    except TimeoutException:
        print("Fim da paginação ou botão 'Próxima' não encontrado.")
        break

# Finaliza
driver.quit()

if produtos:
    df = pd.DataFrame(produtos)
    print(df.head())
    df.to_excel("ofertas_mercadolivre.xlsx", index=False)
    print("Arquivo 'ofertas_mercadolivre.xlsx' salvo com sucesso!")
else:
    print("Nenhum produto extraído. Verifique o scraping.")
