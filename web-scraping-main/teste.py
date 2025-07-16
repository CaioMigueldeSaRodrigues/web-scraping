from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException
import random
import time
import pandas as pd

# Configuração mobile realista
MOBILE_EMULATION = {
    "deviceMetrics": {"width": 360, "height": 640, "pixelRatio": 3.0},
    "userAgent": "Mozilla/5.0 (Linux; Android 10; SM-G981B) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.162 Mobile Safari/537.36"
}

# Configuração avançada do navegador
def configure_driver():
    options = webdriver.ChromeOptions()
    options.add_experimental_option("mobileEmulation", MOBILE_EMULATION)
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument("--disable-infobars")
    options.add_argument("--disable-notifications")
    options.add_argument("--disable-popup-blocking")
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    
    # Configurações anti-detecção
    options.add_argument("--disable-web-security")
    options.add_argument("--disable-site-isolation-trials")
    options.add_argument("--disable-features=IsolateOrigins,site-per-process")
    
    driver = webdriver.Chrome(options=options)
    driver.execute_cdp_cmd("Network.setUserAgentOverride", {
        "userAgent": MOBILE_EMULATION["userAgent"]
    })
    return driver

# Comportamento humano simulado
def human_like_interaction(driver):
    # Scroll aleatório
    scroll_height = random.randint(200, 800)
    driver.execute_script(f"window.scrollBy(0, {scroll_height})")
    time.sleep(random.uniform(0.5, 1.5))
    
    # Movimentos do mouse aleatórios
    action = webdriver.ActionChains(driver)
    action.move_by_offset(random.randint(1,10), random.randint(1,10)).perform()

# Função principal de scraping
def mobile_scrape(url, max_pages=5):
    driver = configure_driver()
    driver.get(url)
    data = []
    
    try:
        # Aceitar cookies
        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.ID, "newCookieDisclaimerButton"))
        ).click()
        time.sleep(2)
    except TimeoutException:
        pass
    
    for _ in range(max_pages):
        try:
            # Espera dinâmica baseada no conteúdo
            WebDriverWait(driver, 15).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "div.ui-search-result__wrapper"))
            )
            
            # Coleta de produtos
            products = driver.find_elements(By.CSS_SELECTOR, "div.ui-search-result__wrapper")
            for product in products:
                try:
                    human_like_interaction(driver)
                    
                    # Extração de dados otimizada
                    title = product.find_element(By.CSS_SELECTOR, "h2.ui-search-item__title").text
                    price = product.find_element(By.CSS_SELECTOR, "span.andes-money-amount__fraction").text
                    link = product.find_element(By.CSS_SELECTOR, "a.ui-search-link").get_attribute("href")
                    
                    data.append({
                        "title": title,
                        "price": f"R$ {price}",
                        "url": link.split("?")[0]
                    })
                except Exception as e:
                    continue
            
            # Paginação com comportamento humano
            next_button = driver.find_element(By.CSS_SELECTOR, "li.andes-pagination__button--next a")
            driver.execute_script("arguments[0].scrollIntoView({behavior: 'smooth'});", next_button)
            time.sleep(random.uniform(1, 2))
            next_button.click()
            time.sleep(random.uniform(2, 4))
            
        except Exception as e:
            print(f"Erro: {str(e)}")
            break
    
    driver.quit()
    return data

# Execução
if __name__ == "__main__":
    url = "https://m.mercadolivre.com.br/ofertas"
    print("Iniciando scraping mobile...")
    
    try:
        results = mobile_scrape(url)
        df = pd.DataFrame(results)
        
        # Pós-processamento
        df.drop_duplicates(subset=['url'], inplace=True)
        df.to_csv('ofertas_mobile.csv', index=False)
        print(f"Coleta concluída! {len(df)} produtos salvos.")
        
    except Exception as e:
        print(f"Falha crítica: {str(e)}")