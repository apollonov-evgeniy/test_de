# Код, демонстрирующий работу с Selenium
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.by import By
import time
import pandas as pd
# Install Webdriver
service = Service(ChromeDriverManager().install())


for pk in mpk:
    for sdate in dates:
        if pk + ' ' + sdate not in finished:
            print (pk, sdate)
            driver = webdriver.Chrome(service=service)
            driver.get('https://fips.ru/iiss/search.xhtml')
            driver.find_element(By.XPATH, "//div[contains(text(), 'Патентные документы РФ (рус.)')]").click()
            driver.find_element(By.XPATH, "//div[contains(text(), 'Рефераты российских изобретений')]").click()
            time.sleep(1)
            driver.find_element(By.XPATH, "//input[@value='перейти к поиску']").click()
            dinput = driver.find_element(By.XPATH, "//span[contains(text(), '(51) МПК')]").find_element(By.XPATH, '..').find_element(By.XPATH, "following-sibling::*[1]")
            dinput.find_element(By.TAG_NAME,'input').send_keys(pk)
            dinput = driver.find_element(By.XPATH, "//span[contains(text(), '(22) Дата подачи заявки')]").find_element(By.XPATH, '..').find_element(By.XPATH, "following-sibling::*[1]")
            dinput.find_element(By.TAG_NAME,'input').send_keys(sdate)
            time.sleep(1)
            driver.find_element(By.XPATH, "//input[@class='save']").click()
            x = True

            while x:
                try:
                    driver.find_element(By.XPATH, "//div[contains(text(), 'По заданным параметрам ничего не найдено. Измените условия поиска.')]")
                    break
                except:
                    for row in driver.find_element(By.XPATH, "//div[@class='table']").find_elements(By.TAG_NAME,'a'):
                        regnum = row.find_elements(By.TAG_NAME,'div')[1].text
                        regdate = row.find_elements(By.TAG_NAME,'div')[2].text.replace('(', '').replace(')', '')
                        img = row.find_elements(By.TAG_NAME,'div')[3].find_element(By.TAG_NAME,'img').get_attribute("src")
                        desc = row.find_elements(By.TAG_NAME,'div')[4].text
                        if regnum not in [x[0] for x in rez]:
                            rez.append([regnum, regdate, img, desc, pk])
                    try:
                        driver.find_element(By.XPATH, "//a[@class='ui-commandlink ui-widget modern-page-next']").click()
                        time.sleep(10)
                    except:
                        x = False    

            driver.close()
df = pd.DataFrame(rez)
#%%
df.drop_duplicates(subset=[0]).to_csv('form_rospatent_G.csv')            