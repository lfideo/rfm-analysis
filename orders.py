import pandas as pd
import numpy as np
from matplotlib import pyplot as plt
import shutil
import os
import glob

pd.options.display.float_format='{:,.2f}'.format
pd.set_option('display.max_colwidth', 100)

directory = '***'
os.chdir(directory)

path = os.getcwd()
files = glob.glob(path + '/*.xlsx')
files = [file for file in files if file.find('20')!=-1 and file.find('$')==-1]

# функции для чтения и импорта файлов с переименованием столбцов
def read_file(path, *cols, sheet=None):
    if sheet is None:
        sheet=0
    dtypes = {col: 'object' for col in cols}
    df = pd.read_excel(path, sheet_name=sheet, dtype=dtypes, engine='openpyxl')\
        .rename(columns={
            ''
        })[:-2]
    
    return df

def rename_and_export(df, path=directory):
    reverse_mapping = {
        ''
    }
    
    df = df.rename(columns=reverse_mapping)
    df.to_excel(path, index=False)

# из 8 файлов собирается один датафрейм с заказами
li = []
for file in files:
        try:
                orders = read_file(
                        file,
                        'Код ДК','ИМ','ID товара'
                        ).drop(columns=['Unnamed: 1', 'Unnamed: 2', 'Unnamed: 3'])
                li.append(orders)
        except Exception as e:
                print(f"Failed to read file {file} with error {e}")
orders = pd.concat(li, axis=0, ignore_index=True)
orders_copy = orders.copy(deep=True)
orders.sample(n=5)

# фильтрация ненужных заказов
strange_orders = orders.query('documentType=="Возврат товаров от клиента" or orderType=="no_order"')
orders = orders.loc[~orders.document.isin(strange_orders.document)]

# таблица с клиентами
clients = read_file(
    '***',
    'clientID'
)

# из таблицы с клиентами оставляю только тех, кто присутствует в таблице с orders
clients = clients.loc[clients.clientID.isin(orders.clientID)]
# дедубликация задвоенных clientID
clients['rn'] = clients.groupby('clientID').cumcount() + 1

clients = clients.query('rn==1')

clients.drop(columns='rn', inplace=True)

# таблица с товарами 
items = read_file(
        '***',
        'Характеристика.Код77', 'supplyYear')[1:]

items = items[[
    'itemID', 'itemInternalID', 'item', 'itemCategory', 'itemActivityType',
    'itemGroup', 'itemSize', 'itemGender', 'itemManufacturer', 'itemSizeWWW', 'itemCountryOrigin', 
    'rrp', 'costPrice'
]]
# дедубликация задвоенных строк
items['rn'] = items.groupby('itemID').cumcount() + 1
items = items.query('rn==1')

items.drop(columns='rn', inplace=True)

# присоединяю товары к заказам
ordersItems = orders.merge(items, how='left', on='itemID', suffixes=['_orders', '_items'])
# присоединяю клиентов к таблице ordersItems
ordersItemsClients = ordersItems.merge(
    clients[[
        'cardType', 'retail', 'releaseDate', 'emailList', 'smsList',
        'birthYear', 'retailGeo', 'retailType', 'clientID'
    ]], 
    on='clientID', how='left'
)

crm = ordersItemsClients[[
    'orderDate', 'orderID', 'clientID',
    'itemCategory', 'itemID', 'revenue'# 'quantity', 'rrp'
]]
# группировка данных по дате, id заказа и id клиента
crmClients = crm.groupby(['orderDate', 'orderID', 'clientID'], dropna=False)[['revenue']].sum().reset_index()
# делаю смещение по дате для подсчета recency
snapshot_date = crmClients.orderDate.max() + pd.DateOffset(days=1)

# здесь создаются столбцы диапазоны суммы заказов, коризны количества заказов, сколько месяцев прошло с последнего заказа
# сколько месяце прошло с момента регистрации
crmClients = crmClients.assign(
    orderQuantity = crmClients.groupby('clientID')[['orderID']].transform('nunique').fillna(0),
    monthsAgo = crmClients.groupby('clientID')[['orderDate']].transform(lambda x: round((snapshot_date - x.max()).days/30.44)),
    monthsSinceRegistration = crmClients.groupby('clientID')[['orderDate']].transform(lambda x: round((x.max() - x.min()).days/30.44)),
    billRange = lambda x: pd.cut(
            x.revenue, bins=[
                0, 3000, 6500, 15000, 25000, 35000,
                55000, 80000, 120000, 200000, 400000, 909000    
            ], 
            labels=[
                '0-3000', '3000-6500', '6500-15000', '15000-25000', '25000-35000',
                '35000-55000', '55000-80000', '80000-120000', '120000-200000', '200000-400000', '400000+'
            ]
        )
)
# здесь создаются столбцы на основе соданных корзин выше
crmClients = crmClients.assign(
    monthBins = pd.cut(
        crmClients.monthsAgo, bins = [i for i in range(0, 13)] + [24, 36, 48, 60, 72, 84, 96] + [np.inf],
        labels = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11,
        '1+ год', '2+ года', '3+ года', '4+ лет', '5+ лет', '6+ лет', '7+ лет', '8 и более']
    ),
    monthRegistrationBins = pd.cut(
        crmClients.monthsSinceRegistration, bins = [i for i in range(0, 13)] + [24, 36, 48, 60, 72, 84, 96] + [np.inf],
        labels = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11,
        '1+ год', '2+ года', '3+ года', '4+ лет', '5+ лет', '6+ лет', '7+ лет', '8 и более']
    ),
    orderBins = pd.cut(
        crmClients.orderQuantity, bins = [i for i in range(0, 11)] + [np.inf],
        labels = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, '10 и больше']
    )
)

# функция для подсчета простых сводных таблиц, таких как: распределение клиентов по давности сделанного заказа
def rfm(operation, dimension, col='clientID'):
    if operation == 'nunique':
        df = crmClients.groupby(dimension)[col].nunique().reset_index()
    elif operation == 'sum':
        df = crmClients.groupby(dimension)[col].sum().reset_index()
    df = df.assign(fromTotal = 100*(df[col] / df[col].sum()))
    return df
# вот пример - он будет в ворде
recency = rfm('nunique', 'monthBins')

# функция для подсчета значений rfm
def r_score(x, p, d):
    """
    Вычисляет значение (R) для RFM анализа. Более высокая оценка означает недавнюю аквтиность.
    
    Аргументы:
        x (float): Значение, для которого требуется вычислить оценку.
        p (str): Имя столбца, для которого вычисляется оценка.
        d (dict): Словарь, содержащий квантили для каждого столбца.

    Возвращает:
        int: Оценка R, где более низкие значения x дают более высокую оценку.
    """
    if x <= d[p][0.25]:
        return 4
    elif x <= d[p][0.5]:
        return 3
    elif x <= d[p][0.75]:
        return 2
    else:
        return 1

def fm_score(x, p, d):
    """
    Вычисляет значения (F или M) для RFM анализа. 
    Высокий бал (макс. 4) означает более высокую частнотность или более высокое денежное значение.
    
    Args:
        x (float): Значение, для которого требуется вычислить оценку.
        p (str): Имя столбца, для которого вычисляется оценка.
        d (dict): Словарь, содержащий квантили для каждого столбца.

    Returns:
        int: The F or M score based on the frequency or monetary value. 
        Диапазон от 1 (низкая частотность или денежное значение) до 4 (высокая частотность или денежное значение).
    """
    if x <= d[p][0.25]:
        return 1
    elif x <= d[p][0.5]:
        return 2
    elif x <= d[p][0.75]:
        return 3
    else:
        return 4

rfmClients = crmClients.groupby('clientID').agg({
    'orderDate': lambda x: round((snapshot_date - x.max()).days / 30.44),
    'orderID': 'nunique',
    'revenue': 'sum'
})
rfmClients.rename(columns={
    'orderDate': 'recency', 'orderID': 'frequency', 'revenue': 'monetary'
}, inplace=True)

rfmClients.reset_index(inplace=True)

# функция для подсчета квантилей
def calcQuantiles(df):
    quantiles = df[['recency', 'frequency', 'monetary']].quantile(q=[0.25, 0.5, 0.75])
    quantiles = quantiles.to_dict()
    return quantiles

clientsQuantiles = calcQuantiles(rfmClients)

# таблица с rfm значениями
rfmClients = rfmClients.assign(
    r = rfmClients.recency.apply(r_score, args=['recency', clientsQuantiles]),
    f = rfmClients.frequency.apply(fm_score, args=['frequency', clientsQuantiles]),
    m = rfmClients.monetary.apply(fm_score, args=['monetary', clientsQuantiles])
)

# сегментация на основе rfm значений
def rfm_level(df):
    if bool(df['r'] >= 4) and bool(df['f'] >= 4) and bool(df['m'] >= 4):
        return 'Чемпионы'
    elif bool(df['r'] >= 3) and bool(df['f'] >= 3) and bool(df['m'] >= 3):
        return 'Лояльные'
    elif bool(3>= df['r'] > 2) and bool(3>= df['f'] > 2) and bool(3>= df['m'] > 2):
        return 'Потенциельные лояльные'
    elif bool(df['r'] >= 4) and bool(df['f'] <= 2) and bool(df['m'] <= 2):
        return 'Недавние'
    elif bool(df['r'] >= 3) and bool(df['f'] <= 1) and bool(df['m'] <= 1):
        return 'Многообещающие'
    elif bool(2>= df['r'] > 1) and bool(2>= df['f'] > 1) and bool(2>= df['m'] > 1):
        return 'Требуют внимания'
    elif bool(df['r'] <= 2) and bool(df['f'] <= 2) and bool(df['m'] <= 2):
        return 'Спящие'
    elif bool(df['r'] <= 2) and bool(df['f'] >= 3) and bool(df['m'] >= 3):
        return 'В зоне риска'
    else:
        return 'Надо им напомнить' 

rfmClients['rfmLevel'] = rfmClients.apply(rfm_level, axis=1)

# сколько каждая категория занимает в процентах от всей базы
rfmClients.groupby('rfmLevel').agg({
    'clientID': lambda x: len(x)/len(rfmClients) * 100
}).reset_index()