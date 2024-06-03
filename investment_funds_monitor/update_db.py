import sqlite3
import datetime
import time
from urllib.request import urlretrieve
import zipfile
import locale
import workadays as wd

import time as t
import warnings

from datetime import timedelta
import datetime as dt
import pandas as pd

from Helpers import common, googleSheets
import os
import requests
from requests.packages.urllib3.exceptions import InsecureRequestWarning

requests.packages.urllib3.disable_warnings(InsecureRequestWarning)
warnings.simplefilter(action="ignore", category=pd.errors.PerformanceWarning)
warnings.simplefilter("ignore", UserWarning)
warnings.simplefilter("ignore", RuntimeWarning)
warnings.simplefilter(action='ignore', category=FutureWarning)
warnings.simplefilter(
    action='ignore', category=pd.errors.SettingWithCopyWarning)
warnings.simplefilter(action='ignore', category=pd.errors.DtypeWarning)


# Create your connection.
db_name = 'db_position.sqlite'


def update_cvm():
    cnx = sqlite3.connect(db_name)
    dta_inicio_str = pd.to_datetime(
        datetime.datetime.now() - timedelta(days=10)).strftime('%d-%m-%Y')
    dta_fim_str = pd.to_datetime(datetime.datetime.now()).strftime('%d-%m-%Y')

    main_folder = common.check_gdrive_path()
    controle_path = f'G://{main_folder}//Produtos DTVM'
    controle_file = f'{controle_path}//Controle Fundos.xlsx'

    df_fundos_geral = pd.read_excel(controle_file, sheet_name='Fundos_Geral')
    df_fundos_geral = df_fundos_geral[~df_fundos_geral.Administradora.isna()]
    # Set Ups
    locale.setlocale(locale.LC_ALL, 'pt_BR.UTF-8')
    pd.set_option("display.max_colwidth", 150)
    nome_arq = r'\inf_diario_fi_'

    # CVM
    url_informe = r"https://dados.cvm.gov.br/dados/FI/DOC/INF_DIARIO/DADOS/"
    url_informe_hist = r"https://dados.cvm.gov.br/dados/FI/DOC/INF_DIARIO/DADOS/HIST/"
    url_cadastro = r"http://dados.cvm.gov.br/dados/FI/CAD/DADOS/cad_fi.csv"
    cadastro_cvm = pd.read_csv(url_cadastro, sep=';', encoding='ISO-8859-1')

    # Caminhos Rede
    origin_path = r'G:\Drives compartilhados\Produtos DTVM\Distribuição Fundos\Rentabilidade'
    destiny_path = r'G:\Drives compartilhados\Produtos DTVM\Distribuição Fundos\Rentabilidade\Arquivos'

    # Range datas
    dta_inicio = dt.datetime.strptime(dta_inicio_str, '%d-%m-%Y')
    dta_fim = dt.datetime.strptime(dta_fim_str, '%d-%m-%Y')
    dta_inicio_util = wd.workdays.workdays(wd.workdays.workdays(
        dta_inicio, +1, country='BR'), -1, country='BR')
    dta_fim_util = wd.workdays.workdays(wd.workdays.workdays(
        dta_fim, +1, country='BR'), -1, country='BR')

    dta_range = []
    dta_range_str = []
    dta_alvo = dta_inicio_util
    memory = ''
    inf_diario_cvm = pd.DataFrame()

    while dta_alvo <= dta_fim_util:

        dta_alvo_str = dta_alvo.strftime('%Y-%m-%d')
        dta_range.append(dta_alvo)
        dta_range_str.append(dta_alvo.strftime('%Y-%m-%d'))

        if memory != dta_alvo.strftime('%Y') + dta_alvo.strftime('%m'):
            csv_alvo = destiny_path + nome_arq + \
                dta_alvo.strftime('%Y') + dta_alvo.strftime('%m') + '.csv'
            zip_alvo = destiny_path + nome_arq + \
                dta_alvo.strftime('%Y') + '.zip'

            while os.path.exists(csv_alvo) == False:
                if os.path.exists(zip_alvo):
                    with zipfile.ZipFile(zip_alvo, "r") as zip_ref:
                        zip_ref.extractall(destiny_path)
                else:
                    urlretrieve(
                        url_informe_hist + nome_arq[1:] + dta_alvo.strftime('%Y') + '.zip', zip_alvo)

            else:
                csv_alvo = destiny_path + nome_arq + \
                    dta_alvo.strftime('%Y') + dta_alvo.strftime('%m') + '.csv'
                zip_alvo = destiny_path + nome_arq + \
                    dta_alvo.strftime('%Y') + dta_alvo.strftime('%m') + '.zip'

                while os.path.exists(csv_alvo) == False:
                    if os.path.exists(zip_alvo):
                        with zipfile.ZipFile(zip_alvo, "r") as zip_ref:
                            zip_ref.extractall(destiny_path)
                    else:
                        urlretrieve(url_informe + nome_arq[1:] + dta_alvo.strftime(
                            '%Y') + dta_alvo.strftime('%m') + '.zip', zip_alvo)

            inf_diario_cvm_anomes = pd.read_csv(
                csv_alvo, sep=';', encoding='ISO-8859-1')
            inf_diario_cvm = pd.concat([inf_diario_cvm, inf_diario_cvm_anomes])
            inf_diario_cvm.reset_index(drop=True, inplace=True)

        if inf_diario_cvm[inf_diario_cvm['DT_COMPTC'] == dta_alvo_str].empty == True:
            csv_alvo = destiny_path + nome_arq + \
                dta_alvo.strftime('%Y') + dta_alvo.strftime('%m') + '.csv'
            zip_alvo = destiny_path + nome_arq + \
                dta_alvo.strftime('%Y') + dta_alvo.strftime('%m') + '.zip'

            urlretrieve(url_informe + nome_arq[1:] + dta_alvo.strftime(
                '%Y') + dta_alvo.strftime('%m') + '.zip', zip_alvo)
            with zipfile.ZipFile(zip_alvo, "r") as zip_ref:
                zip_ref.extractall(destiny_path)

            inf_diario_cvm_anomes = pd.read_csv(
                csv_alvo, sep=';', encoding='ISO-8859-1')
            inf_diario_cvm = pd.concat(
                [inf_diario_cvm, inf_diario_cvm_anomes[inf_diario_cvm_anomes['DT_COMPTC'] == dta_alvo_str]])
            inf_diario_cvm = pd.concat(
                [inf_diario_cvm, inf_diario_cvm_anomes[~inf_diario_cvm_anomes['DT_COMPTC'].isin(dta_range_str)]])
            inf_diario_cvm.reset_index(drop=True, inplace=True)

        memory = dta_alvo.strftime('%Y') + dta_alvo.strftime('%m')
        dta_alvo = wd.workdays.workdays(dta_alvo, + 1, country='BR')

    inf_diario_filtro = inf_diario_cvm[(
        inf_diario_cvm['DT_COMPTC'].isin(dta_range_str))]
    inf_diario_filtro.reset_index(drop=True, inplace=True)

    df_fundos_geral.set_index('CNPJ', inplace=True)

    inf_diario_filtro.set_index('CNPJ_FUNDO', inplace=True)
    inf_diario_filtro = inf_diario_filtro[inf_diario_filtro.index.isin(
        df_fundos_geral.index.unique().tolist())]
    inf_diario_filtro = inf_diario_filtro.reset_index()

    inf_diario_filtro = cadastro_cvm.merge(
        inf_diario_filtro, left_on='CNPJ_FUNDO', right_on='CNPJ_FUNDO')
    inf_diario_filtro.drop(
        columns=['VL_PATRIM_LIQ_x', 'TP_FUNDO_y'], inplace=True)

    def remove_prefix(column_name):
        return column_name.replace('_x', '').replace('_y', '')

    # Rename columns using the lambda function
    inf_diario_filtro.rename(columns=lambda x: remove_prefix(x), inplace=True)

    df_all_db = pd.read_sql('select * from funds_position_cvm',  con=cnx)

    df = pd.concat([df_all_db, inf_diario_filtro])
    df.set_index(['CNPJ_FUNDO', 'DT_COMPTC'], inplace=True)
    df = df[~df.index.duplicated(keep='first')]
    df.reset_index(inplace=True)

    # Specify the table name from which you want to delete all data
    table_name = 'funds_position_cvm'

    df.to_sql(name=table_name, if_exists='replace', index=False, con=cnx)

    cnx.close()


def update_galgo():
    table_name = 'funds_position_galgo'

    cnx = sqlite3.connect(db_name)
    # Specify the table name from which you want to delete all data

    main_folder = common.check_gdrive_path()
    directory_path = f'G://{main_folder}//XMLs-Galgo//db//'
    # List all files in the directory
    files = os.listdir(directory_path)

    # Filter out only the pickle files
    pickle_files = [file for file in files if file.endswith('.pickle')]

    df_all_files = pd.DataFrame()
    for file in pickle_files:
        if len(df_all_files) == 0:
            df_all_files = pd.read_pickle(
                f"G://{main_folder}//XMLs-Galgo//db//{file}")
        else:
            df_aux = pd.read_pickle(
                f"G://{main_folder}//XMLs-Galgo//db//{file}")
            df_all_files = pd.concat([df_all_files, df_aux])

    df_all_files = df_all_files.drop_duplicates()

    df_all_db = pd.read_sql(f'select * from {table_name}',  con=cnx)

    df = pd.concat([df_all_db, df_all_files])

    df.set_index(['dataInformacao', 'cnpj'], inplace=True)
    df = df[~df.index.duplicated(keep='first')]
    df.reset_index(inplace=True)

    df = df.drop_duplicates()
    df.dataInformacao = pd.to_datetime(
        df.dataInformacao).dt.strftime('%Y-%m-%d')

    df.to_sql(name=table_name, if_exists='replace', index=False, con=cnx)

    cnx.close()


def update_fund_position():
    table_name = 'funds_position'

    cnx = sqlite3.connect(db_name)

    df_galgo = pd.read_sql('select * from funds_position_galgo',  con=cnx)
    df_cvm = pd.read_sql('select * from funds_position_cvm',  con=cnx)

    df_cvm = df_cvm.loc[:, ['CNPJ_FUNDO', 'DENOM_SOCIAL',
                            'DT_COMPTC', 'VL_QUOTA', 'VL_PATRIM_LIQ']]
    df_galgo = df_galgo.loc[:, ['cnpj', 'fundo',
                                'dataInformacao', 'valorCota', 'valorPl']]
    df_galgo.columns = ['CNPJ_FUNDO', 'DENOM_SOCIAL',
                        'DT_COMPTC', 'VL_QUOTA', 'VL_PATRIM_LIQ']

    df_cvm.DT_COMPTC = pd.to_datetime(df_cvm.DT_COMPTC)
    df_galgo.DT_COMPTC = pd.to_datetime(df_galgo.DT_COMPTC)

    df_cvm.VL_QUOTA = pd.to_numeric(df_cvm.VL_QUOTA)
    df_galgo.VL_QUOTA = pd.to_numeric(df_galgo.VL_QUOTA)

    df_merge = df_cvm.merge(df_galgo,
                            left_on=['CNPJ_FUNDO', 'DT_COMPTC'],
                            right_on=['CNPJ_FUNDO', 'DT_COMPTC'],
                            suffixes=('_CVM', '_GALGO'),
                            how='outer'
                            )

    df_merge.loc[:, 'DENOM_SOCIAL'] = ''
    df_merge.loc[:, 'DENOM_SOCIAL'][df_merge.DENOM_SOCIAL_CVM.isna(
    )] = df_merge[df_merge.DENOM_SOCIAL_CVM.isna()].DENOM_SOCIAL_GALGO
    df_merge.loc[:, 'DENOM_SOCIAL'][df_merge.DENOM_SOCIAL_GALGO.isna(
    )] = df_merge[df_merge.DENOM_SOCIAL_GALGO.isna()].DENOM_SOCIAL_CVM
    df_merge.loc[:, 'DENOM_SOCIAL'][df_merge.DENOM_SOCIAL ==
                                    ''] = df_merge[df_merge.DENOM_SOCIAL == ''].DENOM_SOCIAL_CVM
    df_merge.drop(columns=['DENOM_SOCIAL_CVM',
                  'DENOM_SOCIAL_GALGO'], inplace=True)

    df_merge = df_merge.sort_values(by=['CNPJ_FUNDO', 'DT_COMPTC'])
    df_merge.loc[:, ['VL_QUOTA', 'VL_PATRIM_LIQ']] = 0

    df_merge.loc[:, 'VL_QUOTA'][df_merge.VL_QUOTA_GALGO.isna(
    )] = df_merge.loc[:, 'VL_QUOTA_CVM'][df_merge.VL_QUOTA_GALGO.isna()]
    df_merge.loc[:, 'VL_PATRIM_LIQ'][df_merge.VL_QUOTA_GALGO.isna(
    )] = df_merge.loc[:, 'VL_PATRIM_LIQ_CVM'][df_merge.VL_QUOTA_GALGO.isna()]

    df_merge.loc[:, 'VL_QUOTA'][df_merge.VL_QUOTA_CVM.isna(
    )] = df_merge.loc[:, 'VL_QUOTA_GALGO'][df_merge.VL_QUOTA_CVM.isna()]
    df_merge.loc[:, 'VL_PATRIM_LIQ'][df_merge.VL_QUOTA_CVM.isna(
    )] = df_merge.loc[:, 'VL_PATRIM_LIQ_GALGO'][df_merge.VL_QUOTA_CVM.isna()]

    df_merge.loc[:, 'VL_QUOTA'][(df_merge.VL_QUOTA_CVM.fillna(0) != 0) & (df_merge.VL_QUOTA_GALGO.fillna(
        0) != 0)] = df_merge[(df_merge.VL_QUOTA_CVM.fillna(0) != 0) & (df_merge.VL_QUOTA_GALGO.fillna(0) != 0)].VL_QUOTA_CVM
    df_merge.loc[:, 'VL_PATRIM_LIQ'][(df_merge.VL_PATRIM_LIQ_CVM.fillna(0) != 0) & (df_merge.VL_PATRIM_LIQ_GALGO.fillna(
        0) != 0)] = df_merge[(df_merge.VL_PATRIM_LIQ_CVM.fillna(0) != 0) & (df_merge.VL_PATRIM_LIQ_GALGO.fillna(0) != 0)].VL_PATRIM_LIQ_CVM

    df_merge.set_index(['DT_COMPTC', 'CNPJ_FUNDO'], inplace=True)
    df_merge = df_merge[~df_merge.index.duplicated(keep='first')]

    df_merge.fillna(0, inplace=True)
    df_merge.reset_index(inplace=True)

    df_merge.to_sql(name=table_name, if_exists='replace', index=False, con=cnx)
    cnx.close()


def save_gsheets():
    main_folder = common.check_gdrive_path()
    controle_path = f'G://{main_folder}//Produtos DTVM'
    controle_file = f'{controle_path}//Controle Fundos.xlsx'

    df_fundos_geral = pd.read_excel(controle_file, sheet_name='Fundos_Geral')
    df_fundos_geral = df_fundos_geral[~df_fundos_geral.Administradora.isna()]
    df_fundos_geral = df_fundos_geral[df_fundos_geral.loc[:, 'STATUS OPERACIONAL'].isin([
        'ATIVO'])]

    cnx = sqlite3.connect(db_name)
    df_db = pd.read_sql('select * from funds_position',  con=cnx)
    df_db = df_db.loc[:, ['CNPJ_FUNDO', 'DT_COMPTC',
                          'DENOM_SOCIAL', 'VL_QUOTA', 'VL_PATRIM_LIQ']]
    df_db = df_db[df_db.CNPJ_FUNDO.isin(
        df_fundos_geral.CNPJ.unique().tolist())]
    df_db.DT_COMPTC = pd.to_datetime(df_db.DT_COMPTC)

    df_db = df_db.replace(
        pd.Series(df_fundos_geral.Fundo.values, index=df_fundos_geral.CNPJ).to_dict())

    df_db = pd.pivot_table(df_db, values=['VL_QUOTA', 'VL_PATRIM_LIQ'], index=[
        'DT_COMPTC'], columns=['CNPJ_FUNDO'], fill_value=0)
    df_db.columns = df_db.columns.swaplevel()
    df_db = df_db.loc[:, df_db.columns.get_level_values(0)]

    df_dates = pd.DataFrame(pd.date_range(
        start=df_db.index.min(), end=df_db.index.max()))
    df_dates.columns = ['DT_COMPTC']

    df_db.reset_index(inplace=True)

    df_db.loc[-1, :] = df_db.columns.get_level_values(1)
    df_db.sort_index(inplace=True)
    df_db = df_db.loc[:, ~df_db.columns.duplicated()]

    df_db.columns = df_db.columns.get_level_values(0)
    df_db = df_db.merge(df_dates, left_on=['DT_COMPTC'], right_on=[
                        'DT_COMPTC'], how='outer').ffill()

    df_db.DT_COMPTC = df_db.DT_COMPTC.dt.strftime('%Y-%m-%d').fillna('')

    googleSheets.update_or_create_sheet(
        df=df_db, file_name='Posicoes-db', sheet_name='Posicoes', folder_id='1EHC5uw2jnUEs29lb0moihcePph-L3xFF')
